package io.olake.iceberg.tableIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import io.olake.iceberg.rpc.RecordIngest.ReadRowsRequest;

/**
 * Reads the requested columns of rows addressed by {@code (data file, row position)}.
 * Skips row groups without a wanted row and, when the file has a page index, pages too;
 * without one it reads whole row groups (correct, but more data).
 */
public final class RowReader {
  private RowReader() {
  }

  /**
   * One row's projected values by column name. A column the data file does not carry is left
   * out, which is not the same as a stored NULL (a null value).
   */
  public static final class Row {
    public final long position;
    public final Map<String, Object> values;

    private Row(long position, Map<String, Object> values) {
      this.position = position;
      this.values = values;
    }
  }

  /** Receives rows as they are read, so neither side holds a whole file's values. */
  public interface RowConsumer {
    void accept(String filePath, Row row) throws Exception;
  }

  /**
   * Reads the given rows of one data file, each with its own columns, and hands each row to
   * {@code consumer}. {@code rows} are in ascending position order.
   *
   * <p>The file is projected to every column some row wants, since Parquet reads a set of
   * rows across all projected columns; each row is still returned with only its own.
   */
  public static void read(Table table, String filePath, List<ReadRowsRequest.Row> rows, RowConsumer consumer)
      throws Exception {
    Set<String> columns = new LinkedHashSet<>();
    rows.forEach(row -> columns.addAll(row.getColumnsList()));

    InputFile inputFile = table.io().newInputFile(filePath);
    // ParquetInput adapts Iceberg's file to Parquet's type, so the read keeps using the
    // table's FileIO and credentials.
    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInput(inputFile),
        ParquetReadOptions.builder().build())) {
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      // Matched by Iceberg field id, as Iceberg readers do; a column the file was written
      // without is left out.
      MessageType projection = ParquetSchemaUtil.pruneColumns(fileSchema, table.schema().select(columns));
      reader.setRequestedSchema(projection);

      ColumnIOFactory columnIOFactory = new ColumnIOFactory();
      int cursor = 0; // index in rows of the next row to read
      long firstRowOfBlock = 0; // file position of the current row group's first row

      for (int blockIndex = 0; blockIndex < reader.getRowGroups().size() && cursor < rows.size(); blockIndex++) {
        BlockMetaData block = reader.getRowGroups().get(blockIndex);
        long rowsInBlock = block.getRowCount();

        // the rows from cursor on that fall in this row group
        int blockStart = cursor;
        while (cursor < rows.size() && rows.get(cursor).getPosition() < firstRowOfBlock + rowsInBlock) {
          cursor++;
        }
        if (cursor > blockStart) {
          List<ReadRowsRequest.Row> wanted = rows.subList(blockStart, cursor);
          PageReadStore pages = readPages(reader, blockIndex, block, projection, wanted, firstRowOfBlock, rowsInBlock);
          emit(columnIOFactory, projection, fileSchema, pages, wanted, firstRowOfBlock, filePath, consumer);
        }

        firstRowOfBlock += rowsInBlock;
      }
    }
  }

  /**
   * Reads only the pages holding the wanted rows if the file has a page index, else the
   * whole row group. Each projected column's pages narrow the rows read, so a narrow column
   * such as _op_type, whose one page spans thousands of rows, does not pull in every page of
   * a wide column around the wanted rows.
   */
  private static PageReadStore readPages(ParquetFileReader reader, int blockIndex, BlockMetaData block,
      MessageType projection, List<ReadRowsRequest.Row> wanted, long firstRowOfBlock, long rowsInBlock)
      throws IOException {
    RowRanges ranges = null;
    for (ColumnChunkMetaData chunk : block.getColumns()) {
      if (!projection.containsPath(chunk.getPath().toArray())) {
        continue;
      }
      OffsetIndex offsetIndex = reader.readOffsetIndex(chunk);
      if (offsetIndex == null) {
        return reader.readRowGroup(blockIndex);
      }
      RowRanges columnRanges = RowRanges.create(rowsInBlock,
          pagesHolding(offsetIndex, wanted, firstRowOfBlock, rowsInBlock), offsetIndex);
      ranges = ranges == null ? columnRanges : RowRanges.intersection(ranges, columnRanges);
    }

    return reader.readFilteredRowGroup(blockIndex, ranges);
  }

  /** Indexes, within one column chunk, of the pages holding the wanted rows. */
  private static PrimitiveIterator.OfInt pagesHolding(OffsetIndex offsetIndex, List<ReadRowsRequest.Row> wanted,
      long firstRowOfBlock, long rowsInBlock) {
    IntStream.Builder pages = IntStream.builder();
    int cursor = 0;
    for (int page = 0; page < offsetIndex.getPageCount() && cursor < wanted.size(); page++) {
      long lastRow = firstRowOfBlock + offsetIndex.getLastRowIndex(page, rowsInBlock);
      if (wanted.get(cursor).getPosition() > lastRow) {
        continue;
      }
      pages.add(page);
      while (cursor < wanted.size() && wanted.get(cursor).getPosition() <= lastRow) {
        cursor++;
      }
    }
    return pages.build().iterator();
  }

  /** Walks the rows read and emits the wanted ones. */
  private static void emit(ColumnIOFactory columnIOFactory, MessageType projection, MessageType fileSchema,
      PageReadStore pages, List<ReadRowsRequest.Row> wanted, long firstRowOfBlock, String filePath,
      RowConsumer consumer) throws Exception {
    MessageColumnIO columnIO = columnIOFactory.getColumnIO(projection, fileSchema, true);
    RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(projection), FilterCompat.NOOP);
    PrimitiveIterator.OfLong rowIndexes = pages.getRowIndexes().orElse(null);

    // every wanted row is among the rows read, which come in position order
    int cursor = 0;
    for (long read = 0; cursor < wanted.size(); read++) {
      Group group = records.read();
      long position = firstRowOfBlock + (rowIndexes != null ? rowIndexes.nextLong() : read);
      if (position != wanted.get(cursor).getPosition()) {
        continue;
      }

      List<String> rowColumns = wanted.get(cursor).getColumnsList();
      Map<String, Object> values = new HashMap<>(rowColumns.size());
      for (String column : rowColumns) {
        if (projection.containsField(column)) {
          values.put(column, value(group, projection.getFieldIndex(column)));
        }
      }
      consumer.accept(filePath, new Row(position, values));
      cursor++;
    }
  }

  /**
   * Reads one field of a row, or null when the column is null in that row. The columns read
   * are text (Postgres text, json, arrays and the like, _op_type, the stored row) or numeric
   * stored as double.
   */
  private static Object value(Group group, int slot) {
    if (group.getFieldRepetitionCount(slot) == 0) {
      return null;
    }
    return group.getType().getType(slot).asPrimitiveType().getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE
        ? group.getDouble(slot, 0)
        : group.getString(slot, 0);
  }

  /**
   * Adapts an Iceberg file to Parquet's InputFile, so reads go through the table's FileIO.
   * Iceberg's own adapter ({@code ParquetIO}) is package private.
   */
  private record ParquetInput(InputFile file) implements org.apache.parquet.io.InputFile {
    @Override
    public long getLength() {
      return file.getLength();
    }

    @Override
    public org.apache.parquet.io.SeekableInputStream newStream() {
      SeekableInputStream stream = file.newStream();

      // Parquet provides the read methods; only position and seek are needed.
      return new DelegatingSeekableInputStream(stream) {
        @Override
        public long getPos() throws IOException {
          return stream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
          stream.seek(newPos);
        }
      };
    }
  }
}
