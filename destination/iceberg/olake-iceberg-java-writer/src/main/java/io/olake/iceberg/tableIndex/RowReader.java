package io.olake.iceberg.tableIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore.MissingOffsetIndexException;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord.FieldValue;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsBatch;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsRequest;

/**
 * Reads the requested columns of rows addressed by {@code (data file, row position)}.
 * Skips row groups without a wanted row and, when the file has a page index, pages too;
 * without one it reads whole row groups (correct, but more data).
 */
public final class RowReader {
  /**
   * A row whose values pass this is sent as several parts, so one message stays under the 2 GB
   * gRPC limit however many large values the row holds: a message is then at most this, the
   * rows batched before it, and one value (Postgres caps a value at 1 GB).
   */
  static final int ROW_PART_BYTES = 16 * 1024 * 1024;

  /** Marks a soft-deleted row ("d"), and is repeated in every part of a split row. */
  private static final String OP_TYPE = "_op_type";

  private RowReader() {
  }

  /** Receives rows as they are read, so neither side holds a whole file's values. */
  public interface RowConsumer {
    void accept(ReadRowsBatch.Row row) throws Exception;
  }

  /**
   * Reads the given rows of one data file, each with its own columns, and hands each row to
   * {@code consumer} with the values of those columns. A column the data file does not carry
   * is left out, which is not the same as a stored NULL (an unset value).
   * {@code rows} are in ascending position order.
   *
   * <p>The file is projected to every column some row wants, since Parquet reads a set of
   * rows across all projected columns; each row is still returned with only its own.
   */
  public static void read(Table table, String filePath, List<ReadRowsRequest.Row> rows, RowConsumer consumer)
      throws Exception {
    Set<String> columns = new HashSet<>();
    rows.forEach(row -> columns.addAll(row.getColumnsList()));

    InputFile inputFile = table.io().newInputFile(filePath);
    // ParquetInput adapts Iceberg's file to Parquet's type, so the read keeps using the
    // table's FileIO and credentials.
    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInput(inputFile),
        ParquetReadOptions.builder().build())) {
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      // Matched by Iceberg field id, as Iceberg readers do; a column the file was written
      // without is left out.
      Schema selected = table.schema().select(columns);
      MessageType projection = ParquetSchemaUtil.pruneColumns(fileSchema, selected);
      reader.setRequestedSchema(projection);
      Map<String, Integer> slots = slots(selected, projection);

      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projection, fileSchema, true);
      List<BlockMetaData> blocks = reader.getRowGroups();
      int cursor = 0; // index in rows of the next row to read
      long firstRowOfBlock = 0; // file position of the current row group's first row

      for (int blockIndex = 0; blockIndex < blocks.size() && cursor < rows.size(); blockIndex++) {
        long rowsInBlock = blocks.get(blockIndex).getRowCount();

        // the rows from cursor on that fall in this row group
        int blockStart = cursor;
        while (cursor < rows.size() && rows.get(cursor).getPosition() < firstRowOfBlock + rowsInBlock) {
          cursor++;
        }
        if (cursor > blockStart) {
          List<ReadRowsRequest.Row> wanted = rows.subList(blockStart, cursor);
          PageReadStore pages = readPages(reader, blockIndex, projection, wanted, firstRowOfBlock, rowsInBlock);
          emit(columnIO, projection, slots, pages, wanted, firstRowOfBlock, filePath, consumer);
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
  private static PageReadStore readPages(ParquetFileReader reader, int blockIndex, MessageType projection,
      List<ReadRowsRequest.Row> wanted, long firstRowOfBlock, long rowsInBlock) throws IOException {
    // The reader caches this store and readFilteredRowGroup reuses it, so each offset index is fetched from storage once.
    ColumnIndexStore indexes = reader.getColumnIndexStore(blockIndex);
    RowRanges ranges = null;
    for (ColumnDescriptor column : projection.getColumns()) {
      OffsetIndex offsetIndex;
      try {
        offsetIndex = indexes.getOffsetIndex(ColumnPath.get(column.getPath()));
      } catch (MissingOffsetIndexException e) {
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

  /**
   * Where each table column sits in the projection, by field id. The file may store a column
   * under another name: Iceberg's Parquet writer escapes characters such as a space, so the
   * table's "big doc" is "big_x20doc" in the file.
   */
  private static Map<String, Integer> slots(Schema selected, MessageType projection) {
    Map<String, Integer> slots = new HashMap<>();
    for (int slot = 0; slot < projection.getFieldCount(); slot++) {
      slots.put(selected.findField(projection.getType(slot).getId().intValue()).name(), slot);
    }
    return slots;
  }

  /** Walks the rows read and emits the wanted ones. */
  private static void emit(MessageColumnIO columnIO, MessageType projection, Map<String, Integer> slots,
      PageReadStore pages, List<ReadRowsRequest.Row> wanted, long firstRowOfBlock, String filePath,
      RowConsumer consumer) throws Exception {
    RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(projection), FilterCompat.NOOP);
    PrimitiveIterator.OfLong rowIndexes = pages.getRowIndexes().orElse(null);
    Integer opTypeSlot = slots.get(OP_TYPE);

    // every wanted row is among the rows read, which come in position order
    int cursor = 0;
    for (long read = 0; cursor < wanted.size(); read++) {
      Group group = records.read();
      long position = firstRowOfBlock + (rowIndexes != null ? rowIndexes.nextLong() : read);
      if (position != wanted.get(cursor).getPosition()) {
        continue;
      }

      // a soft-deleted row has nothing to recover: only its _op_type goes back, which the
      // caller skips the row on
      boolean deleted = opTypeSlot != null && group.getFieldRepetitionCount(opTypeSlot) > 0
          && "d".equals(group.getString(opTypeSlot, 0));
      ReadRowsBatch.Row.Builder row = ReadRowsBatch.Row.newBuilder().setFilePath(filePath).setPosition(position);
      for (String column : wanted.get(cursor).getColumnsList()) {
        Integer slot = slots.get(column);
        if (slot != null && (!deleted || column.equals(OP_TYPE))) {
          row.putValues(column, value(group, slot));
        }
      }
      for (ReadRowsBatch.Row part : split(row.build(), ROW_PART_BYTES)) {
        consumer.accept(part);
      }
      cursor++;
    }
  }

  /**
   * Splits a row whose values pass maxBytes into parts of the same file and position, each
   * holding some of the columns and _op_type; the caller merges them. A part passes maxBytes
   * only by its one value.
   */
  static List<ReadRowsBatch.Row> split(ReadRowsBatch.Row row, int maxBytes) {
    // summed as long: the row's own size is an int and overflows past 2 GB
    long rowBytes = 0;
    for (FieldValue value : row.getValuesMap().values()) {
      rowBytes += value.getSerializedSize();
    }
    if (rowBytes <= maxBytes) {
      return List.of(row);
    }

    // Go asks for _op_type on every row and every data file carries it
    ReadRowsBatch.Row.Builder empty = ReadRowsBatch.Row.newBuilder()
        .setFilePath(row.getFilePath()).setPosition(row.getPosition())
        .putValues(OP_TYPE, row.getValuesOrThrow(OP_TYPE));

    List<ReadRowsBatch.Row> parts = new ArrayList<>();
    ReadRowsBatch.Row.Builder part = empty.clone();
    long partBytes = 0;
    for (Map.Entry<String, FieldValue> value : row.getValuesMap().entrySet()) {
      if (value.getKey().equals(OP_TYPE)) {
        continue;
      }
      long size = value.getValue().getSerializedSize();
      if (partBytes > 0 && partBytes + size > maxBytes) {
        parts.add(part.build());
        part = empty.clone();
        partBytes = 0;
      }
      part.putValues(value.getKey(), value.getValue());
      partBytes += size;
    }
    parts.add(part.build());

    return parts;
  }

  /**
   * Reads one field of a row, unset when the column is null in that row. Only text and double
   * columns are read: a value the source could not send is text (Postgres text, json, arrays
   * and the like) or numeric stored as double, and _op_type and the stored row are text.
   */
  private static FieldValue value(Group group, int slot) {
    if (group.getFieldRepetitionCount(slot) == 0) {
      return FieldValue.getDefaultInstance();
    }

    PrimitiveType type = group.getType().getType(slot).asPrimitiveType();
    if (type.getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE) {
      return FieldValue.newBuilder().setDoubleValue(group.getDouble(slot, 0)).build();
    }
    if (type.getLogicalTypeAnnotation() instanceof StringLogicalTypeAnnotation) {
      return FieldValue.newBuilder().setStringValue(group.getString(slot, 0)).build();
    }
    throw new IllegalArgumentException("cannot read back column " + type.getName() + " of type " + type
        + ": only string and double columns are supported");
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
