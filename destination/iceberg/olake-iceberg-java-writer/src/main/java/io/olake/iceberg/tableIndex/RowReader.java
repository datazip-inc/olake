package io.olake.iceberg.tableIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.protobuf.ByteString;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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
 * Reads the requested columns of rows addressed by {@code (data file, row position)}, one row at a
 * time. A row costs the pages holding it when the file has a page index, else its whole row group
 * (correct, but more data). Open it for the rows of one file, {@link #read} each, close it.
 */
public final class RowReader implements AutoCloseable {
  /**
   * A row whose values pass this is sent as several parts, so one message stays under the 2 GB
   * gRPC limit however many large values the row holds: a message is then at most this, the
   * rows batched before it, and one value (Postgres caps a value at 1 GB).
   */
  private static final int ROW_PART_BYTES = 16 * 1024 * 1024;

  /** Marks a soft-deleted row ("d"). */
  private static final String OP_TYPE = "_op_type";

  private final String filePath;
  private final ParquetFileReader reader;
  private final List<BlockMetaData> blocks;
  private final long[] blockStarts; // file position of each row group's first row, ascending
  // the columns read are fixed when the reader opens: the parquet reader caches each row group's page
  // index for the columns requested then
  private final MessageType projection;
  private final Map<String, Integer> slots;
  private final MessageColumnIO columnIO;

  /** Opens a data file to read the given rows of it, whose columns together fix the columns read. */
  public RowReader(Table table, String filePath, List<ReadRowsRequest.Row> rows) throws IOException {
    this.filePath = filePath;
    // ParquetInput adapts Iceberg's file to Parquet's type, so the read keeps using the
    // table's FileIO and credentials.
    this.reader = ParquetFileReader.open(new ParquetInput(table.io().newInputFile(filePath)),
        ParquetReadOptions.builder().build());
    this.blocks = reader.getRowGroups();
    this.blockStarts = new long[blocks.size()];
    long start = 0;
    for (int blockIndex = 0; blockIndex < blocks.size(); blockIndex++) {
      blockStarts[blockIndex] = start;
      start += blocks.get(blockIndex).getRowCount();
    }

    MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
    // Matched by Iceberg field id, as Iceberg readers do; a column the file was written
    // without is left out of the projection.
    Schema selected = table.schema().select(columnsOf(rows));
    this.projection = ParquetSchemaUtil.pruneColumns(fileSchema, selected);
    reader.setRequestedSchema(projection);
    this.slots = slots(selected, projection);
    this.columnIO = new ColumnIOFactory().getColumnIO(projection, fileSchema, true);
  }

  /** The columns the rows ask for, together. */
  private static Set<String> columnsOf(List<ReadRowsRequest.Row> rows) {
    Set<String> columns = new HashSet<>();
    rows.forEach(row -> columns.addAll(row.getColumnsList()));
    return columns;
  }

  /**
   * Reads one of the rows the reader was opened for and hands it to consumer with the values of
   * its columns. A column the file does not carry comes back unset, as a stored NULL does: neither
   * has a value to recover.
   */
  public void read(ReadRowsRequest.Row row, Consumer<ReadRowsBatch.Row> consumer) throws IOException {
    int blockIndex = blockOf(row.getPosition());
    emit(readPages(blockIndex, row.getPosition()), row, blockStarts[blockIndex], consumer);
  }

  /** The row group holding the row at position. */
  private int blockOf(long position) {
    int found = Arrays.binarySearch(blockStarts, position);
    return found >= 0 ? found : -found - 2; // the row group starting before the insertion point
  }

  /**
   * Reads only the pages holding the row if the file has a page index, else its whole row group.
   * Every column narrows the rows read, so _op_type's one big page does not pull in every page of
   * a wide column.
   */
  private PageReadStore readPages(int blockIndex, long position) throws IOException {
    long rowsInBlock = blocks.get(blockIndex).getRowCount();
    long rowInBlock = position - blockStarts[blockIndex];
    // The reader caches this store and readFilteredRowGroup reuses it, so each offset index is fetched from storage once.
    ColumnIndexStore indexes = reader.getColumnIndexStore(blockIndex);
    RowRanges ranges = RowRanges.createSingle(rowsInBlock);
    for (ColumnDescriptor column : projection.getColumns()) {
      OffsetIndex offsetIndex;
      try {
        offsetIndex = indexes.getOffsetIndex(ColumnPath.get(column.getPath()));
      } catch (MissingOffsetIndexException e) {
        return reader.readRowGroup(blockIndex);
      }
      int page = pageHolding(offsetIndex, rowInBlock, rowsInBlock);
      ranges = RowRanges.intersection(ranges, RowRanges.create(rowsInBlock, IntStream.of(page).iterator(), offsetIndex));
    }

    return reader.readFilteredRowGroup(blockIndex, ranges);
  }

  /** Index, within one column chunk, of the page holding the row (given by its index within the row group). */
  private static int pageHolding(OffsetIndex offsetIndex, long rowInBlock, long rowsInBlock) {
    int page = 0;
    while (rowInBlock > offsetIndex.getLastRowIndex(page, rowsInBlock)) {
      page++;
    }
    return page;
  }

  /**
   * Walks the rows read to the wanted one and emits it. A row whose values pass ROW_PART_BYTES goes
   * out in parts, each with the same file and position and some of the columns; Go merges them.
   */
  private void emit(PageReadStore pages, ReadRowsRequest.Row row, long firstRowOfBlock, Consumer<ReadRowsBatch.Row> consumer) {
    RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(projection), FilterCompat.NOOP);
    // a whole row group read carries no row indexes: its rows come in order from 0
    PrimitiveIterator.OfLong rowIndexes = pages.getRowIndexes()
        .orElseGet(() -> LongStream.range(0, pages.getRowCount()).iterator());

    // the wanted row is among the rows read, which come in position order
    Group group;
    do {
      group = records.read();
    } while (firstRowOfBlock + rowIndexes.nextLong() != row.getPosition());

    // a soft-deleted row has nothing to recover: every column except _op_type goes back unset. A
    // file written before olake took the table over has no _op_type, so its rows are live.
    Integer opTypeSlot = slots.get(OP_TYPE);
    boolean deleted = opTypeSlot != null && "d".equals(group.getString(opTypeSlot, 0));
    ReadRowsBatch.Row.Builder part = ReadRowsBatch.Row.newBuilder().setFilePath(filePath).setPosition(row.getPosition());
    long partBytes = 0;
    for (String column : row.getColumnsList()) {
      Integer slot = slots.get(column);
      FieldValue value = slot == null || (deleted && !column.equals(OP_TYPE))
          ? FieldValue.getDefaultInstance() : value(group, slot);
      if (partBytes > 0 && partBytes + value.getSerializedSize() > ROW_PART_BYTES) {
        consumer.accept(part.build());
        part.clearValues();
        partBytes = 0;
      }
      part.putValues(column, value);
      partBytes += value.getSerializedSize();
    }
    consumer.accept(part.build());
  }

  @Override
  public void close() throws IOException {
    reader.close();
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

  /**
   * Reads one field of a row, unset when it is NULL. Only text and double columns can hold a value
   * the source left out (Postgres numeric is stored as double); text that is not UTF-8 goes as bytes.
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
      ByteString text = ByteString.copyFrom(group.getBinary(slot, 0).toByteBuffer());
      // A proto string must be UTF-8. The arrow writer stores whatever bytes a string column held
      // (Postgres bytea is one), so such text goes back as bytes and is written back as stored.
      return text.isValidUtf8()
          ? FieldValue.newBuilder().setStringValueBytes(text).build()
          : FieldValue.newBuilder().setBytesValue(text).build();
    }
    throw new IllegalArgumentException("cannot read back column " + type.getName() + " of type " + type
        + ": only string and double columns are supported");
  }

  /**
   * Adapts an Iceberg file to Parquet's InputFile, so reads go through the table's FileIO.
   * Iceberg's own adapter ({@code ParquetIO}) is package private.
   */
  private record ParquetInput(org.apache.iceberg.io.InputFile file) implements org.apache.parquet.io.InputFile {
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
