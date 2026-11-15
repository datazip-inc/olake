package io.olake.iceberg.tableIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.protobuf.ByteString;

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
  private static final int ROW_PART_BYTES = 16 * 1024 * 1024;

  /** Marks a soft-deleted row ("d"). */
  private static final String OP_TYPE = "_op_type";

  private RowReader() {
  }

  /** Receives rows as they are read, so neither side holds a whole file's values. */
  public interface RowConsumer {
    void accept(ReadRowsBatch.Row row) throws Exception;
  }

  /**
   * Reads the given rows of one data file, in ascending position order, and hands each to consumer
   * with the values of its own columns. A column the file does not carry comes back unset, as a
   * stored NULL does: neither has a value to recover.
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
      // without is left out of the projection.
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
   * Reads only the pages holding the wanted rows if the file has a page index, else the whole row
   * group. Every column narrows the rows read, so _op_type's one big page does not pull in every
   * page of a wide column.
   */
  private static PageReadStore readPages(ParquetFileReader reader, int blockIndex, MessageType projection,
      List<ReadRowsRequest.Row> wanted, long firstRowOfBlock, long rowsInBlock) throws IOException {
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
      RowRanges columnRanges = RowRanges.create(rowsInBlock,
          pagesHolding(offsetIndex, wanted, firstRowOfBlock, rowsInBlock), offsetIndex);
      ranges = RowRanges.intersection(ranges, columnRanges);
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

  /**
   * Walks the rows read and emits the wanted ones. A row whose values pass ROW_PART_BYTES goes out
   * in parts, each with the same file and position and some of the columns; Go merges them.
   */
  private static void emit(MessageColumnIO columnIO, MessageType projection, Map<String, Integer> slots,
      PageReadStore pages, List<ReadRowsRequest.Row> wanted, long firstRowOfBlock, String filePath,
      RowConsumer consumer) throws Exception {
    RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(projection), FilterCompat.NOOP);
    // a whole row group read carries no row indexes: its rows come in order from 0
    PrimitiveIterator.OfLong rowIndexes = pages.getRowIndexes()
        .orElseGet(() -> LongStream.range(0, pages.getRowCount()).iterator());
    Integer opTypeSlot = slots.get(OP_TYPE);

    // every wanted row is among the rows read, which come in position order
    int cursor = 0;
    while (cursor < wanted.size()) {
      Group group = records.read();
      long position = firstRowOfBlock + rowIndexes.nextLong();
      if (position != wanted.get(cursor).getPosition()) {
        continue;
      }

      // a soft-deleted row has nothing to recover: every column except _op_type goes back unset
      boolean deleted = opTypeSlot != null && group.getFieldRepetitionCount(opTypeSlot) > 0
          && "d".equals(group.getString(opTypeSlot, 0));
      ReadRowsBatch.Row.Builder part = ReadRowsBatch.Row.newBuilder().setFilePath(filePath).setPosition(position);
      long partBytes = 0;
      for (String column : wanted.get(cursor++).getColumnsList()) {
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
