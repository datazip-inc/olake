package io.debezium.server.iceberg.tableIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types.NestedField;
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
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the requested columns of rows addressed by {@code (data file, row position)}.
 * Skips row groups without a wanted row and, when the file has a page index, pages too;
 * without one it reads whole row groups (correct, but more data).
 */
public final class RowReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowReader.class);

  /** Marks a column the data file does not carry, which is not the same as a stored NULL. */
  public static final Object ABSENT = new Object();

  private RowReader() {
  }

  /** One row's projected values, in the order the columns were requested. */
  public static final class Row {
    public final long position;
    public final List<Object> values;

    private Row(long position, List<Object> values) {
      this.position = position;
      this.values = values;
    }
  }

  /** Receives rows as they are read, so neither side holds a whole file's values. */
  public interface RowConsumer {
    void accept(String filePath, Row row) throws Exception;
  }

  /**
   * Reads {@code positions} (ascending) of one data file and hands each row to {@code consumer}.
   * Positions past the end of the file are ignored.
   */
  public static void read(Table table, String filePath, List<Long> positions, List<String> columns, RowConsumer consumer)
      throws Exception {
    if (positions.isEmpty() || columns.isEmpty()) {
      return;
    }

    InputFile inputFile = table.io().newInputFile(filePath);
    // ParquetInput adapts Iceberg's file to Parquet's type, so the read keeps using the
    // table's FileIO and credentials.
    try (ParquetFileReader reader = ParquetFileReader.open(new ParquetInput(inputFile),
        ParquetReadOptions.builder().build())) {
      MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
      MessageType projection = project(table.schema(), fileSchema, columns);
      if (projection.getFieldCount() == 0) {
        LOGGER.warn("none of the requested columns {} exist in {}, skipping", columns, filePath);
        return;
      }
      reader.setRequestedSchema(projection);

      List<Integer> columnSlots = slots(projection, columns);
      ColumnIOFactory columnIOFactory = new ColumnIOFactory();
      int cursor = 0;
      long firstRowOfBlock = 0;

      for (int blockIndex = 0; blockIndex < reader.getRowGroups().size() && cursor < positions.size(); blockIndex++) {
        BlockMetaData block = reader.getRowGroups().get(blockIndex);
        long rowsInBlock = block.getRowCount();
        long lastRowOfBlock = firstRowOfBlock + rowsInBlock - 1;

        int blockStart = cursor;
        while (cursor < positions.size() && positions.get(cursor) <= lastRowOfBlock) {
          cursor++;
        }

        if (blockStart == cursor) {
          reader.skipNextRowGroup();
          firstRowOfBlock += rowsInBlock;
          continue;
        }

        List<Long> wanted = positions.subList(blockStart, cursor);
        PageReadStore pages = readPages(reader, blockIndex, block, projection, wanted, firstRowOfBlock, rowsInBlock);
        emit(columnIOFactory, projection, fileSchema, pages, columnSlots, wanted, firstRowOfBlock, filePath, consumer);

        firstRowOfBlock += rowsInBlock;
      }
    }
  }

  /**
   * Reads only the pages holding the wanted rows if the file has a page index, else the
   * whole row group.
   */
  private static PageReadStore readPages(ParquetFileReader reader, int blockIndex, BlockMetaData block,
      MessageType projection, List<Long> wanted, long firstRowOfBlock, long rowsInBlock) throws IOException {
    ColumnChunkMetaData chunk = chunkOf(block, projection.getFields().get(0).getName());
    OffsetIndex offsetIndex = chunk == null ? null : reader.readOffsetIndex(chunk);
    if (offsetIndex == null) {
      return reader.readNextRowGroup();
    }

    List<Integer> pages = new ArrayList<>();
    int cursor = 0;
    for (int page = 0; page < offsetIndex.getPageCount(); page++) {
      long firstRow = firstRowOfBlock + offsetIndex.getFirstRowIndex(page);
      long lastRow = firstRowOfBlock + offsetIndex.getLastRowIndex(page, rowsInBlock);
      while (cursor < wanted.size() && wanted.get(cursor) < firstRow) {
        cursor++;
      }
      if (cursor < wanted.size() && wanted.get(cursor) <= lastRow) {
        pages.add(page);
      }
    }

    if (pages.isEmpty()) {
      return reader.readNextRowGroup();
    }

    RowRanges ranges = RowRanges.create(rowsInBlock, pages.stream().mapToInt(Integer::intValue).iterator(), offsetIndex);
    PageReadStore pages2 = reader.readFilteredRowGroup(blockIndex, ranges);
    // readFilteredRowGroup does not advance the reader's own row-group cursor.
    reader.skipNextRowGroup();

    return pages2;
  }

  /** Walks the rows read and emits the wanted ones. */
  private static void emit(ColumnIOFactory columnIOFactory, MessageType projection, MessageType fileSchema,
      PageReadStore pages, List<Integer> columnSlots, List<Long> wanted, long firstRowOfBlock,
      String filePath, RowConsumer consumer) throws Exception {
    if (pages == null) {
      return;
    }

    MessageColumnIO columnIO = columnIOFactory.getColumnIO(projection, fileSchema, true);
    RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(projection), FilterCompat.NOOP);
    PrimitiveIterator.OfLong rowIndexes = pages.getRowIndexes().orElse(null);

    int cursor = 0;
    for (long read = 0; read < pages.getRowCount() && cursor < wanted.size(); read++) {
      Group group = records.read();
      long position = firstRowOfBlock + (rowIndexes != null ? rowIndexes.nextLong() : read);
      if (position < wanted.get(cursor)) {
        continue;
      }
      if (position > wanted.get(cursor)) {
        // passed a wanted row that was not returned: move to the next one
        while (cursor < wanted.size() && wanted.get(cursor) < position) {
          cursor++;
        }
        if (cursor >= wanted.size() || position != wanted.get(cursor)) {
          continue;
        }
      }

      List<Object> values = new ArrayList<>(columnSlots.size());
      for (Integer slot : columnSlots) {
        values.add(slot == null ? ABSENT : value(group, slot));
      }
      consumer.accept(filePath, new Row(position, values));
      cursor++;
    }
  }

  /** Reads one field of a row, or null when the column is null in that row. */
  private static Object value(Group group, int slot) {
    if (group.getFieldRepetitionCount(slot) == 0) {
      return null;
    }

    Type field = group.getType().getType(slot);
    switch (field.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        // strings are UTF-8 binary with a logical type; other binary is returned as bytes
        return field.getLogicalTypeAnnotation() == null
            ? group.getBinary(slot, 0).getBytes()
            : group.getString(slot, 0);
      case INT32:
        return (long) group.getInteger(slot, 0);
      case INT64:
        return group.getLong(slot, 0);
      case FLOAT:
        return (double) group.getFloat(slot, 0);
      case DOUBLE:
        return group.getDouble(slot, 0);
      case BOOLEAN:
        return group.getBoolean(slot, 0);
      default:
        return group.getValueToString(slot, 0);
    }
  }

  /**
   * Picks the requested columns from the file schema by Iceberg field id, so renames still
   * match, else by name.
   */
  private static MessageType project(Schema tableSchema, MessageType fileSchema, List<String> columns) {
    Map<Integer, Type> byId = new HashMap<>();
    Map<String, Type> byName = new HashMap<>();
    for (Type field : fileSchema.getFields()) {
      if (field.getId() != null) {
        byId.put(field.getId().intValue(), field);
      }
      byName.put(field.getName(), field);
    }

    List<Type> fields = new ArrayList<>(columns.size());
    for (String column : columns) {
      NestedField tableField = tableSchema.findField(column);
      Type field = tableField == null ? null : byId.get(tableField.fieldId());
      if (field == null) {
        field = byName.get(column);
      }
      if (field != null && !fields.contains(field)) {
        fields.add(field);
      }
    }

    return new MessageType(fileSchema.getName(), fields);
  }

  /** Position of each requested column inside the projection, or null when absent. */
  private static List<Integer> slots(MessageType projection, List<String> columns) {
    Map<String, Integer> byName = new HashMap<>();
    for (int slot = 0; slot < projection.getFieldCount(); slot++) {
      byName.put(projection.getType(slot).getName(), slot);
    }

    List<Integer> slots = new ArrayList<>(columns.size());
    for (String column : columns) {
      slots.add(byName.get(column));
    }

    return slots;
  }

  private static ColumnChunkMetaData chunkOf(BlockMetaData block, String columnName) {
    for (ColumnChunkMetaData chunk : block.getColumns()) {
      if (chunk.getPath().toDotString().equals(columnName)) {
        return chunk;
      }
    }
    return null;
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

  /** Row positions of one file, ascending and without duplicates. */
  public static List<Long> normalize(Collection<Long> positions) {
    List<Long> ordered = new ArrayList<>(positions);
    ordered.sort(Long::compare);
    List<Long> unique = new ArrayList<>(ordered.size());
    for (Long position : ordered) {
      if (unique.isEmpty() || !unique.get(unique.size() - 1).equals(position)) {
        unique.add(position);
      }
    }
    return unique;
  }
}
