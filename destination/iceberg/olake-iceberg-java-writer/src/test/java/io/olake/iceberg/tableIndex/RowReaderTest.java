package io.olake.iceberg.tableIndex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload.IceRecord.FieldValue;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsBatch;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsRequest;

/**
 * Reads rows back by position, the way ReadRows does, from a file with many row groups and many
 * pages per column.
 */
class RowReaderTest {
    private static final int ROWS = 600;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
            Types.NestedField.optional(2, "_op_type", Types.StringType.get()),
            Types.NestedField.optional(3, "payload", Types.StringType.get()),
            Types.NestedField.optional(4, "amt", Types.DoubleType.get()),
            Types.NestedField.optional(5, "id", Types.LongType.get()),
            Types.NestedField.optional(6, "big doc", Types.StringType.get()));

    @TempDir
    Path dir;

    private Table table;
    private String filePath;

    /** Every column breaks pages on the same rows. */
    private static final Map<String, String> EVEN_PAGES = Map.of(TableProperties.PARQUET_PAGE_ROW_LIMIT, "10");

    /**
     * Pages end by size, without dictionaries: the narrow _op_type has one page per row group while
     * the wide payload has several, as the arrow writer lays them out.
     */
    private static final Map<String, String> UNEVEN_PAGES = Map.of(
            TableProperties.PARQUET_PAGE_ROW_LIMIT, "1000",
            TableProperties.PARQUET_PAGE_SIZE_BYTES, "1024",
            TableProperties.PARQUET_DICT_SIZE_BYTES, "1024");

    @BeforeEach
    void writeFile() throws Exception {
        writeFile(EVEN_PAGES);
    }

    /**
     * Writes 600 rows through Iceberg's own Parquet writer in row groups of a few pages, then adds
     * a table column the file does not carry.
     */
    private void writeFile(Map<String, String> pageLayout) throws Exception {
        Map<String, String> properties = new HashMap<>(pageLayout);
        properties.put(TableProperties.PARQUET_COMPRESSION, "uncompressed"); // sizes, so pages and row groups, as written
        properties.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "8192");
        properties.put(TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "10");
        properties.put(TableProperties.PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "10");
        Path location = Files.createTempDirectory(dir, "table");
        table = new HadoopTables(new Configuration()).create(SCHEMA, PartitionSpec.unpartitioned(), properties,
                location.toString());

        filePath = location.resolve("data.parquet").toString();
        try (FileAppender<Record> appender = new GenericAppenderFactory(SCHEMA).setAll(table.properties())
                .newAppender(table.io().newOutputFile(filePath), FileFormat.PARQUET)) {
            for (int position = 0; position < ROWS; position++) {
                GenericRecord record = GenericRecord.create(SCHEMA);
                record.setField("_olake_id", "k" + position);
                record.setField("_op_type", position % 7 == 0 ? "d" : "u");
                record.setField("payload", position % 13 == 0 ? null : payload(position));
                record.setField("amt", position * 1.5);
                record.setField("id", (long) position);
                record.setField("big doc", "doc" + position);
                appender.add(record);
            }
        }

        table.updateSchema().addColumn("later", Types.StringType.get()).commit();
    }

    static Stream<Arguments> pageLayoutCases() {
        return Stream.of(
                Arguments.of("pages end on the same rows in every column", EVEN_PAGES, false),
                // the pages read are the intersection over all columns, so a narrow column's large
                // page must not drop the wanted rows from a wide column's small ones
                Arguments.of("pages end on different rows per column", UNEVEN_PAGES, true));
    }

    // most pages are skipped, so a wanted row next to a page or row group edge is where an
    // off-by-one would read the neighbouring row instead
    @ParameterizedTest(name = "{0}")
    @MethodSource("pageLayoutCases")
    @DisplayName("rows on both sides of page and row group boundaries read back exactly")
    void readsRowsAcrossBoundaries(String name, Map<String, String> pageLayout, boolean pagesDiffer) throws Exception {
        writeFile(pageLayout);
        List<Long> rowGroupStarts = new ArrayList<>();
        try (ParquetFileReader reader = openFile()) {
            long start = 0;
            for (BlockMetaData block : reader.getRowGroups()) {
                rowGroupStarts.add(start);
                start += block.getRowCount();
            }
            List<ColumnChunkMetaData> chunks = reader.getRowGroups().get(0).getColumns();
            int opTypePages = reader.readOffsetIndex(chunks.get(1)).getPageCount();
            int payloadPages = reader.readOffsetIndex(chunks.get(2)).getPageCount();
            assertTrue(payloadPages > 1, "several pages per column chunk");
            assertEquals(pagesDiffer, opTypePages < payloadPages, "_op_type has " + opTypePages
                    + " page(s), payload " + payloadPages);
        }
        assertTrue(rowGroupStarts.size() > 2, "several row groups");

        // the first and last row of the file, both sides of a page boundary, a run of rows in
        // one page, and the rows around every row group boundary
        List<Long> positions = new ArrayList<>(List.of(0L, 9L, 10L, 11L, 12L, 13L, (long) ROWS - 1));
        for (long start : rowGroupStarts.subList(1, rowGroupStarts.size())) {
            positions.add(start - 1);
            positions.add(start);
        }
        positions = positions.stream().distinct().sorted().toList();

        // each row asks for its own columns
        List<ReadRowsRequest.Row> rows = new ArrayList<>();
        for (long position : positions) {
            List<String> columns = position % 2 == 0 ? List.of("_op_type", "payload") : List.of("_op_type", "amt");
            rows.add(ReadRowsRequest.Row.newBuilder().setPosition(position).addAllColumns(columns).build());
        }

        Map<Long, ReadRowsBatch.Row> read = read(rows);

        assertEquals(positions, read.keySet().stream().sorted().toList());
        for (long position : positions) {
            Map<String, FieldValue> values = read.get(position).getValuesMap();
            assertEquals(filePath, read.get(position).getFilePath());
            assertEquals(2, values.size());
            assertEquals(position % 7 == 0 ? "d" : "u", values.get("_op_type").getStringValue(),
                    "_op_type of " + position);
            if (position % 2 == 1) {
                assertEquals(position * 1.5, values.get("amt").getDoubleValue(), "amt of " + position);
            } else if (position % 13 == 0) {
                assertEquals(FieldValue.ValueCase.VALUE_NOT_SET, values.get("payload").getValueCase(),
                        "stored NULL at " + position);
            } else {
                assertEquals(payload((int) position), values.get("payload").getStringValue(),
                        "payload of " + position);
            }
        }
    }

    static Stream<Arguments> columnCases() {
        return Stream.of(
                // added to the table after the file was written: left out, not read as a NULL
                Arguments.of("a column the file was written without is left out", "later", null, null),
                // Iceberg's writer escapes the space, so the file stores it as big_x20doc
                Arguments.of("a column the file stores under an escaped name is read",
                        "big doc", "big_x20doc", "doc7"));
    }

    // a table column is found in the file by Iceberg field id, never by its table name
    @ParameterizedTest(name = "{0}")
    @MethodSource("columnCases")
    @DisplayName("table columns are matched to the file by field id")
    void matchesColumnsByFieldId(String name, String column, String fileColumn, String expected) throws Exception {
        try (ParquetFileReader reader = openFile()) {
            MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
            assertFalse(fileSchema.containsField(column), "the file does not carry the table name");
            if (fileColumn != null) {
                assertTrue(fileSchema.containsField(fileColumn), "the file carries the column as " + fileColumn);
            }
        }

        // Go always asks for _op_type too, so a row is read even when no other column is in the file
        Map<String, FieldValue> values = read(List.of(ReadRowsRequest.Row.newBuilder()
                .setPosition(7).addColumns("_op_type").addColumns(column).build())).get(7L).getValuesMap();

        if (expected == null) {
            assertFalse(values.containsKey(column), "a column the file does not carry must be left out");
        } else {
            assertEquals(expected, values.get(column).getStringValue());
        }
    }

    // only text and double columns can hold a value the source left out; anything else must fail
    // the read rather than reach Go as a wrong value
    @Test
    @DisplayName("a column that is neither text nor double fails the read")
    void failsOnUnsupportedColumnTypes() {
        assertThrows(IllegalArgumentException.class,
                () -> read(List.of(ReadRowsRequest.Row.newBuilder().setPosition(5).addColumns("id").build())));
    }

    /** Reads the rows, keyed by position; a position emitted twice fails the test. */
    private Map<Long, ReadRowsBatch.Row> read(List<ReadRowsRequest.Row> rows) throws Exception {
        Map<Long, ReadRowsBatch.Row> read = new HashMap<>();
        RowReader.read(table, filePath, rows, row -> assertNull(read.put(row.getPosition(), row)));
        return read;
    }

    private ParquetFileReader openFile() throws Exception {
        return ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(filePath),
                new Configuration()), ParquetReadOptions.builder().build());
    }

    /** Large enough that a row group holds only a few pages. */
    private static String payload(int position) {
        return ("v" + position + "-").repeat(20);
    }
}
