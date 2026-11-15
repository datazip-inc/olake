package io.olake.iceberg.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsBatch;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsRequest;
import io.olake.iceberg.tableoperator.DeleteMode;

/**
 * Reads rows back through ReadRows as the Go side does, checking where and how the stream ends.
 */
class OlakeTableIndexerTest {
    private static final int ROWS = 100;
    private static final String THREAD = "thread";

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
            Types.NestedField.optional(2, "_op_type", Types.StringType.get()),
            Types.NestedField.optional(3, "payload", Types.StringType.get()));

    @TempDir
    Path dir;

    private OlakeTableIndexer indexer;
    private Table table;

    @BeforeEach
    void writeFiles() throws Exception {
        table = new HadoopTables(new Configuration()).create(SCHEMA, PartitionSpec.unpartitioned(), Map.of(),
                Files.createTempDirectory(dir, "table").toString());
        writeFile("a.parquet");
        writeFile("b.parquet");

        ConcurrentMap<String, IcebergSession> sessions = new ConcurrentHashMap<>();
        sessions.put(THREAD, new IcebergSession(table, true, "_olake_id", DeleteMode.POSITION));
        indexer = new OlakeTableIndexer(sessions);
    }

    /** Writes ROWS rows to the named file; ReadRows reads any path it is given. */
    private void writeFile(String file) throws Exception {
        try (FileAppender<Record> appender = new GenericAppenderFactory(SCHEMA)
                .newAppender(table.io().newOutputFile(path(file)), FileFormat.PARQUET)) {
            for (int position = 0; position < ROWS; position++) {
                GenericRecord record = GenericRecord.create(SCHEMA);
                record.setField("_olake_id", file + position);
                record.setField("_op_type", "u");
                record.setField("payload", "payload " + position);
                appender.add(record);
            }
        }
    }

    /** Keeps the stream's messages and how it ended. */
    private static final class Collected implements StreamObserver<ReadRowsBatch> {
        final List<ReadRowsBatch.Row> rows = new ArrayList<>();
        final CountDownLatch ended = new CountDownLatch(1);
        int messages;
        int completions;
        Throwable error;

        @Override
        public void onNext(ReadRowsBatch batch) {
            messages++;
            rows.addAll(batch.getRowsList());
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            ended.countDown();
        }

        @Override
        public void onCompleted() {
            completions++;
            ended.countDown();
        }
    }

    /** Asks every row of each named file, _op_type and payload, and waits for the stream to end. */
    private Collected readRows(String threadId, long maxBytes, String... files) throws Exception {
        ReadRowsRequest.Builder request = ReadRowsRequest.newBuilder().setThreadId(threadId).setMaxBytes(maxBytes);
        for (String file : files) {
            ReadRowsRequest.FileRows.Builder rows = ReadRowsRequest.FileRows.newBuilder().setFilePath(path(file));
            for (int position = 0; position < ROWS; position++) {
                rows.addRows(ReadRowsRequest.Row.newBuilder().setPosition(position)
                        .addColumns("_op_type").addColumns("payload"));
            }
            request.addFiles(rows);
        }

        Collected collected = new Collected();
        indexer.readRows(request.build(), collected);
        assertTrue(collected.ended.await(30, TimeUnit.SECONDS), "the stream ends");
        return collected;
    }

    private String path(String file) {
        return dir.resolve(file).toString();
    }

    /** The bytes Java counts against max_bytes. */
    private static long bytes(List<ReadRowsBatch.Row> rows) {
        return rows.stream().mapToLong(ReadRowsBatch.Row::getSerializedSize).sum();
    }

    static Stream<Arguments> maxBytesCases() {
        return Stream.of(
                // max_bytes is the bytes of the file's first rows plus extra bytes
                Arguments.of("a max_bytes below the first row still sends that row", 0, 1L, 1, 1),
                Arguments.of("a max_bytes a byte short of ten rows stops after the tenth", 10, -1L, 10, 1),
                // only passing max_bytes stops the stream, not meeting it
                Arguments.of("a max_bytes ten rows meet exactly sends the eleventh too", 10, 0L, 11, 1),
                Arguments.of("a max_bytes every row fits in sends them all", ROWS, 0L, ROWS, 2),
                // the proto default; Go always sends a budget
                Arguments.of("an unset max_bytes sends every row", 0, 0L, ROWS, 2));
    }

    // Go reads the stream to its end and never cancels it, so Java itself stops once the rows sent
    // pass max_bytes; the row that passes it is still sent, so every call makes progress
    @ParameterizedTest(name = "{0}")
    @MethodSource("maxBytesCases")
    @DisplayName("the stream ends normally after the row that takes the bytes sent past max_bytes")
    void stopsAfterTheRowThatPassesMaxBytes(String name, int fittingRows, long extraBytes, int expectedRows,
            int expectedMessages) throws Exception {
        List<ReadRowsBatch.Row> every = readRows(THREAD, Long.MAX_VALUE, "a.parquet").rows;

        Collected collected = readRows(THREAD, bytes(every.subList(0, fittingRows)) + extraBytes, "a.parquet");

        assertNull(collected.error);
        assertEquals(1, collected.completions);
        assertEquals(every.subList(0, expectedRows), collected.rows, "a file's first rows, in order");
        assertEquals(expectedMessages, collected.messages, "the row that passes max_bytes is the last sent");
    }

    // files are read concurrently, so the stop must hold across them: only the last row sent
    // passes max_bytes, and every other file stops at its next row
    @Test
    @DisplayName("reads of every file stop once the rows sent pass max_bytes")
    void stopsEveryFileOnceMaxBytesIsPassed() throws Exception {
        Collected collected = readRows(THREAD, 1000, "a.parquet", "b.parquet");

        assertNull(collected.error);
        assertEquals(1, collected.completions);
        assertTrue(collected.rows.size() < 2 * ROWS, "not every row asked is sent");
        assertTrue(bytes(collected.rows) > 1000, "the rows sent pass max_bytes");
        assertTrue(bytes(collected.rows.subList(0, collected.rows.size() - 1)) <= 1000,
                "only the last row takes them past it");
        for (String file : List.of("a.parquet", "b.parquet")) {
            long expected = 0;
            for (ReadRowsBatch.Row row : collected.rows) {
                if (row.getFilePath().equals(path(file))) {
                    assertEquals(expected++, row.getPosition(), file + " is sent from its first row on");
                }
            }
        }
    }

    // once max_bytes is passed, a file still waiting for a reader is skipped, so the call does not
    // open files for rows it will not send
    @Test
    @DisplayName("a file still queued once max_bytes is passed is not opened")
    void skipsFilesQueuedAfterTheLimit() throws Exception {
        // 4 files keep every reader busy, so the missing fifth is queued until one has sent a row
        Collected collected = readRows(THREAD, 1, "a.parquet", "b.parquet", "a.parquet", "b.parquet",
                "missing.parquet");

        assertNull(collected.error, "the missing file is never read");
        assertEquals(1, collected.completions);
        assertEquals(1, collected.messages);
        assertEquals(1, collected.rows.size(), "the row that passes max_bytes is sent, nothing after it");
    }

    // rows of all files share messages, so a call over many files does not send a message per file
    @Test
    @DisplayName("rows of every file are sent 64 to a message")
    void groupsRowsOfEveryFileIntoMessages() throws Exception {
        Collected collected = readRows(THREAD, Long.MAX_VALUE, "a.parquet", "b.parquet");

        assertNull(collected.error);
        assertEquals(1, collected.completions);
        assertEquals(2 * ROWS, collected.rows.size());
        assertEquals(4, collected.messages, "three full messages and one with the 8 rows left");
    }

    static Stream<Arguments> failureCases() {
        return Stream.of(
                Arguments.of("a file that cannot be read", THREAD, new String[] {"a.parquet", "missing.parquet"}),
                // fails before any file is read
                Arguments.of("a thread without a session", "other", new String[] {"a.parquet"}));
    }

    // a normal end would leave the rows unread, and Go would write the placeholder in their place,
    // so a call that cannot read its rows must fail
    @ParameterizedTest(name = "{0}")
    @MethodSource("failureCases")
    @DisplayName("a call that cannot read its rows fails")
    void failsTheCall(String name, String threadId, String[] files) throws Exception {
        Collected collected = readRows(threadId, Long.MAX_VALUE, files);

        assertNotNull(collected.error);
        assertEquals(Status.Code.INTERNAL, Status.fromThrowable(collected.error).getCode());
        assertEquals(0, collected.completions);
    }
}
