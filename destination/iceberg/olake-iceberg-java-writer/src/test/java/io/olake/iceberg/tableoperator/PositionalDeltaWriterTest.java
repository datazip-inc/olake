package io.olake.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.olake.iceberg.rpc.RecordIngest.ReadRowsBatch;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsRequest;
import io.olake.iceberg.tableIndex.RowReader;

/**
 * closeOpen, which ReadRows uses to read rows the legacy writer has not committed yet.
 */
class PositionalDeltaWriterTest {
    private static final Schema SCHEMA = new Schema(List.of(
            Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
            Types.NestedField.optional(2, "payload", Types.StringType.get()),
            Types.NestedField.optional(3, "region", Types.StringType.get())), Set.of(1));

    @TempDir
    Path dir;

    private Table table;
    private PositionalDeltaWriter writer;
    private Record lastWritten;

    /** An unpartitioned positional-delete writer holding two rows in one open file. */
    @BeforeEach
    void writeRows() throws Exception {
        writer = newWriter(PartitionSpec.unpartitioned());
        writer.write(row("1", "a", "eu"));
        lastWritten = row("2", "b", "eu");
        writer.write(lastWritten);
    }

    static Stream<Arguments> untouchedPathCases() {
        return Stream.of(
                // ReadRows names every file it reads, most of them committed long ago
                Arguments.of("a path the writer never held", false),
                // a later batch reads the same file again after an earlier ReadRows closed it
                Arguments.of("a path closed by an earlier call", true));
    }

    // only a file the writer still has open may be closed; any other path is a no-op
    @ParameterizedTest(name = "{0}")
    @MethodSource("untouchedPathCases")
    @DisplayName("a path the writer does not have open closes nothing")
    void ignoresPathsNotOpen(String name, boolean closedEarlier) throws Exception {
        String path = closedEarlier ? writer.currentPath(lastWritten).toString() : "elsewhere.parquet";
        if (closedEarlier) {
            assertTrue(writer.closeOpen(Set.of(path)));
        }

        assertFalse(writer.closeOpen(Set.of(path)));

        assertEquals(1, writer.complete().dataFiles().length, "no file was rolled by the second call");
    }

    // write, close the open file, write again: the closed file reads back before the commit,
    // and the writer carries on in a new file
    @Test
    @DisplayName("an open file is closed for reading and the writer keeps going")
    void closesTheRequestedFileAndKeepsWriting() throws Exception {
        String closedPath = writer.currentPath(lastWritten).toString();

        assertTrue(writer.closeOpen(Set.of(closedPath)));

        // the routing cache pointed at the closed file: the same record now routes to a new one
        String nextPath = writer.currentPath(lastWritten).toString();
        assertNotEquals(closedPath, nextPath);

        writer.write(row("3", "c", "eu"));

        assertEquals(List.of("a", "b"), payloads(closedPath, 0, 1));
        WriteResult result = writer.complete();
        assertEquals(Map.of(closedPath, 2L, nextPath, 1L), recordCounts(result));
        assertEquals(0, result.deleteFiles().length);
    }

    // an UPDATE later in the same batch supersedes a row that now sits in the closed file; the
    // writer still remembers where it landed, so the old version gets its positional delete
    @Test
    @DisplayName("a row in a closed file is still superseded by a later write of the same key")
    void supersedesRowsInClosedFiles() throws Exception {
        String closedPath = writer.currentPath(lastWritten).toString();
        assertTrue(writer.closeOpen(Set.of(closedPath)));

        writer.write(row("1", "a2", "eu"));

        WriteResult result = writer.complete();
        assertEquals(1, result.deleteFiles().length, "the old version of key 1 is deleted");
        assertEquals(List.of(closedPath), Arrays.stream(result.referencedDataFiles()).map(CharSequence::toString).toList(),
                "the delete addresses the closed file");
        assertEquals(2L, recordCounts(result).get(closedPath));
    }

    // a partitioned table keeps one open file per partition; only the requested one is closed
    @Test
    @DisplayName("only the requested partition's file is closed")
    void closesOnlyTheRequestedPartition() throws Exception {
        writer.abort(); // the unpartitioned writer from setup is not used here
        writer = newWriter(PartitionSpec.builderFor(SCHEMA).identity("region").build());
        Record eu = row("1", "a", "eu");
        Record us = row("2", "b", "us");
        writer.write(eu);
        writer.write(us);
        String euPath = writer.currentPath(eu).toString();
        String usPath = writer.currentPath(us).toString();

        assertTrue(writer.closeOpen(Set.of(euPath)));

        assertEquals(usPath, writer.currentPath(us).toString(), "the other partition keeps its open file");
        Record euAgain = row("3", "c", "eu");
        writer.write(euAgain);
        String euNext = writer.currentPath(euAgain).toString();
        assertNotEquals(euPath, euNext);

        assertEquals(Map.of(euPath, 1L, euNext, 1L, usPath, 1L), recordCounts(writer.complete()));
    }

    /** A positional-delete writer for a new table, built the way the table operator builds it. */
    private PositionalDeltaWriter newWriter(PartitionSpec spec) {
        table = new HadoopTables(new Configuration()).create(SCHEMA, spec, Map.of(),
                dir.resolve(spec.isUnpartitioned() ? "unpartitioned" : "partitioned").toString());
        IcebergTableWriterFactory factory = new IcebergTableWriterFactory();
        factory.deleteMode = DeleteMode.POSITION;
        factory.identifierField = "_olake_id";
        return (PositionalDeltaWriter) factory.create(table);
    }

    private List<String> payloads(String path, long... positions) throws Exception {
        List<ReadRowsRequest.Row> rows = Arrays.stream(positions)
                .mapToObj(position -> ReadRowsRequest.Row.newBuilder().setPosition(position).addColumns("payload").build())
                .toList();
        List<ReadRowsBatch.Row> read = new ArrayList<>();
        RowReader.read(table, path, rows, read::add);
        return read.stream().map(row -> row.getValuesMap().get("payload").getStringValue()).toList();
    }

    private static Map<String, Long> recordCounts(WriteResult result) {
        return Arrays.stream(result.dataFiles()).collect(Collectors.toMap(DataFile::location, DataFile::recordCount));
    }

    private static Record row(String id, String payload, String region) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("_olake_id", id);
        record.setField("payload", payload);
        record.setField("region", region);
        return new RecordWrapper(record, Operation.CREATE);
    }
}
