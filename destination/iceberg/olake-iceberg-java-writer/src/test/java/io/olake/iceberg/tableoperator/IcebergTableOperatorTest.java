package io.olake.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pins the olake_2pc state Go reads back as metadata state on the next sync.
 */
class IcebergTableOperatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String STATE_KEY_2PC = "olake_2pc";

    private Table table;

    @BeforeEach
    void setUp() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("db"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        table = catalog.createTable(TableIdentifier.of("db", "stream"), schema);
    }

    /** Commits a thread that produced no files, as a CDC or incremental sync with zero records does. */
    private long commitWithoutFiles(String threadId, String payload) {
        return new IcebergTableOperator(false).commitThread(threadId, payload, table, null);
    }

    /** Returns the stored LSN for the stream, from the state Go passes as the commit payload. */
    private String storedLSN() throws Exception {
        table.refresh();
        JsonNode root = MAPPER.readTree(table.properties().get(STATE_KEY_2PC));
        return MAPPER.readTree(root.get("state").asText()).get("lsn").asText();
    }

    @Test
    @DisplayName("sync without files commits the 2pc state")
    void commitsStateWithoutFiles() throws Exception {
        long snapshotId = commitWithoutFiles("cdc-thread", "{\"id\":\"cdc-thread\",\"state\":\"{\\\"lsn\\\":\\\"0/1\\\"}\"}");

        assertEquals(0L, snapshotId);
        assertNull(table.currentSnapshot(), "a state-only commit must not create a snapshot");
        assertEquals("0/1", storedLSN());
    }

    @Test
    @DisplayName("sync without files moves the 2pc state forward")
    void advancesStateWithoutFiles() throws Exception {
        commitWithoutFiles("cdc-thread", "{\"id\":\"cdc-thread\",\"state\":\"{\\\"lsn\\\":\\\"0/1\\\"}\"}");
        commitWithoutFiles("cdc-thread", "{\"id\":\"cdc-thread\",\"state\":\"{\\\"lsn\\\":\\\"0/2\\\"}\"}");

        assertEquals("0/2", storedLSN());
    }

    @Test
    @DisplayName("backfill without files leaves the 2pc state untouched")
    void skipsBackfillWithoutFiles() {
        long snapshotId = commitWithoutFiles("backfill-thread", "");

        assertEquals(0L, snapshotId);
        assertNull(table.properties().get(STATE_KEY_2PC));
    }
}
