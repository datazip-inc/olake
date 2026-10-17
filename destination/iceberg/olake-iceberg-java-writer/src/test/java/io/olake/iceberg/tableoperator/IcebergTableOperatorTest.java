package io.olake.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
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

    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "stream");

    private InMemoryCatalog catalog;
    private Table table;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("db"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        table = catalog.createTable(TABLE_ID, schema);
    }

    /** Commits a thread that produced no files, as a CDC or incremental sync with zero records does. */
    private long commitWithoutFiles(String threadId, String payload) {
        return new IcebergTableOperator(false).commitThread(threadId, payload, table, null);
    }

    /** Returns the stored state for the stream, from the state Go passes as the commit payload. */
    private JsonNode storedState() throws Exception {
        table.refresh();
        JsonNode root = MAPPER.readTree(table.properties().get(STATE_KEY_2PC));
        return MAPPER.readTree(root.get("state").asText());
    }

    private String storedLSN() throws Exception {
        return storedState().get("lsn").asText();
    }

    /** Delegating operations that run a competing commit once, between the caller's refresh and its commit. */
    private static class RacingOperations implements TableOperations {
        private final TableOperations delegate;
        private Runnable race;

        RacingOperations(TableOperations delegate, Runnable race) {
            this.delegate = delegate;
            this.race = race;
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            return delegate.refresh();
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            if (race != null) {
                Runnable competing = race;
                race = null;
                competing.run();
            }
            delegate.commit(base, metadata);
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }
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

    @Test
    @DisplayName("concurrent state-only commits keep both writers' state")
    void keepsConcurrentWriterState() throws Exception {
        Runnable competingWriter = () -> new IcebergTableOperator(false).commitThread("writer-b",
                "{\"id\":\"writer-b\",\"state\":\"{\\\"p1\\\":\\\"20\\\"}\"}", catalog.loadTable(TABLE_ID), null);
        TableOperations racing = new RacingOperations(((HasTableOperations) table).operations(), competingWriter);
        Table racingTable = new BaseTable(racing, table.name());

        new IcebergTableOperator(false).commitThread("writer-a",
                "{\"id\":\"writer-a\",\"state\":\"{\\\"p0\\\":\\\"10\\\"}\"}", racingTable, null);

        JsonNode state = storedState();
        assertEquals("10", state.path("p0").asText());
        assertEquals("20", state.path("p1").asText(), "the competing writer's state must not be reverted");
    }
}
