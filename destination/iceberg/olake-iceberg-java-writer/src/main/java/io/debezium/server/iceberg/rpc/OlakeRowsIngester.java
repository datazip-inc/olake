package io.debezium.server.iceberg.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.SchemaConvertor;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

/**
 * Multi-Thread-Session gRPC service for the legacy (rows-based) Iceberg write path.
 *
 * Design mirrors the old N-JVM model exactly, but inside one process:
 *   old model  → each JVM owned one Table handle + one IcebergTableOperator
 *   new model  → each ThreadSession owns one Table handle + one IcebergTableOperator
 *
 * The single {@link #sessions} map (keyed by threadId) is the only shared
 * state. There are no cross-session table caches or locks — every session is
 * fully isolated from every other session, just as separate JVM processes were.
 *
 * Per-stream context (namespace, upsert, partition spec, identifier-field flag)
 * arrives once on the GET_OR_CREATE_TABLE payload and is captured on the
 * session; later payloads carry only thread_id (+ evolving schema), so the JVM
 * still needs no global config.
 */
@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private final Catalog icebergCatalog;

    // Shared sessions map (one entry per active Go writer thread)
    // Used by both OlakeRowsIngester and OlakeArrowIngester
    private final ConcurrentMap<String, IcebergSession> sessions;

    public OlakeRowsIngester(Catalog icebergCatalog, ConcurrentMap<String, IcebergSession> sessions) {
        this.icebergCatalog = icebergCatalog;
        this.sessions = sessions;
    }

    @Override
    public void sendRecords(IcebergPayload request, StreamObserver<RecordIngest.RecordIngestResponse> responseObserver) {
        String requestId = String.format("[Thread-%d-%d]", Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();

        try {
            IcebergPayload.Metadata metadata = request.getMetadata();
            String threadId = metadata.getThreadId();
            String destTableName = metadata.getDestTableName();

            if (threadId == null || threadId.isEmpty()) {
                throw new Exception("Thread id not present in metadata");
            }

            // CLOSE_SESSION: release the session's Table handle and operator.
            // Mirrors what process exit did for free in the old per-JVM model.
            if (request.getType() == IcebergPayload.PayloadType.CLOSE_SESSION) {
                 // Give active RECORDS/COMMIT gRPC requests on this cancelled thread some time to check isCancelled(),
                // call closeQuietly() to abort/close the writer cleanly, and exit, before we remove the session.
                try {
                    Thread.sleep(1000); // 1 second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                sessions.remove(threadId);
                sendResponse(responseObserver, requestId + " closed session " + threadId);
                LOGGER.debug("{} closed session {}", requestId, threadId);
                return;
            }

            // DROP_TABLE carries "db.table" in destTableName and must NOT create
            // a per-thread session (computeIfAbsent would load/create the very
            // table we're about to drop). Handle it before session setup.
            if (request.getType() == IcebergPayload.PayloadType.DROP_TABLE) {
                if (destTableName == null || destTableName.isEmpty()) {
                    throw new Exception("Destination table name not present in metadata");
                }
                String[] parts = destTableName.split("\\.", 2);
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid destination table name: " + destTableName);
                }
                String dropNamespace = parts[0];
                String dropTableName = parts[1];
                LOGGER.warn("{} Dropping table {}.{}", requestId, dropNamespace, dropTableName);
                boolean dropped = IcebergUtil.dropIcebergTable(dropNamespace, dropTableName, icebergCatalog);
                if (dropped) {
                    sendResponse(responseObserver, "Successfully dropped table " + dropTableName);
                    LOGGER.info("{} Table {} dropped", requestId, dropTableName);
                } else {
                    sendResponse(responseObserver, "Table " + dropTableName + " does not exist");
                    LOGGER.warn("{} Table {} not dropped, table does not exist", requestId, dropTableName);
                }
                LOGGER.info("{} Total time taken: {} ms", requestId, (System.currentTimeMillis() - startTime));
                return;
            }

            // Session-constant context (namespace, upsert, identifier-field,
            // create-identifier-fields, partition spec) rides only on the
            // GET_OR_CREATE_TABLE payload; the session captures it once. Every
            // later request omits those fields and we read them from the session.
            IcebergSession session;
            if (request.getType() == IcebergPayload.PayloadType.GET_OR_CREATE_TABLE) {
                if (destTableName == null || destTableName.isEmpty()) {
                    throw new Exception("Destination table name not present in metadata");
                }
                String namespace = metadata.getNamespace();
                if (namespace == null || namespace.isEmpty()) {
                    throw new Exception("Namespace not present in metadata");
                }
                String identifierField = metadata.getIdentifierField();
                boolean upsert = metadata.getUpsert();
                List<IcebergPayload.SchemaField> schemaMetadata = metadata.getSchemaList();
                List<Map<String, String>> partitionTransforms = toPartitionList(metadata.getPartitionFieldsList());
                TableIdentifier tid = TableIdentifier.of(namespace, destTableName);

                // computeIfAbsent creates a new session if the threadId is not present in the sessions map
                session = sessions.computeIfAbsent(threadId,
                        k -> {
                            Schema schema = new SchemaConvertor(identifierField, schemaMetadata).convertToIcebergSchema();
                            Table icebergTable = loadOrCreateTable(tid, schema, partitionTransforms);
                            return new IcebergSession(icebergTable, upsert, identifierField);
                        });
            } else {
                // RECORDS / COMMIT / EVOLVE_SCHEMA / REFRESH_TABLE_SCHEMA: the
                // session must already exist (GET_OR_CREATE_TABLE runs first in
                // every Go flow). Fail loudly rather than silently mis-configure.
                session = sessions.get(threadId);
                if (session == null) {
                    throw new Exception("No active session for thread " + threadId
                            + "; GET_OR_CREATE_TABLE must be called before " + request.getType());
                }
            }

            switch (request.getType()) {
                case COMMIT:
                    session.op.commitThread(threadId, metadata.getPayload(), session.icebergTable);
                    sendResponse(responseObserver, requestId + " Successfully committed data for thread " + threadId);
                    LOGGER.debug("{} Successfully committed data for thread: {}", requestId, threadId);
                    break;

                case EVOLVE_SCHEMA:
                    SchemaConvertor convertor = new SchemaConvertor(session.identifierField, metadata.getSchemaList());
                    session.op.applyFieldAddition(session.icebergTable, convertor.convertToIcebergSchema(), session.createIdentifierFields());
                    session.icebergTable.refresh();
                    // complete current writer
                    session.op.completeWriter();
                    sendResponse(responseObserver, session.icebergTable.schema().toString());
                    LOGGER.info("{} Successfully applied schema evolution for thread: {}", requestId, threadId);
                    break;

                case REFRESH_TABLE_SCHEMA:
                    session.icebergTable.refresh();
                    // complete current writer
                    session.op.completeWriter();
                    sendResponse(responseObserver, session.icebergTable.schema().toString());
                    break;

                case GET_OR_CREATE_TABLE:
                    session.icebergTable.refresh();
                    String commitState = session.op.getCommitState(session.icebergTable);
                    sendResponse(responseObserver, session.icebergTable.schema().toString(),
                            commitState != null ? commitState : "");
                    break;

                case RECORDS:
                    LOGGER.debug("{} Received {} records for thread {}", requestId, request.getRecordsCount(), threadId);
                    SchemaConvertor recordsConvertor = new SchemaConvertor(session.identifierField, metadata.getSchemaList());
                    List<RecordWrapper> finalRecords = recordsConvertor.convert(session.upsert, session.icebergTable.schema(), request.getRecordsList());
                    
                    session.op.addToTablePerSchema(threadId, session.icebergTable, finalRecords);
                    
                    sendResponse(responseObserver, "successfully pushed records: " + request.getRecordsCount());
                    LOGGER.debug("{} Successfully wrote {} records for thread {}", requestId, request.getRecordsCount(), threadId);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown payload type: " + request.getType());
            }

            LOGGER.info("{} Total time taken: {} ms", requestId, (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            String errorMessage = String.format("%s Failed to process request: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(errorMessage).asRuntimeException());
        }
    }

    private void sendResponse(StreamObserver<RecordIngest.RecordIngestResponse> responseObserver, String message) {
        sendResponse(responseObserver, message, null);
    }

    private void sendResponse(StreamObserver<RecordIngest.RecordIngestResponse> responseObserver, String message, String olake2pcState) {
        RecordIngest.RecordIngestResponse.Builder builder = RecordIngest.RecordIngestResponse.newBuilder().setResult(message);
        if (olake2pcState != null) {
            builder.setOlake2PcState(olake2pcState);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private Table loadOrCreateTable(TableIdentifier tableId, Schema schema, List<Map<String, String>> partitionTransforms) {
        return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
            try {
                // no need to check if the table already exists, because the table is created by the thread that calls the get_or_create_table method
                return IcebergUtil.createIcebergTable(icebergCatalog, tableId, schema, "parquet", partitionTransforms);
            } catch (Exception e) {
                String errorMessage = String.format("Failed to create table from debezium event schema: %s Error: %s",
                                                    tableId, e.getMessage());
                LOGGER.error(errorMessage, e);
                throw new DebeziumException(errorMessage, e);
            }
        });
    }

    private static List<Map<String, String>> toPartitionList(List<IcebergPayload.PartitionField> protos) {
        if (protos == null || protos.isEmpty()) return new ArrayList<>();
        List<Map<String, String>> out = new ArrayList<>(protos.size());
        for (IcebergPayload.PartitionField p : protos) {
            Map<String, String> m = new HashMap<>(2);
            m.put("field", p.getField());
            m.put("transform", p.getTransform());
            out.add(m);
        }
        return out;
    }
}
