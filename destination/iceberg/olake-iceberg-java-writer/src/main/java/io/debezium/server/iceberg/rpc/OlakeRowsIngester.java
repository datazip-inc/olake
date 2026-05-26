package io.debezium.server.iceberg.rpc;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload;
import io.debezium.server.iceberg.SchemaConvertor;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Multi-tenant gRPC service for the legacy (rows-based) Iceberg write path.
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
 * arrives on every gRPC payload so the JVM needs no global config.
 */
@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private final Catalog icebergCatalog;

    // One entry per active Go writer thread. Each session is self-contained:
    // it owns its own Table handle (loaded from the catalog on first use) and
    // its own IcebergTableOperator (with an independent BaseTaskWriter).
    // This is the exact isolation the old per-thread JVM gave for free.
    private final ConcurrentMap<String, ThreadSession> sessions = new ConcurrentHashMap<>();

    public OlakeRowsIngester(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    private final class ThreadSession {
        final Table icebergTable;
        final IcebergTableOperator op;

        ThreadSession(TableIdentifier tid,
                      String identifierField,
                      List<IcebergPayload.SchemaField> schemaMetadata,
                      List<Map<String, String>> partitionTransforms,
                      boolean upsert) {
            Schema schema = new SchemaConvertor(identifierField, schemaMetadata).convertToIcebergSchema();
            this.icebergTable = loadOrCreateTable(tid, schema, partitionTransforms);
            this.op = new IcebergTableOperator(upsert);
        }
    }

    @Override
    public void sendRecords(IcebergPayload request, StreamObserver<RecordIngest.RecordIngestResponse> responseObserver) {
        String requestId = String.format("[Thread-%d-%d]", Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();

        try {
            IcebergPayload.Metadata metadata = request.getMetadata();
            String threadId = metadata.getThreadId();
            String destTableName = metadata.getDestTableName();
            String identifierField = metadata.getIdentifierField();
            List<IcebergPayload.SchemaField> schemaMetadata = metadata.getSchemaList();
            String namespace = metadata.getNamespace();
            boolean upsert = metadata.getUpsert();
            boolean createIdentifierFields = metadata.getCreateIdentifierFields();
            List<Map<String, String>> partitionTransforms = toPartitionList(metadata.getPartitionFieldsList());

            if (threadId == null || threadId.isEmpty()) {
                throw new Exception("Thread id not present in metadata");
            }

            // CLOSE_SESSION: release the session's Table handle and operator.
            // Mirrors what process exit did for free in the old per-JVM model.
            if (request.getType() == IcebergPayload.PayloadType.CLOSE_SESSION) {
                ThreadSession closed = sessions.remove(threadId);
                if (closed != null) {
                    closed.op.closeQuietly();
                }
                sendResponse(responseObserver, requestId + " closed session " + threadId);
                LOGGER.debug("{} closed session {}", requestId, threadId);
                return;
            }

            if (destTableName == null || destTableName.isEmpty()) {
                throw new Exception("Destination table name not present in metadata");
            }

            if (namespace == null || namespace.isEmpty()) {
                throw new Exception("Namespace not present in metadata");
            }

            TableIdentifier tid = TableIdentifier.of(namespace, destTableName);

            // Lazily create the session on first request. computeIfAbsent is
            // atomic so even if two gRPC server threads race on the same threadId
            // (which Go's CxGroup limit-1 prevents, but defensive here), only one
            // session is created.
            ThreadSession session = sessions.computeIfAbsent(threadId,
                    k -> new ThreadSession(tid, identifierField, schemaMetadata, partitionTransforms, upsert));

            switch (request.getType()) {
                case DROP_TABLE:
                    handleDropTable(requestId, destTableName, responseObserver);
                    break;

                case COMMIT:
                    session.op.commitThread(threadId, metadata.getPayload(), session.icebergTable);
                    sendResponse(responseObserver, requestId + " Successfully committed data for thread " + threadId);
                    LOGGER.debug("{} Successfully committed data for thread: {}", requestId, threadId);
                    break;

                case EVOLVE_SCHEMA: {
                    SchemaConvertor convertor = new SchemaConvertor(identifierField, schemaMetadata);
                    session.op.applyFieldAddition(session.icebergTable, convertor.convertToIcebergSchema(), createIdentifierFields);
                    session.icebergTable.refresh();
                    session.op.completeWriter();
                    sendResponse(responseObserver, session.icebergTable.schema().toString());
                    LOGGER.info("{} Successfully applied schema evolution for table: {}", requestId, destTableName);
                    break;
                }

                case REFRESH_TABLE_SCHEMA:
                    session.icebergTable.refresh();
                    session.op.completeWriter();
                    sendResponse(responseObserver, session.icebergTable.schema().toString());
                    break;

                case GET_OR_CREATE_TABLE: {
                    session.icebergTable.refresh();
                    String commitState = session.op.getCommitState(session.icebergTable);
                    sendResponse(responseObserver, session.icebergTable.schema().toString(),
                            commitState != null ? commitState : "");
                    break;
                }

                case RECORDS: {
                    LOGGER.debug("{} Received {} records for table {}", requestId, request.getRecordsCount(), destTableName);
                    SchemaConvertor recordsConvertor = new SchemaConvertor(identifierField, schemaMetadata);
                    List<RecordWrapper> finalRecords = recordsConvertor.convert(upsert, session.icebergTable.schema(), request.getRecordsList());
                    session.op.addToTablePerSchema(threadId, session.icebergTable, finalRecords);
                    sendResponse(responseObserver, "successfully pushed records: " + request.getRecordsCount());
                    LOGGER.debug("{} Successfully wrote {} records to table {}", requestId, request.getRecordsCount(), destTableName);
                    break;
                }

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

    private Table loadOrCreateTable(TableIdentifier tableId, Schema schema, List<Map<String, String>> partitionTransforms) {
        return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
            try {
                return IcebergUtil.createIcebergTable(icebergCatalog, tableId, schema, "parquet", partitionTransforms);
            } catch (Exception e) {
                String errorMessage = String.format("Failed to create table from debezium event schema: %s Error: %s",
                                                    tableId, e.getMessage());
                LOGGER.error(errorMessage, e);
                throw new DebeziumException(errorMessage, e);
            }
        });
    }

    private void handleDropTable(String requestId, String dropTable, StreamObserver<RecordIngest.RecordIngestResponse> responseObserver) {
        String[] parts = dropTable.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid destination table name: " + dropTable);
        }
        String namespace = parts[0];
        String tableName = parts[1];
        LOGGER.warn("{} Dropping table {}.{}", requestId, namespace, tableName);
        boolean dropped = IcebergUtil.dropIcebergTable(namespace, tableName, icebergCatalog);
        if (dropped) {
            sendResponse(responseObserver, "Successfully dropped table " + tableName);
            LOGGER.info("{} Table {} dropped", requestId, tableName);
        } else {
            sendResponse(responseObserver, "Table " + tableName + " does not exist");
            LOGGER.warn("{} Table {} not dropped, table does not exist", requestId, tableName);
        }
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
}
