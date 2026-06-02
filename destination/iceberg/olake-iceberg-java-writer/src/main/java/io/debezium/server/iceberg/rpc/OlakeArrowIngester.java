package io.debezium.server.iceberg.rpc;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

/**
 * Multi-tenant gRPC service for the Arrow Iceberg write path.
 *
 * Same isolation model as {@link OlakeRowsIngester}: one session per Go thread,
 * each owning its own Table handle + OutputFileFactory + IcebergTableOperator.
 * No cross-session caches; this exactly mirrors the old per-JVM isolation.
 */
@Dependent
public class OlakeArrowIngester extends ArrowIngestServiceGrpc.ArrowIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeArrowIngester.class);
    private static final String FILE_TYPE_DATA = "data";
    private static final String FILE_TYPE_EQUALITY_DELETE = "equalityDelete";
    private static final String FILE_TYPE_POSITIONAL_DELETE = "positionalDelete";

    private final Catalog icebergCatalog;

    // Single map: one entry per active Go writer thread.
    // Each session is fully self-contained — exact replica of what one JVM owned.
    private final ConcurrentMap<String, ArrowSession> sessions = new ConcurrentHashMap<>();

    public OlakeArrowIngester(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    private static final class ArrowSession {
        final Table icebergTable;
        final OutputFileFactory fileFactory;
        final IcebergTableOperator op;

        ArrowSession(Table icebergTable, boolean upsert) {
            this.icebergTable = icebergTable;
            FileFormat fileFormat = IcebergUtil.getTableFileFormat(icebergTable);
            this.fileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, fileFormat);
            this.op = new IcebergTableOperator(upsert);
        }
    }

    @Override
    public void icebergAPI(ArrowPayload request, StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver) {
        String requestId = String.format("[Arrow-%d-%d]", Thread.currentThread().getId(), System.nanoTime());

        try {
            ArrowPayload.Metadata metadata = request.getMetadata();
            String threadId = metadata.getThreadId();
            String destTableName = metadata.getDestTableName();
            String namespace = metadata.getNamespace();
            boolean upsert = metadata.getUpsert();

            if (threadId == null || threadId.isEmpty()) {
                throw new Exception("Thread id not present in metadata");
            }

            // CLOSE_SESSION: just drop the session. No closeQuietly() — it would
            // clear op.filesToCommit while an in-flight REGISTER_AND_COMMIT may be
            // using that list. Nothing to close here; the session is GC'd.
            if (request.getType() == ArrowPayload.PayloadType.CLOSE_SESSION) {
                sessions.remove(threadId);
                sendResponse(responseObserver, requestId + " closed arrow session " + threadId);
                return;
            }

            if (destTableName == null || destTableName.isEmpty()) {
                throw new Exception("Destination table name not present in metadata");
            }

            if (namespace == null || namespace.isEmpty()) {
                throw new Exception("Namespace not present in metadata");
            }

            TableIdentifier tid = TableIdentifier.of(namespace, destTableName);

            // Lazily create the session on first request for this threadId.
            // Table must already exist (created by the legacy ingester during Setup).
            ArrowSession session = sessions.computeIfAbsent(threadId, k -> {
                if (!icebergCatalog.tableExists(tid)) {
                    throw new RuntimeException("Table does not exist: " + tid);
                }
                Table t = icebergCatalog.loadTable(tid);
                return new ArrowSession(t, upsert);
            });

            switch (request.getType()) {
                case JSONSCHEMA -> {
                    session.icebergTable.refresh();

                    String dataSchemaJson = SchemaParser.toJson(session.icebergTable.schema());

                    NestedField olakeIdField = session.icebergTable.schema().findField("_olake_id");
                    Schema deleteSchema = new Schema(
                            session.icebergTable.schema().schemaId(),
                            Collections.singletonList(olakeIdField),
                            session.icebergTable.schema().identifierFieldIds());
                    String deleteSchemaJson = SchemaParser.toJson(deleteSchema);

                    String posDeleteSchemaJson = SchemaParser.toJson(DeleteSchemaUtil.pathPosSchema());

                    sendSchemaResponse(responseObserver, "Schema JSON retrieved successfully",
                            Map.of(FILE_TYPE_DATA, dataSchemaJson,
                                   FILE_TYPE_EQUALITY_DELETE, deleteSchemaJson,
                                   FILE_TYPE_POSITIONAL_DELETE, posDeleteSchemaJson));
                }

                case REGISTER_AND_COMMIT -> {
                    List<ArrowPayload.FileMetadata> fileMetadataList = metadata.getFileMetadataList();
                    int dataFileCount = 0;
                    int eqDeleteFileCount = 0;
                    int posDeleteFileCount = 0;

                    for (ArrowPayload.FileMetadata fileMeta : fileMetadataList) {
                        String fileType = fileMeta.getFileType();
                        String filePath = fileMeta.getFilePath();
                        long recordCount = fileMeta.getRecordCount();

                        switch (fileType) {
                            case FILE_TYPE_EQUALITY_DELETE -> {
                                NestedField olakeIdField = session.icebergTable.schema().findField("_olake_id");
                                session.op.registerEqDeleteFiles(threadId, session.icebergTable, filePath,
                                        olakeIdField.fieldId(), recordCount, fileMeta.getPartitionValuesList());
                                eqDeleteFileCount++;
                            }
                            case FILE_TYPE_POSITIONAL_DELETE -> {
                                session.op.registerPosDeleteFiles(threadId, session.icebergTable, filePath,
                                        recordCount, fileMeta.getPartitionValuesList());
                                posDeleteFileCount++;
                            }
                            case FILE_TYPE_DATA -> {
                                session.op.registerDataFiles(threadId, session.icebergTable, filePath,
                                        fileMeta.getPartitionValuesList());
                                dataFileCount++;
                            }
                            default -> LOGGER.warn("{} Unknown file type '{}' for path: {}", requestId, fileType, filePath);
                        }
                    }

                    session.op.commitThread(threadId, metadata.getPayload(), session.icebergTable);
                    sendResponse(responseObserver, String.format(
                            "Successfully committed %d data files, %d equality delete files, and %d positional delete files for thread %s",
                            dataFileCount, eqDeleteFileCount, posDeleteFileCount, threadId));
                }

                case UPLOAD_FILE -> {
                    ArrowPayload.FileUploadRequest uploadReq = metadata.getFileUpload();
                    FileIO fileIO = session.icebergTable.io();
                    OutputFile outputFile = fileIO.newOutputFile(uploadReq.getFilePath());
                    try (OutputStream out = outputFile.create()) {
                        out.write(uploadReq.getFileData().toByteArray());
                        out.flush();
                    }
                    LOGGER.info("{} Successfully uploaded file to: {}", requestId, uploadReq.getFilePath());
                    sendResponse(responseObserver, uploadReq.getFilePath());
                }

                case FILEPATH -> {
                    EncryptedOutputFile encryptedFile = session.fileFactory.newOutputFile();
                    String basePath = encryptedFile.encryptingOutputFile().location();
                    LOGGER.debug("{} Allocated base file path: {}", requestId, basePath);
                    sendResponse(responseObserver, basePath);
                }

                default -> throw new IllegalArgumentException("Unknown payload type: " + request.getType());
            }
        } catch (Exception e) {
            String errorMessage = String.format("%s Failed to process request: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(errorMessage).asRuntimeException());
        }
    }

    private void sendResponse(StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver, String message) {
        responseObserver.onNext(RecordIngest.ArrowIngestResponse.newBuilder().setResult(message).build());
        responseObserver.onCompleted();
    }

    private void sendSchemaResponse(StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver,
                                    String message, Map<String, String> schemaMap) {
        responseObserver.onNext(RecordIngest.ArrowIngestResponse.newBuilder()
                .setResult(message).putAllIcebergSchemas(schemaMap).build());
        responseObserver.onCompleted();
    }
}
