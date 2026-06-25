package io.olake.server.iceberg.writer.arrow;

import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;
import io.olake.server.iceberg.session.WriterSession;
import io.olake.server.iceberg.session.SessionManager;
import io.olake.server.iceberg.rpc.RecordIngest.CommitRequest;
import io.olake.server.iceberg.rpc.RecordIngest.CommitResponse;
import io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest;
import io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse;
import io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest;
import io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse;
import io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest;
import io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse;
import io.olake.server.iceberg.rpc.RecordIngest.WriterMode;

public class ArrowWriterHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriterHandler.class);

    private static final String FILE_TYPE_DATA = "data";
    private static final String FILE_TYPE_EQUALITY_DELETE = "equalityDelete";
    private static final String FILE_TYPE_POSITIONAL_DELETE = "positionalDelete";

    public static void initSession(String threadId, InitSessionRequest request, InitSessionResponse.Builder responseBuilder, Catalog icebergCatalog, SessionManager sessionManager, TableIdentifier tid) throws Exception {
        sessionManager.getOrCreateSession(threadId, () -> {
            if (!icebergCatalog.tableExists(tid)) {
                throw new RuntimeException("Table does not exist: " + tid);
            }
            return new WriterSession(icebergCatalog.loadTable(tid), request.getTableConfig().getUpsert(), WriterMode.ARROW);
        });

        WriterSession session = sessionManager.getSession(threadId);
        session.getIcebergTable().refresh();

        String dataSchemaJson = SchemaParser.toJson(session.getIcebergTable().schema());
        NestedField olakeIdField = session.getIcebergTable().schema().findField(WriterSession.OLAKE_ID_FIELD);
        Schema deleteSchema = new Schema(
                session.getIcebergTable().schema().schemaId(),
                Collections.singletonList(olakeIdField),
                session.getIcebergTable().schema().identifierFieldIds());
        String deleteSchemaJson = SchemaParser.toJson(deleteSchema);
        String posDeleteSchemaJson = SchemaParser.toJson(DeleteSchemaUtil.pathPosSchema());

        responseBuilder.setResult("Arrow session initialized")
                       .putAllIcebergSchemas(Map.of(
                               FILE_TYPE_DATA, dataSchemaJson,
                               FILE_TYPE_EQUALITY_DELETE, deleteSchemaJson,
                               FILE_TYPE_POSITIONAL_DELETE, posDeleteSchemaJson));
    }

    public static void getFilePath(WriterSession session, GetFilePathRequest request, StreamObserver<GetFilePathResponse> responseObserver, String requestId) throws Exception {
        EncryptedOutputFile encryptedFile = session.getFileFactory().newOutputFile();
        String basePath = encryptedFile.encryptingOutputFile().location();
        LOGGER.debug("{} Allocated base file path: {}", requestId, basePath);
        
        responseObserver.onNext(GetFilePathResponse.newBuilder()
                .setResult(basePath)
                .build());
    }

    public static void uploadFile(WriterSession session, UploadFileRequest request, StreamObserver<UploadFileResponse> responseObserver, String requestId) throws Exception {
        FileIO fileIO = session.getIcebergTable().io();
        OutputFile outputFile = fileIO.newOutputFile(request.getFilePath());
        try (OutputStream out = outputFile.create()) {
            out.write(request.getFileData().toByteArray());
            out.flush();
        }
        LOGGER.info("{} Successfully uploaded file to: {}", requestId, request.getFilePath());
        
        responseObserver.onNext(UploadFileResponse.newBuilder()
                .setResult(request.getFilePath())
                .build());
    }

    public static void commit(WriterSession session, CommitRequest request, StreamObserver<CommitResponse> responseObserver, String requestId) throws Exception {
        int dataFileCount = 0;
        int eqDeleteFileCount = 0;
        int posDeleteFileCount = 0;

        io.grpc.Context grpcContext = io.grpc.Context.current();
        for (CommitRequest.FileMetadata fileMeta : request.getFileMetadataList()) {
            if (grpcContext.isCancelled()) {
                throw new Exception("gRPC request context is cancelled by client mid-commit");
            }
            String fileType = fileMeta.getFileType();
            String filePath = fileMeta.getFilePath();
            long recordCount = fileMeta.getRecordCount();

            switch (fileType) {
                case FILE_TYPE_EQUALITY_DELETE -> {
                    NestedField olakeIdField = session.getIcebergTable().schema().findField(WriterSession.OLAKE_ID_FIELD);
                    session.getOp().registerEqDeleteFiles(request.getThreadId(), session.getIcebergTable(), filePath,
                            olakeIdField.fieldId(), recordCount, fileMeta.getPartitionValuesList());
                    eqDeleteFileCount++;
                }
                case FILE_TYPE_POSITIONAL_DELETE -> {
                    session.getOp().registerPosDeleteFiles(request.getThreadId(), session.getIcebergTable(), filePath,
                            recordCount, fileMeta.getPartitionValuesList());
                    posDeleteFileCount++;
                }
                case FILE_TYPE_DATA -> {
                    session.getOp().registerDataFiles(request.getThreadId(), session.getIcebergTable(), filePath,
                            fileMeta.getPartitionValuesList());
                    dataFileCount++;
                }
                default -> LOGGER.warn("{} Unknown file type '{}' for path: {}", requestId, fileType, filePath);
            }
        }

        session.getOp().commitThread(request.getThreadId(), request.getPayload(), session.getIcebergTable());
        
        String msg = String.format("Successfully committed %d data files, %d equality delete files, and %d positional delete files for thread %s",
                dataFileCount, eqDeleteFileCount, posDeleteFileCount, request.getThreadId());
        
        responseObserver.onNext(CommitResponse.newBuilder()
                .setResult(msg)
                .build());
    }
}
