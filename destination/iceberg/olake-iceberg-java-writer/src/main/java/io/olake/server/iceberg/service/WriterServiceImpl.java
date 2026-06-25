package io.olake.server.iceberg.service;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.olake.server.iceberg.middleware.GrpcMiddleware;
import io.olake.server.iceberg.session.SessionManager;
import io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest;
import io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse;
import io.olake.server.iceberg.rpc.RecordIngest.CommitRequest;
import io.olake.server.iceberg.rpc.RecordIngest.CommitResponse;
import io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest;
import io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse;
import io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest;
import io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse;
import io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest;
import io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse;
import io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest;
import io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse;
import io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest;
import io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse;
import io.olake.server.iceberg.rpc.RecordIngest.TableConfig;
import io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest;
import io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse;
import io.olake.server.iceberg.rpc.RecordIngest.WriterMode;
import io.olake.server.iceberg.rpc.WriterServiceGrpc;
import io.olake.server.iceberg.writer.arrow.ArrowWriterHandler;
import io.olake.server.iceberg.writer.legacy.LegacyWriterHandler;

import io.grpc.stub.StreamObserver;

public class WriterServiceImpl extends WriterServiceGrpc.WriterServiceImplBase {

    private final Catalog icebergCatalog;
    private final SessionManager sessionManager = new SessionManager();

    public WriterServiceImpl(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public void initSession(InitSessionRequest request, StreamObserver<InitSessionResponse> responseObserver) {
        GrpcMiddleware.processStateless("InitSession", responseObserver, requestId -> {
            String threadId = request.getThreadId();
            TableConfig config = request.getTableConfig();

            if (config.getDestTableName() == null || config.getDestTableName().isEmpty()) {
                throw new Exception("Destination table name not present in config");
            }
            if (config.getNamespace() == null || config.getNamespace().isEmpty()) {
                throw new Exception("Namespace not present in config");
            }

            TableIdentifier tid = TableIdentifier.of(config.getNamespace(), config.getDestTableName());
            InitSessionResponse.Builder responseBuilder = InitSessionResponse.newBuilder();

            if (request.getMode() == WriterMode.LEGACY) {
                LegacyWriterHandler.initSession(threadId, request, responseBuilder, icebergCatalog, sessionManager, tid);
            } else {
                ArrowWriterHandler.initSession(threadId, request, responseBuilder, icebergCatalog, sessionManager, tid);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void sendRecords(SendRecordsRequest request, StreamObserver<SendRecordsResponse> responseObserver) {
        GrpcMiddleware.processSession("SendRecords", request.getThreadId(), responseObserver, sessionManager, (requestId, session) -> {
            LegacyWriterHandler.sendRecords(session, request, responseObserver, requestId);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void evolveSchema(EvolveSchemaRequest request, StreamObserver<EvolveSchemaResponse> responseObserver) {
        GrpcMiddleware.processSession("EvolveSchema", request.getThreadId(), responseObserver, sessionManager, (requestId, session) -> {
            LegacyWriterHandler.evolveSchema(session, request, responseObserver, requestId);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void refreshTableSchema(RefreshTableSchemaRequest request, StreamObserver<RefreshTableSchemaResponse> responseObserver) {
        GrpcMiddleware.processSession("RefreshSchema", request.getThreadId(), responseObserver, sessionManager, (requestId, session) -> {
            LegacyWriterHandler.refreshTableSchema(session, request, responseObserver, requestId);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getFilePath(GetFilePathRequest request, StreamObserver<GetFilePathResponse> responseObserver) {
        GrpcMiddleware.processSession("GetFilePath", request.getThreadId(), responseObserver, sessionManager, (requestId, session) -> {
            ArrowWriterHandler.getFilePath(session, request, responseObserver, requestId);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void uploadFile(UploadFileRequest request, StreamObserver<UploadFileResponse> responseObserver) {
        GrpcMiddleware.processSession("UploadFile", request.getThreadId(), responseObserver, sessionManager, (requestId, session) -> {
            ArrowWriterHandler.uploadFile(session, request, responseObserver, requestId);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitResponse> responseObserver) {
        GrpcMiddleware.processSession("Commit", request.getThreadId(), responseObserver, sessionManager, (requestId, session) -> {
            if (session.getMode() == WriterMode.ARROW) {
                ArrowWriterHandler.commit(session, request, responseObserver, requestId);
            } else {
                LegacyWriterHandler.commit(session, request, responseObserver, requestId);
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void closeSession(CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
        GrpcMiddleware.processStateless("CloseSession", responseObserver, requestId -> {
            sessionManager.removeSession(request.getThreadId());
            responseObserver.onNext(CloseSessionResponse.newBuilder()
                    .setResult(requestId + " closed session " + request.getThreadId())
                    .build());
            responseObserver.onCompleted();
        });
    }
}
