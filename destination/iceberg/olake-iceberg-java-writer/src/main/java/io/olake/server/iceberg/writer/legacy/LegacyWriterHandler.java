package io.olake.server.iceberg.writer.legacy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import io.grpc.stub.StreamObserver;
import io.olake.server.iceberg.util.IcebergUtil;
import io.olake.server.iceberg.writer.legacy.schema.SchemaConvertor;
import io.olake.server.iceberg.session.WriterSession;
import io.olake.server.iceberg.session.SessionManager;
import io.olake.server.iceberg.rpc.RecordIngest.CommitRequest;
import io.olake.server.iceberg.rpc.RecordIngest.CommitResponse;
import io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest;
import io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse;
import io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest;
import io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse;
import io.olake.server.iceberg.rpc.RecordIngest.PartitionField;
import io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest;
import io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse;
import io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest;
import io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse;
import io.olake.server.iceberg.rpc.RecordIngest.WriterMode;
import io.olake.server.iceberg.writer.legacy.operator.RecordWrapper;

public class LegacyWriterHandler {

    public static void initSession(String threadId, InitSessionRequest request, InitSessionResponse.Builder responseBuilder, Catalog icebergCatalog, SessionManager sessionManager, TableIdentifier tid) throws Exception {
        List<Map<String, String>> partitionTransforms = toPartitionList(request.getPartitionFieldsList());
        boolean upsert = request.getTableConfig().getUpsert();
        sessionManager.getOrCreateSession(threadId, () -> {
            String identifierField = upsert ? WriterSession.OLAKE_ID_FIELD : "";
            Schema schema = new SchemaConvertor(identifierField, request.getSchemaList()).convertToIcebergSchema();
            Table icebergTable = IcebergUtil.loadOrCreateIcebergTable(icebergCatalog, tid, schema, partitionTransforms);
            return new WriterSession(icebergTable, upsert, WriterMode.LEGACY);
        });

        WriterSession session = sessionManager.getSession(threadId);
        session.getIcebergTable().refresh();
        String commitState = session.getOp().getCommitState(session.getIcebergTable());
        
        responseBuilder.setResult("Legacy session initialized")
                       .setSchemaJson(session.getIcebergTable().schema().toString())
                       .setOlake2PcState(commitState != null ? commitState : "");
    }

    public static void sendRecords(WriterSession session, SendRecordsRequest request, StreamObserver<SendRecordsResponse> responseObserver, String requestId) throws Exception {
        SchemaConvertor recordsConvertor = new SchemaConvertor(session.getIdentifierField(), request.getSchemaList());
        List<RecordWrapper> finalRecords = recordsConvertor.convert(session.isUpsert(), session.getIcebergTable().schema(), request.getRecordsList());

        session.getOp().addToTablePerSchema(request.getThreadId(), session.getIcebergTable(), finalRecords);

        responseObserver.onNext(SendRecordsResponse.newBuilder()
                .setResult("successfully pushed records: " + request.getRecordsCount())
                .build());
    }

    public static void evolveSchema(WriterSession session, EvolveSchemaRequest request, StreamObserver<EvolveSchemaResponse> responseObserver, String requestId) throws Exception {
        SchemaConvertor convertor = new SchemaConvertor(session.getIdentifierField(), request.getSchemaList());
        session.getOp().applyFieldAddition(session.getIcebergTable(), convertor.convertToIcebergSchema(), session.isCreateIdentifierFields());
        session.getIcebergTable().refresh();
        session.getOp().completeWriter();
        
        responseObserver.onNext(EvolveSchemaResponse.newBuilder()
                .setResult("Successfully applied schema evolution")
                .setSchemaJson(session.getIcebergTable().schema().toString())
                .build());
    }

    public static void refreshTableSchema(WriterSession session, RefreshTableSchemaRequest request, StreamObserver<RefreshTableSchemaResponse> responseObserver, String requestId) throws Exception {
        session.getIcebergTable().refresh();
        session.getOp().completeWriter();
        
        responseObserver.onNext(RefreshTableSchemaResponse.newBuilder()
                .setResult("Successfully refreshed table schema")
                .setSchemaJson(session.getIcebergTable().schema().toString())
                .build());
    }

    public static void commit(WriterSession session, CommitRequest request, StreamObserver<CommitResponse> responseObserver, String requestId) throws Exception {
        session.getOp().commitThread(request.getThreadId(), request.getPayload(), session.getIcebergTable());
        
        responseObserver.onNext(CommitResponse.newBuilder()
                .setResult(requestId + " Successfully committed data for thread " + request.getThreadId())
                .build());
    }

    private static List<Map<String, String>> toPartitionList(List<PartitionField> protos) {
        if (protos == null || protos.isEmpty()) return new ArrayList<>();
        List<Map<String, String>> out = new ArrayList<>(protos.size());
        for (PartitionField p : protos) {
            Map<String, String> m = new HashMap<>(2);
            m.put("field", p.getField());
            m.put("transform", p.getTransform());
            out.add(m);
        }
        return out;
    }
}
