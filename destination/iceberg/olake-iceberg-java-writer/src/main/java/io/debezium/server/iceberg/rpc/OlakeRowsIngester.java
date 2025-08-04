package io.debezium.server.iceberg.rpc;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload;
import io.debezium.server.iceberg.SchemaConvertor;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.server.iceberg.tableoperator.RecordWrapper;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

import org.apache.iceberg.Table;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private final String icebergNamespace;
    private final Catalog icebergCatalog;
    private final boolean upsertRecords;
    private final boolean createIdentifierFields;
    private final IcebergTableOperator icebergTableOperator;
    private final List<Map<String, String>> partitionTransforms;
    private final Object tableCreationLock = new Object();

    public OlakeRowsIngester(boolean upsertRecords, String icebergNamespace, Catalog icebergCatalog, 
                           List<Map<String, String>> partitionTransforms, boolean createIdentifierFields) {
        this.upsertRecords = upsertRecords;
        this.icebergNamespace = icebergNamespace;
        this.icebergCatalog = icebergCatalog;
        this.partitionTransforms = partitionTransforms;
        this.createIdentifierFields = createIdentifierFields;
        this.icebergTableOperator = new IcebergTableOperator(upsertRecords, createIdentifierFields);
    }

    @Override
    public void sendRecords(IcebergPayload request, StreamObserver<RecordIngest.RecordIngestResponse> responseObserver) {
        String requestId = String.format("[Thread-%d-%d]", Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();
        
        try {
            IcebergPayload.Metadata metadata = request.getMetadata();
            String threadId = metadata.getThreadId();
            String destTableName = metadata.getDestTableName();
            String primaryKey = metadata.getPrimaryKey();
            List<IcebergPayload.SchemaField> schemaMetadata = metadata.getSchemaList();
            if (threadId == null || threadId.isEmpty()) {
                throw new Exception("Thread id not present in metadata");
            }
            if (destTableName == null || destTableName.isEmpty()) {
                throw new Exception("Destination table name not present in metadata");
            }
            // TODO: load iceberg table on server and cache
            switch (request.getType()) {
                case COMMIT:
                    LOGGER.info("{} Received commit request for thread: {}", requestId, threadId);
                    icebergTableOperator.commitThread(threadId);
                    sendResponse(responseObserver, requestId + " Successfully committed data for thread " + threadId);
                    LOGGER.info("{} Successfully committed data for thread: {}", requestId, threadId);
                    break;
                    
                case EVOLVE_SCHEMA:
                    SchemaConvertor convertor = new SchemaConvertor(primaryKey, schemaMetadata);
                    Table icebergTable = loadIcebergTable(TableIdentifier.of(icebergNamespace, destTableName), 
                                               convertor.convertToIcebergSchema());
                    icebergTableOperator.applyFieldAddition(icebergTable, convertor.convertToIcebergSchema());
                    sendResponse(responseObserver, "schema evolution applied");
                    LOGGER.info("{} Successfully applied schema evolution for table: {}", requestId, destTableName);
                    break;
                    
                case GET_OR_CREATE_TABLE:
                    SchemaConvertor schemaConvertor = new SchemaConvertor(primaryKey, schemaMetadata);
                    Table table = loadIcebergTable(TableIdentifier.of(icebergNamespace, destTableName), 
                                                schemaConvertor.convertToIcebergSchema());
                    sendResponse(responseObserver, table.toString());
                    LOGGER.info("{} Successfully returned iceberg table {}", requestId, destTableName);
                    break;
                    
                case RECORDS:
                    SchemaConvertor recordsConvertor = new SchemaConvertor(primaryKey, schemaMetadata);
                    Table recordsTable = loadIcebergTable(TableIdentifier.of(icebergNamespace, destTableName), 
                                                         recordsConvertor.convertToIcebergSchema());
                    icebergTableOperator.applyFieldAddition(recordsTable, recordsConvertor.convertToIcebergSchema());
                    List<RecordWrapper> finalRecords = recordsConvertor.convert(upsertRecords,recordsTable.schema(), request.getRecordsList());
                    icebergTableOperator.addToTablePerSchema(threadId, recordsTable, finalRecords);
                    sendResponse(responseObserver, "successfully pushed records: " + request.getRecordsCount());
                    LOGGER.info("{} Successfully wrote {} records to table {}", requestId, request.getRecordsCount(), destTableName);
                    break;
                    
                case DROP_TABLE:
                    LOGGER.warn("{} Table {} not dropped, drop table not implemented", requestId, destTableName);
                    sendResponse(responseObserver, "Drop table not implemented");
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
        RecordIngest.RecordIngestResponse response = RecordIngest.RecordIngestResponse.newBuilder()
            .setResult(message)
            .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public Table loadIcebergTable(TableIdentifier tableId, Schema schema) {
        synchronized (tableCreationLock) {
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
    }
}