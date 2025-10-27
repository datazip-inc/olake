package io.debezium.server.iceberg.rpc;

import java.util.List;
import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
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

@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private final String icebergNamespace;
    private final Catalog icebergCatalog;
    private final boolean upsertRecords;
    private final IcebergTableOperator icebergTableOperator;
    private final List<Map<String, String>> partitionTransforms;
    private Table icebergTable;

    public OlakeRowsIngester(boolean upsertRecords, String icebergNamespace, Catalog icebergCatalog, 
                           List<Map<String, String>> partitionTransforms) {
        this.upsertRecords = upsertRecords;
        this.icebergNamespace = icebergNamespace;
        this.icebergCatalog = icebergCatalog;
        this.partitionTransforms = partitionTransforms;
        this.icebergTable = null;
        this.icebergTableOperator = new IcebergTableOperator(upsertRecords);
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
            
            if (threadId == null || threadId.isEmpty()) {
                // file references are being stored through thread id
                throw new Exception("Thread id not present in metadata");
            }

            if (destTableName == null || destTableName.isEmpty()) {
                throw new Exception("Destination table name not present in metadata");
            }

            if (this.icebergTable == null) {
                SchemaConvertor schemaConvertor = new SchemaConvertor(identifierField, schemaMetadata);
                this.icebergTable = loadIcebergTable(TableIdentifier.of(icebergNamespace, destTableName), 
                                        schemaConvertor.convertToIcebergSchema());
            }
            
            // NOTE: on EVOLVE_SCHEMA and REFRESH_TABLE_SCHEMA we need to complete writer as schema is updated in iceberg table instance
            // but the writer instance still using schema when it got created

            switch (request.getType()) {
                case COMMIT:
                    LOGGER.info("{} Received commit request for thread: {}", requestId, threadId);
                    icebergTableOperator.commitThread(threadId, this.icebergTable);
                    sendResponse(responseObserver, requestId + " Successfully committed data for thread " + threadId);
                    LOGGER.debug("{} Successfully committed data for thread: {}", requestId, threadId);
                    break;
                    
                case EVOLVE_SCHEMA:
                    SchemaConvertor convertor = new SchemaConvertor(identifierField, schemaMetadata);
                    icebergTableOperator.applyFieldAddition(this.icebergTable, convertor.convertToIcebergSchema());
                    this.icebergTable.refresh();
                    // complete current writer 
                    icebergTableOperator.completeWriter();
                    sendResponse(responseObserver, this.icebergTable.schema().toString());
                    LOGGER.info("{} Successfully applied schema evolution for table: {}", requestId, destTableName);
                    break;
                
                case REFRESH_TABLE_SCHEMA:
                    this.icebergTable.refresh();
                    // complete current writer 
                    icebergTableOperator.completeWriter();
                    sendResponse(responseObserver, this.icebergTable.schema().toString());
                    break;

                case GET_OR_CREATE_TABLE:
                    sendResponse(responseObserver, this.icebergTable.schema().toString());
                    LOGGER.info("{} Successfully returned iceberg table {}", requestId, destTableName);
                    break;

                case RECORDS:
                    LOGGER.debug("{} Received records request for  {} records to table {}", requestId, request.getRecordsCount(), destTableName);
                    SchemaConvertor recordsConvertor = new SchemaConvertor(identifierField, schemaMetadata);
                    List<RecordWrapper> finalRecords = recordsConvertor.convert(upsertRecords, this.icebergTable.schema(), request.getRecordsList());
                    icebergTableOperator.addToTablePerSchema(threadId, this.icebergTable, finalRecords);
                    sendResponse(responseObserver, "successfully pushed records: " + request.getRecordsCount());
                    LOGGER.debug("{} Successfully wrote {} records to table {}", requestId, request.getRecordsCount(), destTableName);
                    break;
                    
                case DROP_TABLE:
                    LOGGER.warn("{} Table {} not dropped, drop table not implemented", requestId, destTableName);
                    sendResponse(responseObserver, "Drop table not implemented");
                    break;

                case REGISTER:
                    LOGGER.info("{} Received REGISTER request for thread: {}", requestId, threadId);
                    
                    List<IcebergPayload.FileMetadata> fileMetadataList = metadata.getFileMetadataList();
                    int dataFileCount = 0;
                    int deleteFileCount = 0;
                    
                    for (IcebergPayload.FileMetadata fileMeta : fileMetadataList) {
                        String fileType = fileMeta.getFileType();
                        String filePath = fileMeta.getFilePath();
                        
                        LOGGER.info("{} File type: {}, path: {}", requestId, fileType, filePath);
                        
                        switch (fileType) {
                         case "delete":
                             int fieldId = IcebergUtil.getFieldId(this.icebergTable, "_olake_id");
                             icebergTableOperator.registerDeleteFile(
                                 threadId,
                                 icebergTable,
                                 java.util.Collections.singletonList(filePath),
                                 fieldId
                             );
                             deleteFileCount++;
                             LOGGER.info("{} Successfully registered delete file", requestId);
                             break;
             
                         case "data":
                             icebergTableOperator.registerDataFile(
                                 threadId,
                                 icebergTable,
                                 java.util.Collections.singletonList(filePath)
                             );
                             dataFileCount++;
                             LOGGER.info("{} Successfully registered data file", requestId);
                             break;
             
                         default:
                             LOGGER.warn("{} Unknown file type '{}' for path: {}", requestId, fileType, filePath);
                             break;
                        }
                    }
                    
                    sendResponse(responseObserver, String.format("Successfully registered %d data files and %d delete files for thread %s", 
                                                                  dataFileCount, deleteFileCount, threadId));
                    break;
                
                case GET_FIELD_ID:
                    LOGGER.info("{} Received GET_FIELD_ID request for thread: {}", requestId, threadId);
                    String fieldName = metadata.getFieldName();
                    if (fieldName == null || fieldName.isEmpty()) {
                        throw new IllegalArgumentException("Field name is required for GET_FIELD_ID request");
                    }
                    int fieldId = IcebergUtil.getFieldId(this.icebergTable, fieldName);
                    sendResponse(responseObserver, String.valueOf(fieldId));
                    LOGGER.info("{} Field '{}' has ID: {}", requestId, fieldName, fieldId);
                    break;
                
                case UPLOAD_FILE:
                    LOGGER.info("{} Received UPLOAD_FILE request for thread: {}", requestId, threadId);
                    IcebergPayload.FileUploadRequest uploadReq = metadata.getFileUpload();
                    
                    byte[] fileData = uploadReq.getFileData().toByteArray();
                    String fileType = uploadReq.getFileType();
                    String partitionKey = uploadReq.getPartitionKey();
                    String filename = uploadReq.getFilename();
                    
                    LOGGER.info("{} Uploading {} file: {} (size: {} bytes, partition: {})", 
                        requestId, fileType, filename, fileData.length, partitionKey);
                    
                    org.apache.iceberg.io.FileIO fileIO = this.icebergTable.io();
                    org.apache.iceberg.io.LocationProvider locations = this.icebergTable.locationProvider();
                    
                    String icebergLocation;
                    if (partitionKey != null && !partitionKey.isEmpty()) {
                        String baseLocation = locations.newDataLocation(filename);
                        int lastSlash = baseLocation.lastIndexOf('/');
                        if (lastSlash > 0) {
                            String basePath = baseLocation.substring(0, lastSlash);
                            icebergLocation = basePath + "/" + partitionKey + "/" + filename;
                        } else {
                            icebergLocation = partitionKey + "/" + filename;
                        }
                    } else {
                        icebergLocation = locations.newDataLocation(filename);
                    }
                    
                    org.apache.iceberg.io.OutputFile outputFile = fileIO.newOutputFile(icebergLocation);
                    try (java.io.OutputStream out = outputFile.create()) {
                        out.write(fileData);
                        out.flush();
                    }
                    
                    LOGGER.info("{} Successfully uploaded file to: {}", requestId, icebergLocation);
                    sendResponse(responseObserver, icebergLocation);
                    break;
                
                case GENERATE_FILENAME:
                    LOGGER.debug("{} Received GENERATE_FILENAME request for thread: {}", requestId, threadId);
                    
                    org.apache.iceberg.FileFormat fileFormat = IcebergUtil.getTableFileFormat(this.icebergTable);
                    org.apache.iceberg.io.OutputFileFactory fileFactory = 
                        IcebergUtil.getTableOutputFileFactory(this.icebergTable, fileFormat);
                    
                    org.apache.iceberg.encryption.EncryptedOutputFile encryptedFile = fileFactory.newOutputFile();
                    String fullPath = encryptedFile.encryptingOutputFile().location();
                    
                    int lastSlashIndex = fullPath.lastIndexOf('/');
                    String generatedFilename = lastSlashIndex >= 0 
                        ? fullPath.substring(lastSlashIndex + 1) 
                        : fullPath;
                    
                    LOGGER.info("{} Generated filename: {}", requestId, generatedFilename);
                    
                    RecordIngest.RecordIngestResponse filenameResponse = 
                        RecordIngest.RecordIngestResponse.newBuilder()
                            .setResult("success")
                            .setSuccess(true)
                            .setFilename(generatedFilename)
                            .build();
                    responseObserver.onNext(filenameResponse);
                    responseObserver.onCompleted();
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