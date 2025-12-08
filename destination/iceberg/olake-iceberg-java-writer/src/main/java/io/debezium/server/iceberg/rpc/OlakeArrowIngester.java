package io.debezium.server.iceberg.rpc;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

@Dependent
public class OlakeArrowIngester extends ArrowIngestServiceGrpc.ArrowIngestServiceImplBase {
     private static final Logger LOGGER = LoggerFactory.getLogger(OlakeArrowIngester.class);

     private final String icebergNamespace;
     private final Catalog icebergCatalog;
     private final IcebergTableOperator icebergTableOperator;
     private Table icebergTable;
     private org.apache.iceberg.io.OutputFileFactory outputFileFactory;

     public OlakeArrowIngester(boolean upsertRecords, String icebergNamespace, Catalog icebergCatalog) {
          this.icebergNamespace = icebergNamespace;
          this.icebergCatalog = icebergCatalog;
          this.icebergTableOperator = new IcebergTableOperator(upsertRecords);
          this.icebergTable = null;
          this.outputFileFactory = null;
     }

     @Override
     public void icebergAPI(ArrowPayload request, StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver) {
          String requestId = String.format("[Arrow-%d-%d]", Thread.currentThread().getId(), System.nanoTime());
          long startTime = System.currentTimeMillis();

          try {
               ArrowPayload.Metadata metadata = request.getMetadata();
               String threadId = metadata.getThreadId();
               String destTableName = metadata.getDestTableName();

               if (threadId == null || threadId.isEmpty()) {
                    throw new Exception("Thread id not present in metadata");
               }

               if (destTableName == null || destTableName.isEmpty()) {
                    throw new Exception("Destination table name not present in metadata");
               }

               if (this.icebergTable == null) {
                    this.icebergTable = loadIcebergTable(TableIdentifier.of(icebergNamespace, destTableName));
               }

               switch (request.getType()) {
                    case REGISTER:
                         java.util.List<ArrowPayload.FileMetadata> fileMetadataList = metadata.getFileMetadataList();
                         int dataFileCount = 0;
                         int deleteFileCount = 0;

                         for (ArrowPayload.FileMetadata fileMeta : fileMetadataList) {
                              String fileType = fileMeta.getFileType();
                              String filePath = fileMeta.getFilePath();
                              long recordCount = fileMeta.getRecordCount();

                              switch (fileType) {
                                   case "delete":
                                        if (!fileMeta.hasEqualityFieldId()) {
                                             throw new IllegalArgumentException(
                                                       "Equality field ID is required for delete files");
                                        }
                                        int fieldId = fileMeta.getEqualityFieldId();
                                        icebergTableOperator.accumulateDeleteFiles(
                                                  threadId,
                                                  icebergTable,
                                                  java.util.Collections.singletonList(filePath),
                                                  fieldId,
                                                  recordCount);
                                        deleteFileCount++;
                                        break;

                                   case "data":
                                        icebergTableOperator.accumulateDataFiles(
                                                  threadId,
                                                  icebergTable,
                                                  java.util.Collections.singletonList(filePath));
                                        dataFileCount++;
                                        break;

                                   default:
                                        LOGGER.warn("{} Unknown file type '{}' for path: {}", requestId, fileType,
                                                  filePath);
                                        break;
                              }
                         }

                         sendResponse(responseObserver,
                                   String.format(
                                             "Successfully accumulated %d data files and %d delete files for thread %s",
                                             dataFileCount, deleteFileCount, threadId));
                         break;

                    case COMMIT:
                         icebergTableOperator.commitThread(threadId, this.icebergTable);
                         sendResponse(responseObserver,
                                   String.format("Successfully registered data for thread %s", threadId));
                         break;

                    case GET_SCHEMA_ID:
                         this.icebergTable.refresh();
                         int currentSchemaId = this.icebergTable.schema().schemaId();
                         sendResponse(responseObserver, String.valueOf(currentSchemaId));
                         LOGGER.info("{} Current schema ID: {}", requestId, currentSchemaId);
                         break;

                    case GET_ALL_FIELD_IDS:
                         this.icebergTable.refresh();
                         java.util.Map<String, Integer> allFieldIds = IcebergUtil.getAllFieldIds(this.icebergTable);
                         RecordIngest.ArrowIngestResponse batchResponse = RecordIngest.ArrowIngestResponse.newBuilder()
                                   .setResult("Successfully retrieved all field IDs")
                                   .setSuccess(true)
                                   .putAllFieldIds(allFieldIds)
                                   .build();
                         responseObserver.onNext(batchResponse);
                         responseObserver.onCompleted();
                         LOGGER.info("{} Returned {} field IDs", requestId, allFieldIds.size());
                         break;

                    case UPLOAD_FILE:
                         LOGGER.info("{} Received UPLOAD_FILE request for thread: {}", requestId, threadId);
                         ArrowPayload.FileUploadRequest uploadReq = metadata.getFileUpload();

                         byte[] fileData = uploadReq.getFileData().toByteArray();
                         String fileType = uploadReq.getFileType();
                         String partitionKey = uploadReq.getPartitionKey();

                         // Generate filename using OutputFileFactory
                         if (this.outputFileFactory == null) {
                              org.apache.iceberg.FileFormat fileFormat = IcebergUtil
                                        .getTableFileFormat(this.icebergTable);
                              this.outputFileFactory = IcebergUtil.getTableOutputFileFactory(this.icebergTable,
                                        fileFormat);
                         }

                         org.apache.iceberg.encryption.EncryptedOutputFile encryptedFile = this.outputFileFactory
                                   .newOutputFile();
                         String fullPath = encryptedFile.encryptingOutputFile().location();
                         int lastSlashIndex = fullPath.lastIndexOf('/');
                         String generatedFilename = lastSlashIndex >= 0 ? fullPath.substring(lastSlashIndex + 1)
                                   : fullPath;

                         LOGGER.info("{} Uploading {} file: {} (size: {} bytes, partition: {})",
                                   requestId, fileType, generatedFilename, fileData.length, partitionKey);

                         org.apache.iceberg.io.FileIO fileIO = this.icebergTable.io();
                         org.apache.iceberg.io.LocationProvider locations = this.icebergTable.locationProvider();

                         String icebergLocation;
                         if (partitionKey != null && !partitionKey.isEmpty()) {
                              String baseLocation = locations.newDataLocation(generatedFilename);
                              int lastSlash = baseLocation.lastIndexOf('/');
                              if (lastSlash > 0) {
                                   String basePath = baseLocation.substring(0, lastSlash);
                                   icebergLocation = basePath + "/" + partitionKey + "/" + generatedFilename;
                              } else {
                                   icebergLocation = partitionKey + "/" + generatedFilename;
                              }
                         } else {
                              icebergLocation = locations.newDataLocation(generatedFilename);
                         }

                         org.apache.iceberg.io.OutputFile outputFile = fileIO.newOutputFile(icebergLocation);
                         try (java.io.OutputStream out = outputFile.create()) {
                              out.write(fileData);
                              out.flush();
                         }

                         LOGGER.info("{} Successfully uploaded file to: {}", requestId, icebergLocation);
                         sendResponse(responseObserver, icebergLocation);
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

     private void sendResponse(StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver, String message) {
          RecordIngest.ArrowIngestResponse response = RecordIngest.ArrowIngestResponse.newBuilder()
                    .setResult(message)
                    .setSuccess(true)
                    .build();
          responseObserver.onNext(response);
          responseObserver.onCompleted();
     }

     private Table loadIcebergTable(TableIdentifier tableIdentifier) throws Exception {
          if (icebergCatalog.tableExists(tableIdentifier)) {
               LOGGER.info("Loading existing Iceberg table: {}", tableIdentifier);
               return icebergCatalog.loadTable(tableIdentifier);
          }
          throw new Exception("Table does not exist: " + tableIdentifier);
     }
}
