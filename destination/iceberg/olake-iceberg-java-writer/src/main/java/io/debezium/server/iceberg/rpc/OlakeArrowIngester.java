package io.debezium.server.iceberg.rpc;

import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

@Dependent
public class OlakeArrowIngester extends ArrowIngestServiceGrpc.ArrowIngestServiceImplBase {
     private static final Logger LOGGER = LoggerFactory.getLogger(OlakeArrowIngester.class);
     private static final String FILE_TYPE_DATA = "data";
     private static final String FILE_TYPE_EQUALITY_DELETE = "equalityDelete";
     private static final String FILE_TYPE_POSITIONAL_DELETE = "positionalDelete";

     private final String icebergNamespace;
     private final Catalog icebergCatalog;
     private final IcebergTableOperator icebergTableOperator;
     private Table icebergTable;
     private OutputFileFactory outputFileFactory;

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
                    case JSONSCHEMA -> {
                         this.icebergTable.refresh(); // important for the case of schema evolution

                         Map<String, String> schemaMap = new HashMap<>();

                         Schema tableSchema = this.icebergTable.schema();
                         String dataSchemaJson = SchemaParser.toJson(tableSchema);
                         schemaMap.put(FILE_TYPE_DATA, dataSchemaJson);

                         NestedField olakeIdField = tableSchema.findField("_olake_id");
                         Schema deleteSchema = new Schema(
                                   tableSchema.schemaId(),
                                   Collections.singletonList(olakeIdField),
                                   tableSchema.identifierFieldIds());
                         String deleteSchemaJson = SchemaParser.toJson(deleteSchema);
                         schemaMap.put(FILE_TYPE_EQUALITY_DELETE, deleteSchemaJson);

                         Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
                         String posDeleteSchemaJson = SchemaParser.toJson(posDeleteSchema);
                         schemaMap.put(FILE_TYPE_POSITIONAL_DELETE, posDeleteSchemaJson);

                         sendSchemaResponse(responseObserver, "Schema JSON retrieved successfully", schemaMap);
                         break;
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
                                        NestedField olakeIdFieldForDelete = icebergTable.schema().findField("_olake_id");
                                        int fieldId = olakeIdFieldForDelete.fieldId();
                                        icebergTableOperator.registerEqDeleteFiles(
                                                  threadId,
                                                  icebergTable,
                                                  filePath,
                                                  fieldId,
                                                  recordCount,
                                                  fileMeta.getPartitionValuesList());
                                        eqDeleteFileCount++;
                                        break;
                                   }

                                   case FILE_TYPE_POSITIONAL_DELETE -> {
                                        icebergTableOperator.registerPosDeleteFiles(
                                                  threadId,
                                                  icebergTable,
                                                  filePath,
                                                  recordCount,
                                                  fileMeta.getPartitionValuesList());
                                        posDeleteFileCount++;
                                        break;
                                   }

                                   case FILE_TYPE_DATA -> {
                                        icebergTableOperator.registerDataFiles(
                                                  threadId,
                                                  icebergTable,
                                                  filePath,
                                                  fileMeta.getPartitionValuesList());
                                        dataFileCount++;
                                        break;
                                   }

                                   default -> {
                                        LOGGER.warn("{} Unknown file type '{}' for path: {}", requestId, fileType, filePath);
                                        break;
                                   }
                              }
                         }

                         icebergTableOperator.commitThread(threadId, this.icebergTable);
                         sendResponse(responseObserver,
                                   String.format(
                                             "Successfully committed %d data files, %d equality delete files, and %d positional delete files for thread %s",
                                             dataFileCount, eqDeleteFileCount, posDeleteFileCount, threadId));
                         break;
                    }

                    case UPLOAD_FILE -> {
                         ArrowPayload.FileUploadRequest uploadReq = metadata.getFileUpload();

                         byte[] fileData = uploadReq.getFileData().toByteArray();
                         String filePath = uploadReq.getFilePath();

                         FileIO fileIO = this.icebergTable.io();
                         OutputFile outputFile = fileIO.newOutputFile(filePath);
                         try (OutputStream out = outputFile.create()) {
                              out.write(fileData);
                              out.flush();
                         }

                         LOGGER.info("{} Successfully uploaded file to: {}", requestId, filePath);
                         sendResponse(responseObserver, filePath);
                         break;
                    }

                    case FILEPATH -> {
                         if (this.outputFileFactory == null) {
                              FileFormat fileFormat = IcebergUtil.getTableFileFormat(this.icebergTable);
                              this.outputFileFactory = IcebergUtil.getTableOutputFileFactory(this.icebergTable,
                                        fileFormat);
                         }

                         EncryptedOutputFile encryptedFile = this.outputFileFactory.newOutputFile();
                         String basePath = encryptedFile.encryptingOutputFile().location();

                         LOGGER.debug("{} Allocated base file path: {}", requestId, basePath);
                         sendResponse(responseObserver, basePath);
                         break;
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
          RecordIngest.ArrowIngestResponse response = RecordIngest.ArrowIngestResponse.newBuilder()
                    .setResult(message)
                    .build();
          responseObserver.onNext(response);
          responseObserver.onCompleted();
     }

     private void sendSchemaResponse(StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver, String message,
               Map<String, String> schemaMap) {
          RecordIngest.ArrowIngestResponse response = RecordIngest.ArrowIngestResponse.newBuilder()
                    .setResult(message)
                    .putAllIcebergSchemas(schemaMap)
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
