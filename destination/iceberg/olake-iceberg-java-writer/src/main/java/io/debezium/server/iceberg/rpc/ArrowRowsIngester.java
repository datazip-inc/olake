package io.debezium.server.iceberg.rpc;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.grpc.stub.StreamObserver;

/**
 * Arrow-specific gRPC service implementation for file upload and filename generation.
 * This service is used by the Arrow writer implementation and is separate from the
 * traditional record ingestion service.
 */
public class ArrowRowsIngester extends ArrowRecordIngestServiceGrpc.ArrowRecordIngestServiceImplBase {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ArrowRowsIngester.class);
    private static final AtomicLong requestCounter = new AtomicLong(0);
    
    private final org.apache.iceberg.Table icebergTable;

    public ArrowRowsIngester(Catalog catalog, String namespace, String destinationTable, String threadId) {
        LOGGER.info("Initializing Arrow service for table: {} (thread: {})", destinationTable, threadId);
        TableIdentifier tableId = TableIdentifier.of(namespace, destinationTable);
        this.icebergTable = catalog.loadTable(tableId);
        if (this.icebergTable == null) {
            throw new IllegalStateException("Failed to load Iceberg table: " + destinationTable);
        }
    }

    @Override
    public void uploadFile(RecordIngest.ArrowFileUploadRequest request,
                          StreamObserver<RecordIngest.ArrowFileUploadResponse> responseObserver) {
        long requestId = requestCounter.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        try {
            String threadId = request.getThreadId();
            String filename = request.getFilename();
            String fileType = request.getFileType();
            String partitionKey = request.getPartitionKey();
            byte[] fileData = request.getFileData().toByteArray();
            
            LOGGER.info("[Arrow-{}] Uploading {} file: {} (size: {} bytes, partition: {}, thread: {})", 
                requestId, fileType, filename, fileData.length, partitionKey, threadId);
            
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
            
            long elapsed = System.currentTimeMillis() - startTime;
            LOGGER.info("[Arrow-{}] Successfully uploaded file to: {} (time: {} ms)", 
                requestId, icebergLocation, elapsed);
            
            RecordIngest.ArrowFileUploadResponse response = 
                RecordIngest.ArrowFileUploadResponse.newBuilder()
                    .setStoragePath(icebergLocation)
                    .setSuccess(true)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            String errorMessage = String.format("[Arrow-%d] Failed to upload file: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            
            RecordIngest.ArrowFileUploadResponse response = 
                RecordIngest.ArrowFileUploadResponse.newBuilder()
                    .setStoragePath("")
                    .setSuccess(false)
                    .setError(errorMessage)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void generateFilename(RecordIngest.ArrowFilenameRequest request,
                                 StreamObserver<RecordIngest.ArrowFilenameResponse> responseObserver) {
        long requestId = requestCounter.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        try {
            String threadId = request.getThreadId();
            
            LOGGER.debug("[Arrow-{}] Generating filename for thread: {}", requestId, threadId);
            
            org.apache.iceberg.FileFormat fileFormat = IcebergUtil.getTableFileFormat(this.icebergTable);
            org.apache.iceberg.io.OutputFileFactory fileFactory = 
                IcebergUtil.getTableOutputFileFactory(this.icebergTable, fileFormat);
            
            org.apache.iceberg.encryption.EncryptedOutputFile encryptedFile = fileFactory.newOutputFile();
            String fullPath = encryptedFile.encryptingOutputFile().location();
            
            int lastSlashIndex = fullPath.lastIndexOf('/');
            String generatedFilename = lastSlashIndex >= 0 
                ? fullPath.substring(lastSlashIndex + 1) 
                : fullPath;
            
            long elapsed = System.currentTimeMillis() - startTime;
            LOGGER.info("[Arrow-{}] Generated filename: {} (time: {} ms)", requestId, generatedFilename, elapsed);
            
            RecordIngest.ArrowFilenameResponse response = 
                RecordIngest.ArrowFilenameResponse.newBuilder()
                    .setFilename(generatedFilename)
                    .setSuccess(true)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            String errorMessage = String.format("[Arrow-%d] Failed to generate filename: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            
            RecordIngest.ArrowFilenameResponse response = 
                RecordIngest.ArrowFilenameResponse.newBuilder()
                    .setFilename("")
                    .setSuccess(false)
                    .setError(errorMessage)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
