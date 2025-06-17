package io.debezium.server.iceberg.rpc;

import io.debezium.DebeziumException;
import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.RecordConverter;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.io.IOException;

// This class is used to receive rows from the Olake Golang project and dump it into iceberg using prebuilt code here.
@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private String icebergNamespace = "public";
    Catalog icebergCatalog;
    private final IcebergTableOperator icebergTableOperator;
    // Create a single reusable ObjectMapper instance
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // Map to store partition fields and their transforms
    private Map<String, String> partitionTransforms = new HashMap<>();

    public OlakeRowsIngester() {
        icebergTableOperator = new IcebergTableOperator();
    }

    public OlakeRowsIngester(boolean upsert_records) {
        icebergTableOperator = new IcebergTableOperator(upsert_records);
    }

    public void setIcebergNamespace(String icebergNamespace) {
        this.icebergNamespace = icebergNamespace;
    }

    public void setIcebergCatalog(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    public void setPartitionTransforms(Map<String, String> partitionTransforms) {
        this.partitionTransforms = partitionTransforms;
    }

    @Override
    public void sendRecords(RecordIngest.RecordIngestRequest request, StreamObserver<RecordIngest.RecordIngestResponse> responseObserver) {
        String requestId = String.format("[Thread-%d-%d]", Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();
        // Retrieve the array of strings from the request
        List<String> messages = request.getMessagesList();

        try {
            // First, check if this is a commit request
            if (messages.size() == 1) {
                String message = messages.get(0);
                try {
                    Map<String, Object> messageMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                    if (Boolean.TRUE.equals(messageMap.get("commit")) && messageMap.containsKey("thread_id")) {
                        // This is a commit request
                        String threadId = (String) messageMap.get("thread_id");
                        LOGGER.info("{} Received commit request for thread: {}", requestId, threadId);
                        
                        try {
                            icebergTableOperator.commitThread(threadId);
                            
                            RecordIngest.RecordIngestResponse response = RecordIngest.RecordIngestResponse.newBuilder()
                                    .setResult(requestId + " Successfully committed data for thread " + threadId)
                                    .build();
                            responseObserver.onNext(response);
                            responseObserver.onCompleted();
                            LOGGER.info("{} Successfully committed data for thread: {}", requestId, threadId);
                        } catch (Exception e) {
                            String errorMessage = String.format("%s Failed to commit thread %s: %s", requestId, threadId, e.getMessage());
                            LOGGER.error(errorMessage, e);
                            responseObserver.onError(io.grpc.Status.INTERNAL
                                    .withDescription(errorMessage)
                                    .withCause(e)
                                    .asRuntimeException());
                        }
                        return;
                    }
                } catch (Exception e) {
                    // Not a commit message, continue with normal processing
                    LOGGER.debug("Message is not a commit request, processing normally");
                }
            }
            
            // Normal record processing
            long parsingStartTime = System.currentTimeMillis();
            Map<String, List<RecordConverter>> result =
                    messages.parallelStream() // Use parallel stream for concurrent processing
                            .map(message -> {
                                try {
                                    // Read the entire JSON message into a Map<String, Object>:
                                    Map<String, Object> messageMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

                                    // Get the destination table:
                                    String destinationTable = (String) messageMap.get("destination_table");
                                    
                                    // Extract thread_id if present
                                    String threadId = (String) messageMap.get("thread_id");

                                    // Get key and value objects directly without re-serializing
                                    Object key = messageMap.get("key");
                                    Object value = messageMap.get("value");

                                    // Convert to bytes only once
                                    byte[] keyBytes = key != null ? objectMapper.writeValueAsBytes(key) : null;
                                    byte[] valueBytes = objectMapper.writeValueAsBytes(value);

                                    return new RecordConverter(destinationTable, valueBytes, keyBytes, threadId);
                                } catch (Exception e) {
                                    String errorMessage = String.format("%s Failed to parse message: %s", requestId, message);
                                    LOGGER.error(errorMessage, e);
                                    throw new RuntimeException(errorMessage, e);
                                }
                            })
                            .collect(Collectors.groupingBy(RecordConverter::destination));
            LOGGER.info("{} Parsing messages took: {} ms", requestId, (System.currentTimeMillis() - parsingStartTime));

            // consume list of events for each destination table
            long processingStartTime = System.currentTimeMillis();
            for (Map.Entry<String, List<RecordConverter>> tableEvents : result.entrySet()) {
                try {
                    Table icebergTable = this.loadIcebergTable(TableIdentifier.of(icebergNamespace, tableEvents.getKey()), tableEvents.getValue().get(0));
                    icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
                } catch (Exception e) {
                    String errorMessage = String.format("%s Failed to process table events for table: %s", requestId, tableEvents.getKey());
                    LOGGER.error(errorMessage, e);
                    throw e;
                }
            }
            LOGGER.info("{} Processing tables took: {} ms", requestId, (System.currentTimeMillis() - processingStartTime));

            // Build and send a response
            RecordIngest.RecordIngestResponse response = RecordIngest.RecordIngestResponse.newBuilder()
                    .setResult(requestId + " Received " + messages.size() + " messages")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            LOGGER.info("{} Total time taken: {} ms", requestId, (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            String errorMessage = String.format("%s Failed to process request: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(errorMessage).asRuntimeException());
        }
    }

    public Table loadIcebergTable(TableIdentifier tableId, RecordConverter sampleEvent) {
        return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
            try {
                return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(true), "parquet", partitionTransforms);
            } catch (Exception e) {
                String errorMessage = String.format("Failed to create table from debezium event schema: %s Error: %s", tableId, e.getMessage());
                LOGGER.error(errorMessage, e);
                throw new DebeziumException(errorMessage, e);
            }
        });
    }
    
    /**
     * Handles commit requests for specific threads.
     * This method will be called by the gRPC service once proto files are regenerated.
     * 
     * @param threadId The thread ID to commit
     * @throws IOException if writer operations fail
     * @throws RuntimeException if commit fails
     */
    public void commitThread(String threadId) throws IOException {
        LOGGER.info("Received commit request for thread: {}", threadId);
        icebergTableOperator.commitThread(threadId);
        LOGGER.info("Successfully committed data for thread: {}", threadId);
    }
}

