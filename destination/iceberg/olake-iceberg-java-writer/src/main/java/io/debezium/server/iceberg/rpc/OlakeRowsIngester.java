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
import java.util.ArrayList;

// This class is used to receive rows from the Olake Golang project and dump it into iceberg using prebuilt code here.
@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private String icebergNamespace = "public";
    Catalog icebergCatalog;
    boolean upsertRecords = true;
    boolean createIdentifierFields = true;
    private final IcebergTableOperator icebergTableOperator;
    // Create a single reusable ObjectMapper instance
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // List to store partition fields and their transforms - preserves order and allows duplicates
    private List<Map<String, String>> partitionTransforms = new ArrayList<>();
    // Lock object for table creation synchronization
    private final Object tableCreationLock = new Object();

    public OlakeRowsIngester(boolean upsertRecords, String icebergNamespace, Catalog icebergCatalog, List<Map<String, String>> partitionTransforms, boolean createIdentifierFields) {
        icebergTableOperator = new IcebergTableOperator(upsertRecords, createIdentifierFields);
        this.upsertRecords = upsertRecords;
        this.icebergNamespace = icebergNamespace;
        this.icebergCatalog = icebergCatalog;
        this.partitionTransforms = partitionTransforms;
        this.createIdentifierFields = createIdentifierFields;
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
                Map<String, Object> messageMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                if (messageMap.containsKey("commit") && messageMap.get("commit").equals(true) && messageMap.containsKey("thread_id")) {
                    // This is a commit request
                    String threadId = (String) messageMap.get("thread_id");
                    LOGGER.info("{} Received commit request for thread: {}", requestId, threadId);
                    icebergTableOperator.commitThread(threadId);
                    
                    RecordIngest.RecordIngestResponse response = RecordIngest.RecordIngestResponse.newBuilder()
                            .setResult(requestId + " Successfully committed data for thread " + threadId)
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    LOGGER.info("{} Successfully committed data for thread: {}", requestId, threadId);
                    return;
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
        // This is to prevent multiple writer threads from creating the same table at the same time.
        synchronized (tableCreationLock) {
            return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
                try {
                    return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(createIdentifierFields), "parquet", partitionTransforms);
                } catch (Exception e) {
                    String errorMessage = String.format("Failed to create table from debezium event schema: %s Error: %s", tableId, e.getMessage());
                    LOGGER.error(errorMessage, e);
                    throw new DebeziumException(errorMessage, e);
                }
            });
        }
    }
}

