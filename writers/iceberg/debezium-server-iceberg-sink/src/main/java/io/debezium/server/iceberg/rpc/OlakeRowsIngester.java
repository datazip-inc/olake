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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// This class is used to receive rows from the Olake Golang project and dump it into iceberg using prebuilt code here.
@Dependent
public class OlakeRowsIngester extends StringArrayServiceGrpc.StringArrayServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private String icebergNamespace = "public";
    Catalog icebergCatalog;
    private final IcebergTableOperator icebergTableOperator;
    // Create a single reusable ObjectMapper instance
    private static final ObjectMapper objectMapper = new ObjectMapper();

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

    @Override
    public void sendStringArray(Messaging.StringArrayRequest request, StreamObserver<Messaging.StringArrayResponse> responseObserver) {
        String requestId = String.format("[Thread-%d-%d]", Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();
        // Retrieve the array of strings from the request
        List<String> messages = request.getMessagesList();

        long parsingStartTime = System.currentTimeMillis();
        Map<String, List<RecordConverter>> result =
                messages.parallelStream() // Use parallel stream for concurrent processing
                        .map(message -> {
                            try {
                                // Read the entire JSON message into a Map<String, Object>:
                                Map<String, Object> messageMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

                                // Get the destination table:
                                String destinationTable = (String) messageMap.get("destination_table");

                                // Get key and value objects directly without re-serializing
                                Object key = messageMap.get("key");
                                Object value = messageMap.get("value");

                                // Convert to bytes only once
                                byte[] keyBytes = key != null ? objectMapper.writeValueAsBytes(key) : null;
                                byte[] valueBytes = objectMapper.writeValueAsBytes(value);

                                return new RecordConverter(destinationTable, valueBytes, keyBytes);
                            } catch (Exception e) {
                                throw new RuntimeException("Error parsing message as JSON", e);
                            }
                        })
                        .collect(Collectors.groupingBy(RecordConverter::destination));
        LOGGER.info("{} Parsing messages took: {} ms", requestId, (System.currentTimeMillis() - parsingStartTime));

        // consume list of events for each destination table
        long processingStartTime = System.currentTimeMillis();
        for (Map.Entry<String, List<RecordConverter>> tableEvents : result.entrySet()) {
            Table icebergTable = this.loadIcebergTable(TableIdentifier.of(icebergNamespace, tableEvents.getKey()), tableEvents.getValue().get(0));
            icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
        }
        LOGGER.info("{} Processing tables took: {} ms", requestId, (System.currentTimeMillis() - processingStartTime));

        // Build and send a response
        Messaging.StringArrayResponse response = Messaging.StringArrayResponse.newBuilder()
                .setResult(requestId + " Received " + messages.size() + " messages")
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        LOGGER.info("{} Total time taken: {} ms", requestId, (System.currentTimeMillis() - startTime));
    }

    public Table loadIcebergTable(TableIdentifier tableId, RecordConverter sampleEvent) {
        return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
            try {
                return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(true), "parquet");
            } catch (Exception e) {
                throw new DebeziumException("Failed to create table from debezium event schema:" + tableId + " Error:" + e.getMessage(), e);
            }
        });
    }
}

