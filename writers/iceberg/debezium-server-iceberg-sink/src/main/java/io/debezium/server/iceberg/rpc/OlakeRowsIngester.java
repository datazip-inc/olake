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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.nio.charset.StandardCharsets;
import java.util.List;

import java.util.Map;
import java.util.stream.Collectors;


// This class is used to receive rows from the Olake Golang project and dump it into iceberg using prebuilt code here.
@Dependent
public class OlakeRowsIngester extends StringArrayServiceGrpc.StringArrayServiceImplBase {


    private String icebergNamespace = "public";
    Catalog icebergCatalog;

    private final IcebergTableOperator icebergTableOperator;

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
        // Retrieve the array of strings from the request
        List<String> messages = request.getMessagesList();

        Map<String, List<RecordConverter>> result =
                messages.stream()
                        .map(message -> {
                            try {
                                ObjectMapper objectMapper = new ObjectMapper();

                                // Read the entire JSON message into a Map<String, Object>:
                                Map<String, Object> messageMap =
                                        objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
                                        });

                                // Get the destination table:
                                String destinationTable = (String) messageMap.get("destination_table");

                                // Convert the "key" part back to a JSON string:
                                String keyString = objectMapper.writeValueAsString(messageMap.get("key"));

                                // Convert the "value" part back to a JSON string:
                                String valueString = objectMapper.writeValueAsString(messageMap.get("value"));

                                // Now keyString and valueString contain the JSON for their respective fields.
                                return new RecordConverter(destinationTable,
                                        valueString.getBytes(StandardCharsets.UTF_8),
                                        keyString.getBytes(StandardCharsets.UTF_8));
                                // TODO: implement key being null
                                // return new RecordConverter(destinationTable, value.getBytes(), key == null ? null : key.getBytes());
                            } catch (Exception e) {
                                throw new RuntimeException("Error parsing message as JSON", e);
                            }
                        })
                        .collect(Collectors.groupingBy(RecordConverter::destination));

        // consume list of events for each destination table
        for (Map.Entry<String, List<RecordConverter>> tableEvents : result.entrySet()) {
            Table icebergTable = this.loadIcebergTable(TableIdentifier.of(icebergNamespace, tableEvents.getKey()), tableEvents.getValue().get(0));
            icebergTableOperator.addToTable(icebergTable, tableEvents.getValue());
        }

        // Build and send a response
        Messaging.StringArrayResponse response = Messaging.StringArrayResponse.newBuilder()
                .setResult("Received " + messages.size() + " messages")
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

