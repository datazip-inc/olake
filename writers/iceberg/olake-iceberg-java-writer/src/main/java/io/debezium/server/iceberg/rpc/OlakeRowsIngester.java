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
import java.util.HashMap;

// This class is used to receive rows from the Olake Golang project and dump it into iceberg using prebuilt code here.
@Dependent
public class OlakeRowsIngester extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowsIngester.class);

    private String icebergNamespace = "public";
    private String icebergTableName = "olake_table";

    Catalog icebergCatalog;
    private IcebergTableOperator icebergTableOperator;
    // Create a single reusable ObjectMapper instance
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // Map to store partition fields and their transforms
    private Map<String, String> partitionTransforms = new HashMap<>();
    // Single volatile iceberg table instance
    private volatile Table icebergTable;
    // Lock object for thread safety
    private final Object tableLock = new Object();

    private final boolean upsert_records;
    // Add fields to track metrics
    private long totalRecords = 0;
    private long totalParsingTime = 0;
    // Add fields to track getOrCreateIcebergTable metrics
    private long totalTableCreationTime = 0;

    public OlakeRowsIngester(boolean upsert_records) {
        this.upsert_records = upsert_records;
        this.icebergTable = null;
    }

    public void setIcebergNamespace(String icebergNamespace) {
        this.icebergNamespace = icebergNamespace;
    }

    public void setIcebergTableName(String icebergTableName) {
        this.icebergTableName = icebergTableName;
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
        String message = request.getMessagesList().get(0);

        try {
            
            RecordConverter recordConverter;
            try {
                // Read the entire JSON message into a Map<String, Object>:
                Map<String, Object> messageMap = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});

                if(messageMap.get("commit") != null && (boolean) messageMap.get("commit")) {
                    // Calculate and log average time per record before committing
                    if (totalRecords > 0) {
                        double avgTimePerRecord = (double) totalParsingTime / totalRecords;
                        double avgTableCreationTime = (double) totalTableCreationTime / totalRecords;
                        LOGGER.info("{} Average parsing time per record: {} ms (Total records: {}, Total time: {} ms)", 
                            requestId, String.format("%.2f", avgTimePerRecord), totalRecords, totalParsingTime);
                        LOGGER.info("{} Average getOrCreateIcebergTable time per record: {} ms (Total time: {} ms)", 
                            requestId, String.format("%.2f", avgTableCreationTime), totalTableCreationTime);
                    }
                    commitTable();
                    RecordIngest.RecordIngestResponse response = RecordIngest.RecordIngestResponse.newBuilder()
                    .setResult(requestId + " Commit successful")
                    .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                // Get key and value objects directly without re-serializing
                Object key = messageMap.get("key");
                Object value = messageMap.get("value");

                // Convert to bytes only once
                byte[] keyBytes = key != null ? objectMapper.writeValueAsBytes(key) : null;
                byte[] valueBytes = objectMapper.writeValueAsBytes(value);

                recordConverter = new RecordConverter(icebergTableName, valueBytes, keyBytes);
                totalParsingTime += (System.currentTimeMillis() - startTime);
            } catch (Exception e) {
                String errorMessage = String.format("%s Failed to parse message: %s", requestId, message);
                LOGGER.error(errorMessage, e);
                throw new RuntimeException(errorMessage, e);
            }

            try {
                long tableStartTime = System.currentTimeMillis();
                Table table = getOrCreateIcebergTable(recordConverter);
                totalTableCreationTime += (System.currentTimeMillis() - tableStartTime);
                icebergTableOperator.addToTable(table, recordConverter);
                
            } catch (Exception e) {
                String errorMessage = String.format("%s Failed to process table events for table: %s", requestId, icebergTableName);
                LOGGER.error(errorMessage, e);
                throw e;
            }

            // Build and send a response
            RecordIngest.RecordIngestResponse response = RecordIngest.RecordIngestResponse.newBuilder()
                    .setResult(requestId + " Received 1 message")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            // Update metrics
            totalRecords++;
            
        } catch (Exception e) {
            String errorMessage = String.format("%s Failed to process request: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(errorMessage).asRuntimeException());
        }
    }


    public void commitTable() {
        // // Commit the changes after adding data to the table
        boolean commitSuccess = icebergTableOperator.commitTable();
        if (!commitSuccess) {
            throw new RuntimeException("Failed to commit changes to table");
        }
    }

    /**
     * Gets the existing iceberg table or creates a new one in a thread-safe manner using double-checked locking
     * @param recordConverter The record converter containing schema information
     * @return The iceberg Table instance
     */
    public Table getOrCreateIcebergTable(RecordConverter recordConverter) {
        Table localTable = icebergTable;
        if (localTable == null) {
            synchronized (tableLock) {
                localTable = icebergTable;
                if (localTable == null) {
                    TableIdentifier tableId = TableIdentifier.of(icebergNamespace, icebergTableName);
                    localTable = loadIcebergTable(tableId, recordConverter);
                    icebergTableOperator = new IcebergTableOperator(upsert_records, localTable);
                    icebergTable = localTable;
                }
            }
        }
        return localTable;
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
}

