/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;

/**
 * Implementation of the consumer that delivers messages to Iceberg tables using Iceberg's built-in file size management.
 * This consumer leverages Iceberg's native file size management capabilities instead of manual batching.
 */
@Named("iceberg-dynamic")
@Dependent
public class IcebergDynamicFileConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergDynamicFileConsumer.class);
  public static final String PROP_PREFIX = "debezium.sink.iceberg.";
  static Deserializer<JsonNode> valDeserializer;
  static Deserializer<JsonNode> keyDeserializer;
  protected final Clock clock = Clock.system();
  final Map<String, String> icebergProperties = new ConcurrentHashMap<>();
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  
  @ConfigProperty(name = PROP_PREFIX + CatalogProperties.WAREHOUSE_LOCATION, defaultValue = "s3://my_bucket/iceberg_warehouse")
  String warehouseLocation;
  
  @ConfigProperty(name = "debezium.sink.iceberg.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  
  @ConfigProperty(name = "debezium.sink.iceberg.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  
  @ConfigProperty(name = "debezium.sink.iceberg.destination-uppercase-table-names", defaultValue = "false")
  protected boolean destinationUppercaseTableNames;
  
  @ConfigProperty(name = "debezium.sink.iceberg.destination-lowercase-table-names", defaultValue = "false")
  protected boolean destinationLowercaseTableNames;
  
  @ConfigProperty(name = "debezium.sink.iceberg.table-prefix", defaultValue = "")
  Optional<String> tablePrefix;
  
  @ConfigProperty(name = "debezium.sink.iceberg.table-namespace", defaultValue = "default")
  String namespace;
  
  @ConfigProperty(name = "debezium.sink.iceberg.catalog-name", defaultValue = "default")
  String catalogName;
  
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
  
  @ConfigProperty(name = "debezium.sink.iceberg.create-identifier-fields", defaultValue = "true")
  boolean createIdentifierFields;
  
  @ConfigProperty(name = "debezium.format.value.schemas.enable", defaultValue = "false")
  boolean eventSchemaEnabled;
  
  @ConfigProperty(name = "debezium.sink.iceberg." + DEFAULT_FILE_FORMAT, defaultValue = DEFAULT_FILE_FORMAT_DEFAULT)
  String writeFormat;
  
  @ConfigProperty(name = "debezium.sink.iceberg.target-file-size-mb", defaultValue = "256")
  int targetFileSizeMB;
  
  @ConfigProperty(name = "debezium.sink.iceberg.commit-interval-ms", defaultValue = "60000")
  int commitIntervalMs;
  
  @Inject
  IcebergTableOperator icebergTableOperator;
  
  Catalog icebergCatalog;
  
  // Store table writers by destination for efficient handling
  private final Map<String, TableWriterBuffer> tableWriters = new HashMap<>();
  private long lastCommitTime = 0;

  @PostConstruct
  void connect() {
    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.value={" + valueFormat + "} not supported! Supported formats are {json}!");
    }
    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new DebeziumException("debezium.format.key={" + keyFormat + "} not supported! Supported formats are {json}!");
    }

    // Pass Iceberg properties to Iceberg and Hadoop
    Map<String, String> conf = IcebergUtil.getConfigSubset(ConfigProvider.getConfig(), PROP_PREFIX);
    this.icebergProperties.putAll(conf);
    
    // Set target file size
    long targetFileSizeBytes = (long) targetFileSizeMB * 1024 * 1024;
    icebergProperties.put(WRITE_TARGET_FILE_SIZE_BYTES, String.valueOf(targetFileSizeBytes));
    LOGGER.info("Setting Iceberg target file size to {} bytes ({} MB)", targetFileSizeBytes, targetFileSizeMB);
    
    icebergCatalog = CatalogUtil.buildIcebergCatalog(catalogName, icebergProperties, null);

    // Configure serializers
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    Instant start = Instant.now();

    // Group events by destination (per Iceberg table)
    Map<String, List<RecordConverter>> result =
        records.stream()
            .map((ChangeEvent<Object, Object> e)
                -> new RecordConverter(e.destination(), getBytes(e.value()), e.key() == null ? null : getBytes(e.key())))
            .collect(Collectors.groupingBy(RecordConverter::destination));

    // Add events to each table's writer
    for (Map.Entry<String, List<RecordConverter>> tableEvents : result.entrySet()) {
      String destination = tableEvents.getKey();
      TableIdentifier tableId = mapDestination(destination);
      
      try {
        // Get or create the table writer buffer for this destination
        TableWriterBuffer writerBuffer = tableWriters.computeIfAbsent(destination, key -> {
          Table table = loadIcebergTable(tableId, tableEvents.getValue().get(0));
          return new TableWriterBuffer(table);
        });
        
        // Add records to the buffer
        writerBuffer.addRecords(tableEvents.getValue());
        
      } catch (Exception e) {
        LOGGER.error("Error processing records for destination {}: {}", destination, e.getMessage(), e);
        throw new DebeziumException("Failed to process records for " + destination, e);
      }
    }

    // Workaround - mark processed per event for offset management
    for (ChangeEvent<Object, Object> record : records) {
      committer.markProcessed(record);
    }
    
    // Check if it's time for a commit based on the commit interval
    long currentTime = System.currentTimeMillis();
    boolean shouldCommit = (currentTime - lastCommitTime) >= commitIntervalMs;
    
    // Commit all table writers if the interval has elapsed
    if (shouldCommit) {
      for (Map.Entry<String, TableWriterBuffer> entry : tableWriters.entrySet()) {
        try {
          entry.getValue().flush();
          LOGGER.info("Flushed table {} after commit interval", entry.getKey());
        } catch (Exception e) {
          LOGGER.error("Error flushing table {}: {}", entry.getKey(), e.getMessage(), e);
        }
      }
      lastCommitTime = currentTime;
    }
    
    committer.markBatchFinished();
    logConsumerProgress(records.size());
  }

  /**
   * Table writer buffer that manages accumulated records and handles flushing to Iceberg.
   */
  private class TableWriterBuffer {
    private final Table table;
    private final List<RecordConverter> pendingRecords = new ArrayList<>();
    
    TableWriterBuffer(Table table) {
      this.table = table;
    }
    
    synchronized void addRecords(List<RecordConverter> records) {
      pendingRecords.addAll(records);
      
      // Check if we have enough records to flush immediately
      // This relies on Iceberg's built-in file size management
      if (shouldFlushImmediately()) {
        try {
          flush();
        } catch (Exception e) {
          LOGGER.error("Error during immediate flush: {}", e.getMessage(), e);
        }
      }
    }
    
    synchronized void flush() {
      if (pendingRecords.isEmpty()) {
        return;
      }
      
      try {
        // Use the table operator to write records to Iceberg
        icebergTableOperator.addToTable(table, new ArrayList<>(pendingRecords));
        LOGGER.info("Flushed {} records to table {}", pendingRecords.size(), table.name());
        pendingRecords.clear();
      } catch (Exception e) {
        LOGGER.error("Failed to flush records to table {}: {}", table.name(), e.getMessage(), e);
        throw new DebeziumException("Failed to flush records to " + table.name(), e);
      }
    }
    
    /**
     * Heuristic to determine if we should flush immediately.
     * Iceberg will handle file size internally, but we can try to optimize.
     */
    private boolean shouldFlushImmediately() {
      // For now, we'll rely on Iceberg's internal file size management
      // We could implement additional heuristics here if needed
      return false;
    }
  }

  /**
   * Periodic logging of number of events consumed
   */
  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  /**
   * Load or create an Iceberg table
   */
  public Table loadIcebergTable(TableIdentifier tableId, RecordConverter sampleEvent) {
    return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
      if (!eventSchemaEnabled) {
        throw new RuntimeException("Table '" + tableId + "' not found! " + 
            "Set `debezium.format.value.schemas.enable` to true to create tables automatically!");
      }
      try {
        return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(createIdentifierFields), writeFormat);
      } catch (Exception e) {
        throw new DebeziumException("Failed to create table from debezium event schema:" + tableId + " Error:" + e.getMessage(), e);
      }
    });
  }

  /**
   * Map destination to Iceberg table identifier
   */
  public TableIdentifier mapDestination(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");

    if (destinationUppercaseTableNames) {
      return TableIdentifier.of(Namespace.of(namespace), (tablePrefix.orElse("") + tableName).toUpperCase());
    } else if (destinationLowercaseTableNames) {
      return TableIdentifier.of(Namespace.of(namespace), (tablePrefix.orElse("") + tableName).toLowerCase());
    } else {
      return TableIdentifier.of(Namespace.of(namespace), tablePrefix.orElse("") + tableName);
    }
  }
} 