/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.debezium.server.iceberg.RecordConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;
  
  // Static lock object for synchronizing commits across threads
  private static final Object COMMIT_LOCK = new Object();
  
  // Store pending write results per table for later commit
  private final Map<String, List<WriteResult>> pendingWrites = new ConcurrentHashMap<>();
  // Store table references for commit
  private final Map<String, Table> tableReferences = new ConcurrentHashMap<>();
  // Store writers per schema to reuse them across multiple writeToTable calls
  private final Map<RecordConverter.SchemaConverter, BaseTaskWriter<Record>> writersBySchema = new ConcurrentHashMap<>();

  public IcebergTableOperator() {
    createIdentifierFields = true;
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = true;
    allowFieldAddition = true;
    upsert = true;
    cdcOpField = "_op_type";
    cdcSourceTsMsField = "_cdc_timestamp";
  }

  public IcebergTableOperator(boolean upsert_records) {
    createIdentifierFields = true;
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = upsert_records;
    allowFieldAddition = true;
    upsert = upsert_records;
    cdcOpField = "_op_type";
    cdcSourceTsMsField = "_cdc_timestamp";
  }

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY = ImmutableMap.of(Operation.INSERT, 1, Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-dedup-column", defaultValue = "_cdc_timestamp")
  String cdcSourceTsMsField;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-op-field", defaultValue = "_op_type")
  String cdcOpField;
  @ConfigProperty(name = "debezium.sink.iceberg.allow-field-addition", defaultValue = "true")
  boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.iceberg.create-identifier-fields", defaultValue = "true")
  boolean createIdentifierFields;
  @Inject
  IcebergTableWriterFactory writerFactory;

  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;

  protected List<RecordConverter> deduplicateBatch(List<RecordConverter> events) {

    ConcurrentHashMap<JsonNode, RecordConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e -> {
          if (e.key() == null || e.key().isNull()) {
            throw new RuntimeException("Cannot deduplicate data with null key! destination:'" + e.destination() + "' event: '" + e.value().toString() + "'");
          }

      try {
        // deduplicate using key(PK)
        deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
          if (this.compareByTsThenOp(oldValue, newValue) <= 0) {
            return newValue;
          } else {
            return oldValue;
          }
        });
      } catch (Exception ex) {
        throw new RuntimeException("Failed to deduplicate events", ex);
      }
        }
    );

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   * <p>
   * Forex ample a record can be updated multiple times in the source. for example insert followed by update and
   * delete. for this case we need to only pick last change event for the row.
   * <p>
   * Its used when `upsert` feature enabled (when the consumer operating non append mode) which means it should not add
   * duplicate records to target table.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(RecordConverter lhs, RecordConverter rhs) {

    int result = Long.compare(lhs.cdcSourceTsMsValue(cdcSourceTsMsField), rhs.cdcSourceTsMsValue(cdcSourceTsMsField));

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = CDC_OPERATION_PRIORITY.getOrDefault(lhs.cdcOpValue(cdcOpField), -1)
          .compareTo(
              CDC_OPERATION_PRIORITY.getOrDefault(rhs.cdcOpValue(cdcOpField), -1)
          );
    }

    return result;
  }

  /**
   * If given schema contains new fields compared to target table schema then it adds new fields to target iceberg
   * table.
   * <p>
   * Its used when allow field addition feature is enabled.
   *
   * @param icebergTable
   * @param newSchema
   */
  private void applyFieldAddition(Table icebergTable, Schema newSchema) {

    UpdateSchema us = icebergTable.updateSchema().
        unionByNameWith(newSchema).
        setIdentifierFields(newSchema.identifierFieldNames());
    Schema newSchemaCombined = us.apply();

    // @NOTE avoid committing when there is no schema change. commit creates new commit even when there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }
  }

  /**
   * Writes data to iceberg files but does not commit. Files are stored for later commit.
   *
   * @param icebergTable
   * @param events
   */
  public void writeToTable(Table icebergTable, List<RecordConverter> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!allowFieldAddition) {
      // if field additions not enabled add set of events to table
      // For single schema, use a dummy schema converter key
      RecordConverter.SchemaConverter dummySchemaConverter = events.get(0).schemaConverter();
      BaseTaskWriter<Record> writer = writersBySchema.computeIfAbsent(dummySchemaConverter, k -> {
        LOGGER.info("Creating new writer for single schema");
        return writerFactory2.create(icebergTable);
      });
      writeToTablePerSchema(icebergTable, events, writer);
    } else {
      
      Map<RecordConverter.SchemaConverter, List<RecordConverter>> eventsGroupedBySchema =
          events.parallelStream()
              .collect(Collectors.groupingBy(RecordConverter::schemaConverter));
      
      LOGGER.info("Batch got {} records with {} different schema!!", events.size(), eventsGroupedBySchema.keySet().size());

      for (Map.Entry<RecordConverter.SchemaConverter, List<RecordConverter>> schemaEvents : eventsGroupedBySchema.entrySet()) {
        RecordConverter.SchemaConverter schemaConverter = schemaEvents.getKey();
        List<RecordConverter> schemaEventsList = schemaEvents.getValue();
        
        // extend table schema if new fields found
        applyFieldAddition(icebergTable, schemaEventsList.get(0).icebergSchema(createIdentifierFields));
        
        // Get or create writer for this schema
        BaseTaskWriter<Record> writer = writersBySchema.computeIfAbsent(schemaConverter, k -> {
          LOGGER.info("Creating new writer for schema: {}", k.hashCode());
          return writerFactory2.create(icebergTable);
        });
        
        // add set of events to table using the schema-specific writer
        writeToTablePerSchema(icebergTable, schemaEventsList, writer);
      }
    }
    
    // Store table reference for commit (done here since we know the table)
    tableReferences.put(icebergTable.name(), icebergTable);
  }

  /**
   * Commits all pending writes across all tables
   */
  public void commitPendingWrites() {
    synchronized(COMMIT_LOCK) {
      try {
        // First complete all writers and collect their results
        completeAllWriters();
        
        if (pendingWrites.isEmpty()) {
          LOGGER.info("No pending writes to commit");
          return;
        }
        
        LOGGER.info("Committing pending writes for {} tables", pendingWrites.size());
        
        for (Map.Entry<String, List<WriteResult>> entry : pendingWrites.entrySet()) {
          String tableName = entry.getKey();
          List<WriteResult> writeResults = entry.getValue();
          Table table = tableReferences.get(tableName);
          
          if (table == null) {
            LOGGER.error("Table reference not found for: {}", tableName);
            throw new RuntimeException("Table reference not found for: " + tableName);
          }
          
          try {
            // Refresh table before committing to get the latest state
            table.refresh();
            
            // Commit each write result
            for (WriteResult writeResult : writeResults) {
              if (writeResult.deleteFiles().length > 0) {
                RowDelta newRowDelta = table.newRowDelta();
                Arrays.stream(writeResult.dataFiles()).forEach(newRowDelta::addRows);
                Arrays.stream(writeResult.deleteFiles()).forEach(newRowDelta::addDeletes);
                newRowDelta.commit();
              } else {
                AppendFiles appendFiles = table.newAppend();
                Arrays.stream(writeResult.dataFiles()).forEach(appendFiles::appendFile);
                appendFiles.commit();
              }
            }
            
            LOGGER.info("Successfully committed {} write operations for table: {}", writeResults.size(), tableName);
          } catch (Exception e) {
            LOGGER.error("Failed to commit writes for table: {}", tableName, e);
            throw new RuntimeException("Failed to commit writes for table: " + tableName, e);
          }
        }
        
        LOGGER.info("All pending writes committed successfully");
      } catch (Exception e) {
        // Abort all writers in case of failure
        abortAllWriters();
        throw e;
      } finally {
        // Clear all state after commit (successful or failed)
        cleanupAfterCommit();
      }
    }
  }

  /**
   * Complete all active writers and collect their WriteResults
   */
  private void completeAllWriters() {
    LOGGER.info("Completing {} active writers", writersBySchema.size());
    
    for (Map.Entry<RecordConverter.SchemaConverter, BaseTaskWriter<Record>> entry : writersBySchema.entrySet()) {
      RecordConverter.SchemaConverter schemaConverter = entry.getKey();
      BaseTaskWriter<Record> writer = entry.getValue();
      
      try {
        WriteResult writeResult = writer.complete();
        
        // For now, we'll associate all write results with the first table found
        // This could be improved by tracking table per schema if needed
        String tableName = tableReferences.keySet().iterator().next();
        
        // Store the write result for commit
        pendingWrites.computeIfAbsent(tableName, k -> new ArrayList<>()).add(writeResult);
        
        LOGGER.debug("Completed writer for schema: {}", schemaConverter.hashCode());
      } catch (Exception e) {
        LOGGER.error("Failed to complete writer for schema: {}", schemaConverter.hashCode(), e);
        throw new RuntimeException("Failed to complete writer for schema: " + schemaConverter.hashCode(), e);
      }
    }
  }

  /**
   * Abort all active writers in case of failure
   */
  private void abortAllWriters() {
    LOGGER.warn("Aborting {} active writers due to failure", writersBySchema.size());
    
    for (Map.Entry<RecordConverter.SchemaConverter, BaseTaskWriter<Record>> entry : writersBySchema.entrySet()) {
      RecordConverter.SchemaConverter schemaConverter = entry.getKey();
      BaseTaskWriter<Record> writer = entry.getValue();
      
      try {
        writer.abort();
        LOGGER.debug("Aborted writer for schema: {}", schemaConverter.hashCode());
      } catch (IOException e) {
        LOGGER.warn("Failed to abort writer for schema: {}", schemaConverter.hashCode(), e);
      }
    }
  }

  /**
   * Close all writers and cleanup state
   */
  private void cleanupAfterCommit() {
    // Close all writers
    for (Map.Entry<RecordConverter.SchemaConverter, BaseTaskWriter<Record>> entry : writersBySchema.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close writer for schema: {}", entry.getKey().hashCode(), e);
      }
    }
    
    // Clear all state
    writersBySchema.clear();
    pendingWrites.clear();
    tableReferences.clear();
  }

  /**
   * Writes data to iceberg files for a specific schema but does not commit
   *
   * @param icebergTable
   * @param events
   * @param writer the pre-created writer to use for this schema
   */
  private void writeToTablePerSchema(Table icebergTable, List<RecordConverter> events, BaseTaskWriter<Record> writer) {
    
    try {
      // Write all events
      // Parallelize the conversion step, then collect and write sequentially for thread safety
      List<RecordWrapper> convertedRecords = events.parallelStream()
          .map(e -> (upsert && !icebergTable.schema().identifierFieldIds().isEmpty())
              ? e.convert(icebergTable.schema(), cdcOpField)
              : e.convertAsAppend(icebergTable.schema()))
          .collect(Collectors.toList());
          
      // Write converted records sequentially to maintain thread safety with the writer
      for (RecordWrapper record : convertedRecords) {
          writer.write(record);
      }
      
      LOGGER.info("Successfully wrote {} events to writer for table: {}", events.size(), icebergTable.name());
      
    } catch (Exception ex) {
      LOGGER.error("Failed to write data to writer for table: {}", icebergTable.name(), ex);
      throw new RuntimeException("Failed to write data for table: " + icebergTable.name(), ex);
    }
  }
}
