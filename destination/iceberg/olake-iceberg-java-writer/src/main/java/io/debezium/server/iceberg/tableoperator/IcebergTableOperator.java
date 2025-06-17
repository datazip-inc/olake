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
import java.util.Set;
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
  
  // Lock object for synchronizing commits
  private final Object commitLock = new Object();
  
  // Map to track table references per thread
  private final Map<String, Table> threadTables = new ConcurrentHashMap<>();
  
  // Map to track open writers per thread
  private final Map<String, BaseTaskWriter<Record>> threadWriters = new ConcurrentHashMap<>();

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
   * Adds list of events to iceberg table.
   * <p>
   * If field addition enabled then it groups list of change events by their schema first. Then adds new fields to
   * iceberg table if there is any. And then follows with adding data to the table.
   * <p>
   * New fields are detected using CDC event schema, since events are grouped by their schemas it uses single
   * event to find-out schema for the whole list of events.
   *
   * @param icebergTable
   * @param events
   */
  public void addToTable(Table icebergTable, List<RecordConverter> events) {

    // when operation mode is not upsert deduplicate the events to avoid inserting duplicate row
    if (upsert && !icebergTable.schema().identifierFieldIds().isEmpty()) {
      events = deduplicateBatch(events);
    }

    if (!allowFieldAddition) {
      // if field additions not enabled add set of events to table
      addToTablePerSchema(icebergTable, events);
    } else {
      
      Map<RecordConverter.SchemaConverter, List<RecordConverter>> eventsGroupedBySchema =
          events.parallelStream()
              .collect(Collectors.groupingBy(RecordConverter::schemaConverter));
      
      LOGGER.info("Batch got {} records with {} different schema!!", events.size(), eventsGroupedBySchema.keySet().size());

      for (Map.Entry<RecordConverter.SchemaConverter, List<RecordConverter>> schemaEvents : eventsGroupedBySchema.entrySet()) {
        // extend table schema if new fields found
        applyFieldAddition(icebergTable, schemaEvents.getValue().get(0).icebergSchema(createIdentifierFields));
        // add set of events to table
        addToTablePerSchema(icebergTable, schemaEvents.getValue());
      }
    }

  }

  /**
   * Commits data files for a specific thread
   * 
   * @param threadId The thread ID to commit
   * @throws IOException if writer operations fail
   * @throws RuntimeException if commit fails
   */
  public void commitThread(String threadId) throws IOException {
    LOGGER.info("Committing data for thread: {}", threadId);
    
    // Get the writer and table for this thread
    BaseTaskWriter<Record> writer = threadWriters.remove(threadId);
    Table table = threadTables.remove(threadId);
    
    try {
      // Complete the writer to get the files
      WriteResult files = writer.complete();
      
      LOGGER.info("Writer for thread {} generated {} data files and {} delete files", 
                 threadId, files.dataFiles().length, files.deleteFiles().length);
      
      // If no files were generated, nothing to commit
      if (files.dataFiles().length == 0 && files.deleteFiles().length == 0) {
        LOGGER.info("No files to commit for thread: {}", threadId);
        return;
      }
      
      // Commit the files
      synchronized(commitLock) {
        try {
          // Refresh table before committing
          table.refresh();
          
          if (files.deleteFiles().length > 0) {
            RowDelta rowDelta = table.newRowDelta();
            Arrays.stream(files.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(files.deleteFiles()).forEach(rowDelta::addDeletes);
            rowDelta.commit();
          } else {
            AppendFiles appendFiles = table.newAppend();
            Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
            appendFiles.commit();
          }
          
          LOGGER.info("Successfully committed {} data files and {} delete files for thread: {}", 
                     files.dataFiles().length, files.deleteFiles().length, threadId);
        } catch (Exception e) {
          String errorMsg = String.format("Failed to commit data for thread %s: %s", threadId, e.getMessage());
          LOGGER.error(errorMsg, e);
          throw new RuntimeException(errorMsg, e);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Failed to complete writer for thread: {}", threadId, e);
      try {
        writer.abort();
      } catch (IOException abortEx) {
        LOGGER.warn("Failed to abort writer", abortEx);
      }
      throw e;
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close writer", e);
      }
    }
  }

  /**
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private void addToTablePerSchema(Table icebergTable, List<RecordConverter> events) {
    // Group events by thread ID to ensure proper file separation
    Map<String, List<RecordConverter>> eventsByThread = events.stream()
        .collect(Collectors.groupingBy(e -> {
          String threadId = e.getThreadId();
          if (threadId == null || threadId.isEmpty()) {
            throw new RuntimeException("Thread ID is required for all records");
          }
          return threadId;
        }));
    
    for (Map.Entry<String, List<RecordConverter>> threadEvents : eventsByThread.entrySet()) {
      String threadId = threadEvents.getKey();
      List<RecordConverter> threadSpecificEvents = threadEvents.getValue();
      
      // Get or create a writer for this thread
      BaseTaskWriter<Record> writer = threadWriters.computeIfAbsent(threadId, k -> {
        LOGGER.info("Creating new writer for thread: {}", k);
        return writerFactory2.create(icebergTable);
      });
      
      try {
        // Store table reference for this thread
        threadTables.put(threadId, icebergTable);
        
        // Write all events for this thread
        List<RecordWrapper> convertedRecords = threadSpecificEvents.parallelStream()
            .map(e -> (upsert && !icebergTable.schema().identifierFieldIds().isEmpty())
                ? e.convert(icebergTable.schema(), cdcOpField)
                : e.convertAsAppend(icebergTable.schema()))
            .collect(Collectors.toList());
            
        // Write converted records sequentially to maintain thread safety with the writer
        for (RecordWrapper record : convertedRecords) {
            writer.write(record);
        }
        
        LOGGER.info("Successfully wrote {} events for thread: {}", threadSpecificEvents.size(), threadId);
        
      } catch (Exception ex) {
        LOGGER.error("Failed to write data to table: {} for thread: {}", icebergTable.name(), threadId, ex);
        
        // Remove the failed writer
        BaseTaskWriter<Record> failedWriter = threadWriters.remove(threadId);
        if (failedWriter != null) {
          try {
            failedWriter.abort();
          } catch (IOException abortEx) {
            LOGGER.warn("Failed to abort writer", abortEx);
          }
          try {
            failedWriter.close();
          } catch (IOException e) {
            LOGGER.warn("Failed to close writer", e);
          }
        }
        
        throw new RuntimeException("Failed to write data to table: " + icebergTable.name(), ex);
      }
    }
  }
}
