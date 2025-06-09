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
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private void addToTablePerSchema(Table icebergTable, List<RecordConverter> events) {
    // Remove retry logic and add synchronization
    
    // Initialize the task writer
    BaseTaskWriter<Record> writer = writerFactory2.create(icebergTable);
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

      WriteResult files = writer.complete();
      
      // Synchronize the commit operation to prevent concurrent commits
      synchronized(COMMIT_LOCK) {
        // Refresh table again before committing to get the latest state
        icebergTable.refresh();
        
        if (files.deleteFiles().length > 0) {
          RowDelta newRowDelta = icebergTable.newRowDelta();
          Arrays.stream(files.dataFiles()).forEach(newRowDelta::addRows);
          Arrays.stream(files.deleteFiles()).forEach(newRowDelta::addDeletes);
          newRowDelta.commit();
        } else {
          AppendFiles appendFiles = icebergTable.newAppend();
          Arrays.stream(files.dataFiles()).forEach(appendFiles::appendFile);
          appendFiles.commit();
        }
      }
      
      LOGGER.info("Successfully committed {} events", events.size());
      
    } catch (org.apache.iceberg.exceptions.CommitFailedException e) {
      String errorMessage = e.getMessage();
      LOGGER.error("Commit failed: {}", errorMessage, e);
      
      try {
        writer.abort();
      } catch (IOException abortEx) {
        LOGGER.warn("Failed to abort writer", abortEx);
      }
      
      throw new RuntimeException("Failed to commit", e);
    } catch (Exception ex) {
      LOGGER.error("Failed to write data to table: {}", icebergTable.name(), ex);
      
      try {
        writer.abort();
      } catch (IOException abortEx) {
        LOGGER.warn("Failed to abort writer", abortEx);
      }
      
      throw new RuntimeException("Failed to write data to table: " + icebergTable.name(), ex);
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close writer", e);
      }
    }
  }
}
