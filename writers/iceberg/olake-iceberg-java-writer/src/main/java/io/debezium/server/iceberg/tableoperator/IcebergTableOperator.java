/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;
  Table icebergTable;
  BaseTaskWriter<Record> writer;
  Set<Schema> schemaHash;

  public IcebergTableOperator(Table icebergTable) {
    this.icebergTable = icebergTable;
    createIdentifierFields = true;
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = true;
    allowFieldAddition = true;
    upsert = true;
    cdcOpField = "_op_type";
    cdcSourceTsMsField = "_cdc_timestamp";
    writer = writerFactory2.create(icebergTable);
    schemaHash = new HashSet<>();
  }

  public IcebergTableOperator(boolean upsert_records, Table icebergTable) {
    this.icebergTable = icebergTable;
    createIdentifierFields = true;
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = upsert_records;
    allowFieldAddition = true;
    upsert = upsert_records;
    cdcOpField = "_op_type";
    cdcSourceTsMsField = "_cdc_timestamp";
    writer = writerFactory2.create(icebergTable);
    schemaHash = new HashSet<>();
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

    // Check if the new schema is already in the set
    boolean schemaExists = schemaHash.stream().parallel()
        .anyMatch(schema -> schema.sameSchema(newSchema));

    if (schemaExists) {
      return;
    }

    UpdateSchema us = icebergTable.updateSchema().
        unionByNameWith(newSchema).
        setIdentifierFields(newSchema.identifierFieldNames());
    Schema newSchemaCombined = us.apply();

    // @NOTE avoid committing when there is no schema change. commit creates new commit even when there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }

    schemaHash.add(newSchema);
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
  public void addToTable(Table icebergTable, RecordConverter event) {

    if (!allowFieldAddition) {
      // if field additions not enabled add set of events to table
      addToTablePerSchema(icebergTable, event);
    } else {
      
      applyFieldAddition(icebergTable, event.icebergSchema(createIdentifierFields));
        // add set of events to table
      addToTablePerSchema(icebergTable, event);
    }

  }

  /**
   * Adds list of change events to iceberg table. All the events are having same schema.
   *
   * @param icebergTable
   * @param events
   */
  private void addToTablePerSchema(Table icebergTable, RecordConverter event) {
    try {
        // Convert record based on upsert mode and table schema
        RecordWrapper convertedRecord = upsert && !icebergTable.schema().identifierFieldIds().isEmpty()
                ? event.convert(icebergTable.schema(), cdcOpField)
                : event.convertAsAppend(icebergTable.schema());
            
        // Write converted records sequentially to maintain thread safety with the writer
        writer.write(convertedRecord);
        
    } catch (Exception ex) {
      LOGGER.error("Failed to write data to table: {}", icebergTable.name(), ex);
      
      try {
        writer.abort();
        writer.close();
      } catch (IOException abortEx) {
        LOGGER.warn("Failed to abort writer", abortEx);
      }
      
      throw new RuntimeException("Failed to write data to table: " + icebergTable.name(), ex);
    }
  }

  /**
   * Commits pending changes to the Iceberg table.
   * This method should only be called by OlakeRowIngester when all writes are complete.
   * 
   * @return true if commit was successful, false otherwise
   */
  public boolean commitTable() {
    try {
      WriteResult files = writer.complete();
      
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
      
      LOGGER.info("Successfully committed changes to table");
      return true;
      
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
      LOGGER.error("Failed to commit: {}", ex.getMessage(), ex);
      
      try {
        writer.abort();
      } catch (IOException abortEx) {
        LOGGER.warn("Failed to abort writer", abortEx);
      }
      
      return false;
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close writer", e);
      }
    }
  }

  /**
   * Closes writer resources without committing
   */
  public void closeWithoutCommit() {
    try {
      writer.abort();
      writer.close();
    } catch (IOException e) {
      LOGGER.warn("Failed to close writer resources", e);
    }
  }
}
