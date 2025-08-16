/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import com.google.common.collect.ImmutableMap;
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
import java.util.List;
/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;

  BaseTaskWriter<Record> writer;

  public IcebergTableOperator(boolean upsert_records) {
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = upsert_records;
    allowFieldAddition = true;
    upsert = upsert_records;
    cdcOpField = "_op_type";
    cdcSourceTsMsField = "_cdc_timestamp";
  }

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY = ImmutableMap.of(Operation.INSERT, 1,
      Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
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
   * If given schema contains new fields compared to target table schema then it
   * adds new fields to target iceberg
   * table.
   * <p>
   * Its used when allow field addition feature is enabled.
   *
   * @param icebergTable
   * @param newSchema
   */
  public void applyFieldAddition(Table icebergTable, Schema newSchema) {
    UpdateSchema us = icebergTable.updateSchema().unionByNameWith(newSchema);
    if (createIdentifierFields) {
      us.setIdentifierFields(newSchema.identifierFieldNames());
    }
    Schema newSchemaCombined = us.apply();
    // @NOTE avoid committing when there is no schema change. commit creates new
    // commit even when there is no change!
    if (!icebergTable.schema().sameSchema(newSchemaCombined)) {
      LOGGER.warn("Extending schema of {}", icebergTable.name());
      us.commit();
    }
  }
  /**
   * Commits data files for a specific thread
   * 
   * @param threadId The thread ID to commit
   * @throws RuntimeException if commit fails
   */
  public void commitThread(String threadId, Table table) {
    try {
      if (writer == null) {
        LOGGER.warn("No writer found for thread: {}", threadId);
        return;
      }
      WriteResult writeResult = writer.complete();
      LOGGER.info("Writer for thread {} completed with {} data files and {} delete files",
              threadId, writeResult.dataFiles().length, writeResult.deleteFiles().length);


      if (table == null) {
        LOGGER.warn("No table found for thread: {}", threadId);
        return;
      }

      // Calculate total files across all WriteResults
      int totalDataFiles = writeResult.dataFiles().length;
      int totalDeleteFiles =writeResult.deleteFiles().length;

      LOGGER.info("Committing {} data files and {} delete files for thread: {}",
          totalDataFiles, totalDeleteFiles, threadId);

      // If no files were generated, nothing to commit
      if (totalDataFiles == 0 && totalDeleteFiles == 0) {
        LOGGER.info("No files to commit for thread: {}", threadId);
        return;
      }

      // Commit the files
      try {
        // Refresh table before committing
        table.refresh();

        // Check if any WriteResult has delete files
        boolean hasDeleteFiles = totalDeleteFiles>0;

        if (hasDeleteFiles) {
          RowDelta rowDelta = table.newRowDelta();
          // Add all data and delete files from all WriteResults
          Arrays.stream(writeResult.dataFiles()).forEach(rowDelta::addRows);
          Arrays.stream(writeResult.deleteFiles()).forEach(rowDelta::addDeletes);
          rowDelta.commit();
        } else {
          AppendFiles appendFiles = table.newAppend();
          // Add all data files from all WriteResults
          Arrays.stream(writeResult.dataFiles()).forEach(appendFiles::appendFile);
          appendFiles.commit();
        }

        LOGGER.info("Successfully committed {} data files and {} delete files for thread: {}",
            totalDataFiles, totalDeleteFiles, threadId);
      } catch (Exception e) {
        String errorMsg = String.format("Failed to commit data for thread %s: %s", threadId, e.getMessage());
        LOGGER.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
    } catch (IOException e) {
        LOGGER.error("Failed to complete writer for thread: {}", threadId, e);
        throw new RuntimeException("Failed to complete writer for thread: " + threadId, e);
    } finally {
        // Close the writer
        try {
          if (writer != null) {
            writer.close();
          }
        } catch (IOException e) {
          LOGGER.warn("Failed to close writer for thread: {}", threadId, e);
        }
    }
  }
  
  
  /**
   * Adds list of change events to iceberg table. All the events are having same
   * schema.
   *
   * @param icebergTable
   * @param events
   */
  public void addToTablePerSchema(String threadID, Table icebergTable, List<RecordWrapper> events) {
    if (writer == null) {
      writer = writerFactory2.create(icebergTable);
    }

    try {
      for (RecordWrapper record : events) {
        try{
           writer.write(record);
        }catch (Exception ex) {
          LOGGER.error("Failed to write data: {}, exception: {}", record,ex);
          throw ex;
        }
      }
      LOGGER.info("Successfully wrote {} events for thread: {}", events.size(), threadID);

    } catch (Exception ex) {
      LOGGER.error("Failed to write data to table: {} for thread: {}, exception: {}", icebergTable.name(), threadID, ex);

      // Clean up the writer
      try {
        writer.abort();
      } catch (IOException abortEx) {
        LOGGER.warn("Failed to abort writer", abortEx);
      }
      try {
        writer.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close writer", e);
      }
      throw new RuntimeException("Failed to write data to table: " + icebergTable.name(), ex);
    }
  }
}
