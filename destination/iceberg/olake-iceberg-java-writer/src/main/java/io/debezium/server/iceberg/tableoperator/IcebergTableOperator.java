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
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;
  private static final String DEFAULT_WRITER_KEY = "default_writer";
  private final Map<String, BaseTaskWriter<Record>> writerMap = new ConcurrentHashMap<>();

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
   * Commits data files 
   * @throws RuntimeException if commit fails
   */
  public void commitThread(Table table) {
    if (table == null) {
      throw new IllegalArgumentException("table cannot be null");
    }

    try {
      // Refresh table before committing
      table.refresh();

      AppendFiles appendFiles = table.newAppend();
      RowDelta rowDelta = table.newRowDelta();
      int totalDataFiles = 0;
      int totalDeleteFiles = 0;
      boolean hasAnyDeleteFiles = false;

      // Add all data and delete files from all WriteResults
      for (BaseTaskWriter<Record> writer : writerMap.values()) {
        if (writer == null) {
          LOGGER.warn("skipping commit for writer: null writer found");
        }

        WriteResult writeResult;
        try {
          writeResult = writer.complete();
        } catch (IOException e) {
          LOGGER.error("Failed to complete writer", e);
          throw new RuntimeException("Failed to complete writer", e);
        }

        totalDataFiles += writeResult.dataFiles().length;
        totalDeleteFiles += writeResult.deleteFiles().length;

        // Check if any writer has delete files
        if (writeResult.deleteFiles().length > 0) {
          hasAnyDeleteFiles = true;
        }

        Arrays.stream(writeResult.dataFiles()).forEach(dataFile -> {
          appendFiles.appendFile(dataFile);
          rowDelta.addRows(dataFile);
        });
        Arrays.stream(writeResult.deleteFiles()).forEach(rowDelta::addDeletes);
      }

      // If no files were generated, nothing to commit
      if (totalDataFiles == 0 && totalDeleteFiles == 0) {
        LOGGER.info("No files to commit");
        return;
      }

      if (hasAnyDeleteFiles) {
        rowDelta.commit();
      } else {
        appendFiles.commit();
      }

      LOGGER.info("Successfully committed {} data files and {} delete files",
          totalDataFiles, totalDeleteFiles);

    } catch (Exception e) {
      LOGGER.error("Failed to commit", e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException("Failed to commit", e);
    } finally {
      // Close all writers
      for (BaseTaskWriter<Record> writer : writerMap.values()) {
        if (writer != null) {
          try {
            writer.close();
          } catch (IOException e) {
            LOGGER.warn("Failed to close writer", e);
          }
        }
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
  public void addToTablePerSchema(Table icebergTable, List<RecordWrapper> events) {
    try {
      for (RecordWrapper record : events) {
        try {
          if (icebergTable.spec().isUnpartitioned()) {
            // for unpartitioned use default writer
            BaseTaskWriter<Record> writer = writerMap.get(DEFAULT_WRITER_KEY);
            if (writer == null) {
              writer = writerFactory2.create(icebergTable);
              writerMap.put(DEFAULT_WRITER_KEY, writer);
            }
            writer.write(record);
          } else {
            // get partition value hash
            PartitionKey pk = new PartitionKey(icebergTable.spec(), icebergTable.schema());
            InternalRecordWrapper wrapper = new InternalRecordWrapper(icebergTable.schema().asStruct());
            pk.partition(wrapper.wrap(record));
            int numFields = icebergTable.spec().fields().size();
            String hash = "";
            for (int idx = 0; idx < numFields; idx++) { // Note: assuming key comes in order (if not we might require to
                                                        // sort them first)
              Object v1 = pk.get(idx, Object.class);
              hash = hash.concat(v1.toString());
            }
            BaseTaskWriter<Record> writer = writerMap.get(hash);
            if (writer == null) {
              writer = writerFactory2.create(icebergTable);
              writerMap.put(hash, writer);
            }
            writer.write(record);
          }
        } catch (Exception ex) {
          LOGGER.error("Failed to write data: {}, exception: {}", record, ex);
          throw ex;
        }
      }
      LOGGER.info("Successfully wrote {} events", events.size());

    } catch (Exception ex) {
      LOGGER.error("Failed to write data to table: {}, exception: {}", icebergTable.name(), ex);
      // Clean up the writer
      writerMap.values().forEach(writer -> {
        try {
          writer.abort();
        } catch (Exception e) {
          LOGGER.warn("Failed to abort writer", e);
        } finally {
          try {
            writer.close();
          } catch (IOException e) {
            LOGGER.warn("Failed to close writer", e);
          }
        }
      });
      writerMap.clear();
      throw new RuntimeException("Failed to write data to table: " + icebergTable.name(), ex);
    }
  }
}
