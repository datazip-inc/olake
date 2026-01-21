/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.util.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
@Dependent
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;

  BaseTaskWriter<Record> writer;

  ArrayList<Pair<ArrayList<DeleteFile>, ArrayList<DataFile>>> filesToCommit = new ArrayList<>();

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
    icebergTable.refresh(); // for safe case
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
    if (table == null) {
      LOGGER.warn("No table found for thread: {}", threadId);
      return;
    }
  
    completeWriter();
  
    if (filesToCommit.isEmpty()) {
      LOGGER.info("No files to commit for thread: {}", threadId);
      return;
    }
  
    // Refresh once before committing
    table.refresh();
  
    boolean hasAnyDeletes = false;
    int totalDataFiles = 0;
    int totalDeleteFiles = 0;
  
    for (Pair<ArrayList<DeleteFile>, ArrayList<DataFile>> unit : filesToCommit) {
      ArrayList<DeleteFile> deletes = unit.first();
      ArrayList<DataFile> data = unit.second();
  
      int del = (deletes == null) ? 0 : deletes.size();
      int df = (data == null) ? 0 : data.size();
  
      totalDeleteFiles += del;
      totalDataFiles += df;
  
      if (del > 0) {
        hasAnyDeletes = true;
      }
    }
  
    if (totalDataFiles == 0 && totalDeleteFiles == 0) {
      LOGGER.info("No files to commit for thread: {}", threadId);
      filesToCommit.clear();
      return;
    }
  
    try {
      if (!hasAnyDeletes) {
        AppendFiles append = table.newAppend();
  
        for (Pair<ArrayList<DeleteFile>, ArrayList<DataFile>> unit : filesToCommit) {
          ArrayList<DataFile> dataFiles = unit.second();
          if (dataFiles == null || dataFiles.isEmpty()) {
            continue;
          }
          for (DataFile df : dataFiles) {
            append.appendFile(df);
          }
        }
  
        append.commit();
  
        LOGGER.info("Append-only commit success: {} data files ({} deletes) for thread: {}",
            totalDataFiles, totalDeleteFiles, threadId);
  
      } else {
        Transaction txn = table.newTransaction();
        for (Pair<ArrayList<DeleteFile>, ArrayList<DataFile>> unit : filesToCommit) {
          ArrayList<DeleteFile> eqDeletes = unit.first();
          ArrayList<DataFile> dataFiles = unit.second();
  
          int del = (eqDeletes == null) ? 0 : eqDeletes.size();
          int df = (dataFiles == null) ? 0 : dataFiles.size();
  
          if (del == 0 && df == 0) {
            continue;
          }
  
          RowDelta delta = txn.newRowDelta();

          if (dataFiles != null) {
            dataFiles.forEach(delta::addRows);
          }

          if (eqDeletes != null) {
            eqDeletes.forEach(delta::addDeletes);
          }
  
          delta.commit();
        }
  
        txn.commitTransaction();
  
        LOGGER.info("Txn commit success: {} data files + {} equality delete files for thread: {}",
            totalDataFiles, totalDeleteFiles, threadId);
      }
  
      filesToCommit.clear();
  
    } catch (Exception e) {
      String msg = String.format("Failed to commit for thread %s: %s", threadId, e.getMessage());
      LOGGER.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }
  

  public void completeWriter() {
    try {
      if (writer == null) {
        LOGGER.warn("no writer to complete");
        return;
      }
      WriteResult writerResult = writer.complete();
      ArrayList<DeleteFile> deleteFiles = new ArrayList<>(Arrays.asList(writerResult.deleteFiles()));
      ArrayList<DataFile> dataFiles = new ArrayList<>(Arrays.asList(writerResult.dataFiles()));
      filesToCommit.add(filesToCommit.size(), Pair.of(deleteFiles, dataFiles));
    } catch (IOException e) {
      LOGGER.error("Failed to complete writer", e);
      throw new RuntimeException("Failed to complete writer", e);
    } finally {
      // Close the writer
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to close writer", e);
      }
      // to reinitiate 
      writer = null;
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

     public void registerDataFiles(String threadId, Table table, String filePath,
               List<ArrowPayload.FileMetadata.PartitionValue> partitionValues) {
          try {
               FileIO fileIO = table.io();
               MetricsConfig metricsConfig = MetricsConfig.forTable(table);

               InputFile inputFile = fileIO.newInputFile(filePath);
               Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig);

               DataFiles.Builder dataFileBuilder = DataFiles.builder(table.spec())
                         .withPath(filePath)
                         .withFormat(FileFormat.PARQUET)
                         .withFileSizeInBytes(inputFile.getLength())
                         .withMetrics(metrics);

               if (partitionValues != null && !partitionValues.isEmpty()) {
                    PartitionData partitionData = partitionDataFromTypedValues(table.spec(), partitionValues);
                    dataFileBuilder.withPartition(partitionData);
                    LOGGER.debug("Thread {}: data file scoped to partition with {} values", threadId,
                              partitionValues.size());
               } else {
                    LOGGER.debug("Thread {}: data file created as global (unpartitioned)", threadId);
               }

               DataFile dataFile = dataFileBuilder.build();
               if (filesToCommit.size() > 0) {
                filesToCommit.get(0).second().add(dataFile);
               } else {
                filesToCommit.add(Pair.of(new ArrayList<DeleteFile>(), new ArrayList<>(Arrays.asList(dataFile))));
               }
               LOGGER.info("Thread {}: accumulated data file {} (total: {})", threadId, filePath,
                         filesToCommit.get(0).second().size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to register data file %s: %s", threadId, filePath,
                         e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

     public void registerEqDeleteFiles(String threadId, Table table, String filePath, int equalityFieldId,
               long recordCount, List<ArrowPayload.FileMetadata.PartitionValue> partitionValues) {
          try {
               FileIO fileIO = table.io();
               InputFile inputFile = fileIO.newInputFile(filePath);
               long fileSize = inputFile.getLength();

               FileMetadata.Builder deleteFileBuilder = FileMetadata.deleteFileBuilder(table.spec())
                         .ofEqualityDeletes(equalityFieldId)
                         .withPath(filePath)
                         .withFormat(FileFormat.PARQUET)
                         .withFileSizeInBytes(fileSize)
                         .withRecordCount(recordCount);

               if (partitionValues != null && !partitionValues.isEmpty()) {
                    PartitionData partitionData = partitionDataFromTypedValues(table.spec(), partitionValues);
                    deleteFileBuilder.withPartition(partitionData);
                    LOGGER.debug("Thread {}: delete file scoped to partition with {} values", threadId,
                              partitionValues.size());
               } else {
                    LOGGER.debug("Thread {}: delete file scoped to global (unpartitioned)", threadId);
               }

               DeleteFile deleteFile = deleteFileBuilder.build();
               if (filesToCommit.size() > 0) {
                filesToCommit.get(0).first().add(deleteFile);
               } else {
                filesToCommit.add(Pair.of(new ArrayList<>(Arrays.asList(deleteFile)), new ArrayList<DataFile>()));
               }
               LOGGER.info("Thread {}: accumulated delete file {} with equality field ID {} (total: {})",
                         threadId, filePath, equalityFieldId, filesToCommit.get(0).first().size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to register delete file %s: %s", threadId, filePath,
                         e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

     public void registerPosDeleteFiles(String threadId, Table table, String filePath,
               long recordCount, List<ArrowPayload.FileMetadata.PartitionValue> partitionValues) {
          try {
               FileIO fileIO = table.io();
               InputFile inputFile = fileIO.newInputFile(filePath);
               long fileSize = inputFile.getLength();

               FileMetadata.Builder deleteFileBuilder = FileMetadata.deleteFileBuilder(table.spec())
                         .ofPositionDeletes()
                         .withPath(filePath)
                         .withFormat(FileFormat.PARQUET)
                         .withFileSizeInBytes(fileSize)
                         .withRecordCount(recordCount);

               if (partitionValues != null && !partitionValues.isEmpty()) {
                    PartitionData partitionData = partitionDataFromTypedValues(table.spec(), partitionValues);
                    deleteFileBuilder.withPartition(partitionData);
                    LOGGER.debug("Thread {}: positional delete file scoped to partition with {} values", threadId,
                              partitionValues.size());
               } else {
                    LOGGER.debug("Thread {}: positional delete file scoped to global (unpartitioned)", threadId);
               }

               DeleteFile deleteFile = deleteFileBuilder.build();
               deleteFiles.add(deleteFile);
               LOGGER.info("Thread {}: registered positional delete file {} (total delete files: {})",
                         threadId, filePath, deleteFiles.size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to register positional delete file %s: %s",
                         threadId, filePath, e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

     private PartitionData partitionDataFromTypedValues(PartitionSpec spec,
               List<ArrowPayload.FileMetadata.PartitionValue> partitionValues) {
          PartitionData partitionData = new PartitionData(spec.partitionType());
          if (partitionValues == null || partitionValues.isEmpty()) {
               return partitionData;
          }

          for (int i = 0; i < partitionValues.size() && i < spec.fields().size(); i++) {
               ArrowPayload.FileMetadata.PartitionValue protoValue = partitionValues.get(i);
               Object value = switch (protoValue.getValueCase()) {
                    case INT_VALUE -> protoValue.getIntValue();
                    case LONG_VALUE -> protoValue.getLongValue();
                    case FLOAT_VALUE -> protoValue.getFloatValue();
                    case DOUBLE_VALUE -> protoValue.getDoubleValue();
                    case STRING_VALUE -> protoValue.getStringValue();
                    case BOOL_VALUE -> protoValue.getBoolValue();
                    case VALUE_NOT_SET -> null;
               };
               partitionData.set(i, value);
          }

          return partitionData;
     }
}
