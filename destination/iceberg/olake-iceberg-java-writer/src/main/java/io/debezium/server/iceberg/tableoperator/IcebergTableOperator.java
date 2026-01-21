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
import org.apache.iceberg.PartitionField;
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
  
          if (eqDeletes != null) {
            eqDeletes.forEach(delta::addDeletes);
          }
          if (dataFiles != null) {
            dataFiles.forEach(delta::addRows);
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

     public void accumulateDataFiles(String threadId, Table table, String filePath,
               List<String> partitionValues) {
          if (table == null) {
               LOGGER.warn("No table found for thread: {}", threadId);
               return;
          }

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
                    org.apache.iceberg.PartitionData partitionData = createPartitionDataFromValues(
                              table.spec(),
                              partitionValues);
                    dataFileBuilder.withPartition(partitionData);
                    LOGGER.debug("Thread {}: data file scoped to partition with {} values", threadId,
                              partitionValues.size());
               } else {
                    LOGGER.debug("Thread {}: data file created as global (unpartitioned)", threadId);
               }

               DataFile dataFile = dataFileBuilder.build();
               filesToCommit.get(filesToCommit.size() - 1).second().add(dataFile);
               LOGGER.info("Thread {}: accumulated data file {} (total: {})", threadId, filePath,
                         filesToCommit.get(filesToCommit.size() - 1).second().size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to accumulate data file %s: %s", threadId,
                         filePath, e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

     public void accumulateDeleteFiles(String threadId, Table table, String filePath, int equalityFieldId,
               long recordCount, List<String> partitionValues) {
          if (table == null) {
               LOGGER.warn("No table found for thread: {}", threadId);
               return;
          }

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
                    org.apache.iceberg.PartitionData partitionData = createPartitionDataFromValues(
                              table.spec(),
                              partitionValues);
                    deleteFileBuilder.withPartition(partitionData);
                    LOGGER.debug("Thread {}: delete file scoped to partition with {} values", threadId,
                              partitionValues.size());
               } else {
                    LOGGER.debug("Thread {}: delete file scoped to global (unpartitioned)", threadId);
               }

               DeleteFile deleteFile = deleteFileBuilder.build();
               filesToCommit.get(filesToCommit.size() - 1).first().add(deleteFile);
               LOGGER.info("Thread {}: accumulated delete file {} with equality field ID {} (total: {})",
                         threadId, filePath, equalityFieldId, filesToCommit.get(filesToCommit.size() - 1).first().size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to accumulate delete file %s: %s", threadId,
                         filePath, e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

     private org.apache.iceberg.PartitionData createPartitionDataFromValues(org.apache.iceberg.PartitionSpec spec,
               List<String> partitionValues) {
          PartitionData partitionData = new org.apache.iceberg.PartitionData(spec.partitionType());
          if (partitionValues == null || partitionValues.isEmpty()) {
               return partitionData;
          }

          // Set each value in the PartitionData
          for (int i = 0; i < partitionValues.size() && i < spec.fields().size(); i++) {
               String stringValue = partitionValues.get(i);
               org.apache.iceberg.types.Type fieldType = partitionData.getType(i);
               PartitionField partitionField = spec.fields().get(i);
               String transformName = partitionField.transform().toString().toLowerCase();

               // Convert string value to proper type, handling nulls
               Object typedValue = null;
               if (stringValue != null && !"null".equals(stringValue)) {
                    try {
                         typedValue = org.apache.iceberg.types.Conversions.fromPartitionString(fieldType, stringValue);
                    } catch (NumberFormatException | UnsupportedOperationException e) {
                         try {
                              if (transformName.equals("identity")
                                        && fieldType.typeId() == org.apache.iceberg.types.Type.TypeID.TIMESTAMP) {
                                   java.time.OffsetDateTime offsetDateTime = java.time.OffsetDateTime
                                             .parse(stringValue);
                                   java.time.Instant instant = offsetDateTime.toInstant();
                                   typedValue = instant.toEpochMilli() * 1000;
                              } else if (transformName.contains("year") && stringValue.matches("\\d{4}")) {
                                   typedValue = Integer.parseInt(stringValue);
                              } else if (transformName.contains("month") && stringValue.matches("\\d{4}-\\d{2}")) {
                                   String[] parts = stringValue.split("-");
                                   int year = Integer.parseInt(parts[0]);
                                   int month = Integer.parseInt(parts[1]);
                                   typedValue = (year - 1970) * 12 + (month - 1);
                              } else if (transformName.contains("day") && stringValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                                   java.time.LocalDate date = java.time.LocalDate.parse(stringValue);
                                   java.time.LocalDate epoch = java.time.LocalDate.of(1970, 1, 1);
                                   typedValue = (int) java.time.temporal.ChronoUnit.DAYS.between(epoch, date);
                              } else if (transformName.contains("hour")
                                        && stringValue.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}")) {
                                   String[] parts = stringValue.split("-");
                                   java.time.LocalDateTime dateTime = java.time.LocalDateTime.of(
                                             Integer.parseInt(parts[0]),
                                             Integer.parseInt(parts[1]),
                                             Integer.parseInt(parts[2]),
                                             Integer.parseInt(parts[3]),
                                             0);
                                   java.time.LocalDateTime epoch = java.time.LocalDateTime.of(1970, 1, 1, 0, 0);
                                   typedValue = (int) java.time.temporal.ChronoUnit.HOURS.between(epoch, dateTime);
                              } else {
                                   throw new RuntimeException(
                                             "Cannot parse partition value '" + stringValue + "' for transform "
                                                       + transformName,
                                             e);
                              }
                         } catch (Exception parseError) {
                              LOGGER.warn("Failed to parse partition value '{}': {}", stringValue,
                                        parseError.getMessage());
                              throw new RuntimeException(
                                        "Cannot parse partition value '" + stringValue + "' for transform "
                                                  + transformName,
                                        parseError);
                         }
                    }
               }
               partitionData.set(i, typedValue);
          }

          return partitionData;
     }
}
