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
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.ParquetUtil;
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

  ArrayList<DataFile> dataFiles = new ArrayList<>();
  ArrayList<DeleteFile> deleteFiles = new ArrayList<>();

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

    try {
      completeWriter();

      // Calculate total files across all WriteResults
      int totalDataFiles = dataFiles.size();
      int totalDeleteFiles = deleteFiles.size();

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
        boolean hasDeleteFiles = totalDeleteFiles > 0;

        if (hasDeleteFiles) {
          RowDelta rowDelta = table.newRowDelta();
          // Add all data and delete files from all WriteResults
          dataFiles.forEach(rowDelta::addRows);
          deleteFiles.forEach(rowDelta::addDeletes);
          rowDelta.commit();
        } else {
          AppendFiles appendFiles = table.newAppend();
          // Add all data files from all WriteResults
          dataFiles.forEach(appendFiles::appendFile);
          appendFiles.commit();
        }

        LOGGER.info("Successfully committed {} data files and {} delete files for thread: {}",
            totalDataFiles, totalDeleteFiles, threadId);
      } catch (Exception e) {
        String errorMsg = String.format("Failed to commit data for thread %s: %s", threadId, e.getMessage());
        LOGGER.error(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to commit", e);
    }
  }

  public void completeWriter() {
    try {
      if (writer == null) {
        LOGGER.warn("no writer to complete");
        return;
      }
      WriteResult writerResult = writer.complete();
      deleteFiles.addAll(Arrays.asList(writerResult.deleteFiles()));
      dataFiles.addAll(Arrays.asList(writerResult.dataFiles()));
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

  
   public void accumulateDataFiles(String threadId, Table table, List<String> filePaths) {
          if (table == null) {
               LOGGER.warn("No table found for thread: {}", threadId);
               return;
          }

          try {
               FileIO fileIO = table.io();
               MetricsConfig metricsConfig = MetricsConfig.forTable(table);

               for (String filePath : filePaths) {
                    try {
                         InputFile inputFile = fileIO.newInputFile(filePath);
                         Metrics metrics = ParquetUtil.fileMetrics(inputFile, metricsConfig);

                         // Extract partition path from file path to scope data file to partition
                         String partitionPath = extractPartitionPath(table, filePath);

                         DataFiles.Builder dataFileBuilder = DataFiles.builder(table.spec())
                                   .withPath(filePath)
                                   .withFormat(FileFormat.PARQUET)
                                   .withFileSizeInBytes(inputFile.getLength())
                                   .withMetrics(metrics);

                         if (partitionPath != null && !partitionPath.isEmpty()) {
                              org.apache.iceberg.PartitionData partitionData = createPartitionData(table.spec(),
                              partitionPath);
                              dataFileBuilder.withPartition(partitionData);
                              LOGGER.debug("Thread {}: data file scoped to partition: {}", threadId, partitionPath);
                         } else {
                              LOGGER.debug("Thread {}: data file created as global (unpartitioned)", threadId);
                         }

                         DataFile dataFile = dataFileBuilder.build();
                         dataFiles.add(dataFile);
                         LOGGER.debug("Thread {}: accumulated data file {} (total: {})", threadId, filePath,
                                   dataFiles.size());
                    } catch (Exception e) {
                         LOGGER.error("Thread {}: failed to accumulate file {}: {}", threadId, filePath, e.getMessage(),
                                   e);
                         throw e;
                    }
               }

               LOGGER.info("Thread {}: accumulated {} data files (total: {})", threadId, filePaths.size(),
                         dataFiles.size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to accumulate data files: %s", threadId,
                         e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

    public void accumulateDeleteFiles(String threadId, Table table, List<String> filePaths, int equalityFieldId,
               long recordCount) {
          if (table == null) {
               LOGGER.warn("No table found for thread: {}", threadId);
               return;
          }

          try {
               FileIO fileIO = table.io();

               for (String filePath : filePaths) {
                    try {
                         InputFile inputFile = fileIO.newInputFile(filePath);
                         long fileSize = inputFile.getLength();

                         // Extract partition path from file path to scope delete file to partition
                         String partitionPath = extractPartitionPath(table, filePath);

                         FileMetadata.Builder deleteFileBuilder = FileMetadata.deleteFileBuilder(table.spec())
                                   .ofEqualityDeletes(equalityFieldId)
                                   .withPath(filePath)
                                   .withFormat(FileFormat.PARQUET)
                                   .withFileSizeInBytes(fileSize)
                                   .withRecordCount(recordCount);

                         if (partitionPath != null && !partitionPath.isEmpty()) {
                              // Convert partition path to PartitionData to handle null values properly
                              // This allows "col=null" in paths instead of "col=__HIVE_DEFAULT_PARTITION__"
                              org.apache.iceberg.PartitionData partitionData = createPartitionData(table.spec(),
                                        partitionPath);
                              deleteFileBuilder.withPartition(partitionData);
                              LOGGER.debug("Thread {}: delete file scoped to partition: {}", threadId, partitionPath);
                         } else {
                              LOGGER.debug("Thread {}: delete file created as global (unpartitioned)", threadId);
                         }

                         DeleteFile deleteFile = deleteFileBuilder.build();
                         deleteFiles.add(deleteFile);
                         LOGGER.debug("Thread {}: accumulated delete file {} with equality field ID {} (total: {})",
                                   threadId, filePath, equalityFieldId, deleteFiles.size());
                    } catch (Exception e) {
                         LOGGER.error("Thread {}: failed to accumulate delete file {}: {}", threadId, filePath,
                                   e.getMessage(), e);
                         throw e;
                    }
               }

               LOGGER.info("Thread {}: accumulated {} delete files (total: {})", threadId, filePaths.size(),
                         deleteFiles.size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to accumulate delete files: %s", threadId,
                         e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

    private String extractPartitionPath(Table table, String filePath) {
          if (table.spec().isUnpartitioned()) {
               return "";
          }

          try {
               String tableLocation = table.location();
               if (filePath.startsWith(tableLocation)) {
                    String relativePath = filePath.substring(tableLocation.length());
                    if (relativePath.startsWith("/")) {
                         relativePath = relativePath.substring(1);
                    }

                    // Skip the 'data/' directory prefix if present
                    if (relativePath.startsWith("data/")) {
                         relativePath = relativePath.substring(5); // Remove "data/"
                    }

                    // Get the directory part (everything before the last /)
                    int lastSlash = relativePath.lastIndexOf('/');
                    if (lastSlash > 0) {
                         String partitionPath = relativePath.substring(0, lastSlash);
                         return partitionPath;
                    }
               }
          } catch (Exception e) {
               LOGGER.warn("Failed to extract partition path from {}: {}", filePath, e.getMessage());
          }

          return "";
     }

    private List<String> parsePartitionValues(String partitionPath) {
          List<String> values = new ArrayList<>();

          if (partitionPath == null || partitionPath.isEmpty()) {
               return values;
          }

          // Split by / to get individual partition fields
          String[] parts = partitionPath.split("/");
          for (String part : parts) {
               // Each part is like "col=value"
               int equalsIndex = part.indexOf('=');
               if (equalsIndex > 0 && equalsIndex < part.length() - 1) {
                    String value = part.substring(equalsIndex + 1);
                    try {
                         value = java.net.URLDecoder.decode(value, "UTF-8");
                    } catch (java.io.UnsupportedEncodingException e) {
                         LOGGER.warn("Failed to URL-decode partition value: {}", value);
                    }

                    values.add("null".equals(value) ? null : value);
               }
          }

          return values;
     }

     private org.apache.iceberg.PartitionData createPartitionData(org.apache.iceberg.PartitionSpec spec, String partitionPath) {
          PartitionData partitionData = new org.apache.iceberg.PartitionData(spec.partitionType());

          if (partitionPath == null || partitionPath.isEmpty()) {
               return partitionData;
          }

          List<String> values = parsePartitionValues(partitionPath);

          // Set each value in the PartitionData
          for (int i = 0; i < values.size() && i < spec.fields().size(); i++) {
               String stringValue = values.get(i);
               org.apache.iceberg.types.Type fieldType = partitionData.getType(i);

               // Convert string value to proper type, handling nulls
               Object typedValue = stringValue == null ? null : convertPartitionValue(fieldType, stringValue, spec.fields().get(i));
               partitionData.set(i, typedValue);
          }

          return partitionData;
     }

     private Object convertPartitionValue(org.apache.iceberg.types.Type fieldType, String stringValue, PartitionField partitionField) {
          String transformName = partitionField.transform().toString().toLowerCase();

          try {
               return org.apache.iceberg.types.Conversions.fromPartitionString(fieldType, stringValue);
          } catch (NumberFormatException | UnsupportedOperationException e) {

               try {
                    if (transformName.equals("identity")
                              && fieldType.typeId() == org.apache.iceberg.types.Type.TypeID.TIMESTAMP) {

                         java.time.OffsetDateTime offsetDateTime = java.time.OffsetDateTime.parse(stringValue);
                         java.time.Instant instant = offsetDateTime.toInstant();
                         return instant.toEpochMilli() * 1000; // Convert milliseconds to microseconds
                    } else if (transformName.contains("year") && stringValue.matches("\\d{4}")) {
                         // Year format: "2025" -> return as-is (already an integer)
                         return Integer.parseInt(stringValue);
                    } else if (transformName.contains("month") && stringValue.matches("\\d{4}-\\d{2}")) {
                         // Month format: "2025-12" -> convert to months since epoch
                         String[] parts = stringValue.split("-");
                         int year = Integer.parseInt(parts[0]);
                         int month = Integer.parseInt(parts[1]);
                         int monthsSinceEpoch = (year - 1970) * 12 + (month - 1);
                         return monthsSinceEpoch;
                    } else if (transformName.contains("day") && stringValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                         // Day format: "2025-12-08" -> convert to days since epoch
                         java.time.LocalDate date = java.time.LocalDate.parse(stringValue);
                         java.time.LocalDate epoch = java.time.LocalDate.of(1970, 1, 1);
                         return (int) java.time.temporal.ChronoUnit.DAYS.between(epoch, date);
                    } else if (transformName.contains("hour") && stringValue.matches("\\d{4}-\\d{2}-\\d{2}-\\d{2}")) {
                         // Hour format: "2025-12-08-02" -> convert to hours since epoch
                         String[] parts = stringValue.split("-");
                         java.time.LocalDateTime dateTime = java.time.LocalDateTime.of(
                                   Integer.parseInt(parts[0]), // year
                                   Integer.parseInt(parts[1]), // month
                                   Integer.parseInt(parts[2]), // day
                                   Integer.parseInt(parts[3]), // hour
                                   0 // minute
                         );
                         java.time.LocalDateTime epoch = java.time.LocalDateTime.of(1970, 1, 1, 0, 0);
                         return (int) java.time.temporal.ChronoUnit.HOURS.between(epoch, dateTime);
                    }
               } catch (Exception parseError) {
                    LOGGER.warn("Failed to parse human-readable date format '{}': {}", stringValue,
                              parseError.getMessage());
               }

               throw new RuntimeException(
                         "Cannot parse partition value '" + stringValue + "' for transform " + transformName, e);
          }
     }
}
