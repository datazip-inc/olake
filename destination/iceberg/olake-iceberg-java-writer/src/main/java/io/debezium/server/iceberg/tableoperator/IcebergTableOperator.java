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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.util.Pair;
import io.debezium.server.iceberg.tableIndex.TableIndexScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload;

/**
 * Wrapper to perform operations on iceberg tables
 *
 * @author Rafael Acevedo
 */
public class IcebergTableOperator {

  IcebergTableWriterFactory writerFactory2;

  TaskWriter<Record> writer;

  ArrayList<Pair<ArrayList<DeleteFile>, ArrayList<DataFile>>> filesToCommit = new ArrayList<>();

  /**
   * Delete files a deletion-vector writer's output supersedes. Iceberg allows one
   * vector per data file, so a commit that touches a file which already has a vector
   * must retire the old one in the SAME commit or the table ends up with two. Always
   * empty in eq/pos mode - only DeleteMode.DELETION_VECTOR's writer ever populates it,
   * via WriteResult.rewrittenDeleteFiles().
   */
  ArrayList<DeleteFile> rewrittenDeleteFiles = new ArrayList<>();

  public IcebergTableOperator(boolean upsert_records) {
    this(upsert_records, DeleteMode.EQUALITY);
  }

  public IcebergTableOperator(boolean upsert_records, DeleteMode deleteMode) {
    writerFactory2 = new IcebergTableWriterFactory();
    writerFactory2.keepDeletes = true;
    writerFactory2.upsert = upsert_records;
    writerFactory2.deleteMode = deleteMode;
    this.allowFieldAddition = true;
    this.upsert = upsert_records;
    this.deleteMode = deleteMode;
    // Kept as a plain boolean because it drives write-run tracking in
    // addToTablePerSchema below, which is identical for pos and dv - both need the
    // caller's table index updated with where every row landed. Only the writer
    // factory (which encoding to use for a superseded position) branches on the
    // full three-way mode.
    this.usePositionalDeletes = deleteMode.addressesPositions();
    this.cdcOpField = "_op_type";
    this.cdcSourceTsMsField = "_cdc_timestamp";
  }

  static final ImmutableMap<Operation, Integer> CDC_OPERATION_PRIORITY = ImmutableMap.of(
      Operation.INSERT, 1, Operation.CREATE, 1,
      Operation.READ, 2, Operation.UPDATE, 3, Operation.DELETE, 4);
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableOperator.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  private static final String STATE_KEY_2PC = "olake_2pc";
  private static final String STATE_FIELD_LATEST_THREAD_ID = "id";
  private static final String STATE_FIELD_FULL_REFRESH_COMMITTED_IDS = "full_refresh_committed_ids";
  private static final String STATE_FIELD_DEDUP_INSERTS = "dedup_inserts";


  // Fields are plain (no @ConfigProperty) because each operator instance lives
  // inside a shared JVM and may have different upsert/identifier flags. The
  // OlakeRowsIngester/OlakeArrowIngester construct each operator explicitly.
  String cdcSourceTsMsField;
  String cdcOpField;
  boolean allowFieldAddition;
  boolean upsert;
  boolean usePositionalDeletes;
  DeleteMode deleteMode = DeleteMode.EQUALITY;
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
  public void applyFieldAddition(Table icebergTable, Schema newSchema, boolean createIdentifierFields) {
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
   * @param baseSnapshotId when non-null, table tip after refresh must equal this
   *                       snapshot (the caller's row-index checkpoint); otherwise
   *                       the commit is refused so positional deletes built from a
   *                       stale index cannot be published
   * @throws RuntimeException if commit fails
   */
  public long commitThread(String threadId, String payload, Table table, Long baseSnapshotId) {
    if (table == null) {
      LOGGER.warn("No table found for thread: {}", threadId);
      return 0;
    }
  
    completeWriter();
  
    if (filesToCommit.isEmpty()) {
      LOGGER.info("No files to commit for thread: {}", threadId);
      rewrittenDeleteFiles.clear();
      if (table.currentSnapshot() != null) {
        return 0L;
      }
      return 0;
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
      rewrittenDeleteFiles.clear();
      return 0L;
    }
  
    try {
      Transaction transaction = table.newTransaction();

      // 1. Stage Property Update - mark thread as committed
      UpdateProperties updateProperties = transaction.updateProperties();
      
      updateJsonState(table, updateProperties, threadId, payload);
      
      updateProperties.commit();

      // 2. Stage Data Commit
      if (!hasAnyDeletes) {
        AppendFiles appendFiles = transaction.newAppend();
        
        for (Pair<ArrayList<DeleteFile>, ArrayList<DataFile>> unit : filesToCommit) {
          ArrayList<DataFile> dataFiles = unit.second();
          if (dataFiles == null || dataFiles.isEmpty()) {
            continue;
          }
          for (DataFile df : dataFiles) {
            appendFiles.appendFile(df);
          }
        }
        
        appendFiles.commit();
      } else {
        // RowDelta path (has delete files)
        RowDelta rowDelta = transaction.newRowDelta();
        
        if (baseSnapshotId != null) {
          rowDelta.validateFromSnapshot(baseSnapshotId);
          rowDelta.validateDeletedFiles();
          // Guards the PreviousDeleteLoader staleness window (deletion-vector mode
          // only reads existing deletes once, at session start): if a concurrent
          // writer added a delete to a data file we also reference since baseSnapshotId,
          // our merged vector would silently drop their deletion. Fail the commit
          // instead of publishing a vector that undoes someone else's delete.
          rowDelta.validateNoConflictingDeleteFiles();

          Set<CharSequence> referencedDataFiles = new HashSet<>();
          for (Pair<ArrayList<DeleteFile>, ArrayList<DataFile>> unit : filesToCommit) {
            ArrayList<DeleteFile> deleteFiles = unit.first();
            if (deleteFiles != null) {
              for (DeleteFile deleteFile : deleteFiles) {
                if (deleteFile.content() != FileContent.POSITION_DELETES) {
                  continue;
                }
                // A deletion vector (and any positional delete file scoped to a single
                // data file) carries the file it references directly - no need to open
                // it, and a vector cannot be opened as Parquet if we tried.
                String single = deleteFile.referencedDataFile();
                if (single != null) {
                  referencedDataFiles.add(single);
                  continue;
                }
                // PARTITION-granularity Parquet positional delete files can span every
                // data file in a partition, so referencedDataFile() is null; read the
                // file to enumerate all of them.
                try (CloseableIterable<Object> rows = TableIndexScanner.openParquet(table, deleteFile.location(), DeleteSchemaUtil.pathPosSchema())) {
                  for (Object row : rows) {
                    Object filePath = TableIndexScanner.getFieldValue(row, "file_path");
                    if (filePath != null) {
                      referencedDataFiles.add(filePath.toString());
                    }
                  }
                } catch (Exception e) {
                  LOGGER.warn("Failed to read referenced data files from positional delete file {}", deleteFile.location(), e);
                }
              }
            }
          }

          LOGGER.info("Referenced data files: {}", referencedDataFiles);
          if (!referencedDataFiles.isEmpty()) {
            rowDelta.validateDataFilesExist(referencedDataFiles);
          }
        }

        for (Pair<ArrayList<DeleteFile>, ArrayList<DataFile>> unit : filesToCommit) {
          ArrayList<DeleteFile> deleteFiles = unit.first();
          ArrayList<DataFile> dataFiles = unit.second();

          if (dataFiles != null && !dataFiles.isEmpty()) {
            dataFiles.forEach(rowDelta::addRows);
          }

          if (deleteFiles != null && !deleteFiles.isEmpty()) {
            deleteFiles.forEach(rowDelta::addDeletes);
          }
        }

        // Deletion-vector replace semantics: a superseded vector must leave the table
        // in the SAME commit the new one arrives in, or the table ends up with two
        // vectors for one data file. Always empty outside DELETION_VECTOR mode.
        rewrittenDeleteFiles.forEach(rowDelta::removeDeletes);

        rowDelta.commit();
      }

      transaction.commitTransaction();

      // take transaction snapshot id as commit id
      Snapshot staged = transaction.table().currentSnapshot();
      if (staged == null) {
        throw new IllegalStateException(
            "transaction for thread " + threadId + " staged no snapshot before catalog commit");
      }

      LOGGER.info("Successfully committed {} data files and {} delete files for thread: {} snapshot: {}",
          totalDataFiles, totalDeleteFiles, threadId,  staged.snapshotId());
  
      filesToCommit.clear();
      rewrittenDeleteFiles.clear();
      LOGGER.info("Staged snapshot id: {}, base snapshot id: {}, parent snapshot id: {}", staged.snapshotId(), baseSnapshotId, staged.parentId());
      // check commit parent snapshot if it is not equal to base, then return 0 so that indexer will not save latest snapshot
      if  (staged.parentId() != null && !Objects.equals(baseSnapshotId, staged.parentId())) {
        return 0L;
      }

      return  staged.snapshotId();
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
      // Only ever non-empty for a deletion-vector writer: the vectors it just wrote
      // supersede ones already on the table, and those must be retired at commit.
      rewrittenDeleteFiles.addAll(Arrays.asList(writerResult.rewrittenDeleteFiles()));
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
  private void addRange(Map<String, io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap.Builder> positionMap,
                        String path, int startIdx, long startPos, int count) {
    if (positionMap != null && path != null && count > 0) {
      positionMap.computeIfAbsent(path, p -> io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap.newBuilder().setFilePath(p))
          .addRanges(io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap.Range.newBuilder()
              .setBatchStartIdx(startIdx)
              .setStartPosition(startPos)
              .setCount(count));
    }
  }

  private List<io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap> buildPositionMaps(
      Map<String, io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap.Builder> positionMap) {
    List<io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap> result = new ArrayList<>();
    if (positionMap != null) {
      for (io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap.Builder b : positionMap.values()) {
        result.add(b.build());
      }
    }
    return result;
  }

  public List<io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap> addToTablePerSchema(String threadID, Table icebergTable, List<RecordWrapper> events) {
    if (writer == null) {
      writer = writerFactory2.create(icebergTable);
    }
    Map<String, io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap.Builder> filePositionMap = usePositionalDeletes ? new LinkedHashMap<>() : null;
    try {
      io.grpc.Context grpcContext = io.grpc.Context.current();
      
      PositionTrackableWriter trackable = usePositionalDeletes ? (PositionTrackableWriter) writer : null;
      String currentPath = null;
      int runStartIdx = -1;
      long runStartPos = -1;
      int runCount = 0;

      for (int i = 0; i < events.size(); i++) {
        RecordWrapper record = events.get(i);
        // Cooperative cancel: check on every record to stop processing early if client disconnects
        if (grpcContext.isCancelled()) {
          LOGGER.warn("Thread {}: cancellation observed mid-batch, discarding partial writer", threadID);
          return null;
        }
        try {
          // Normalise _op_type "i" → "c" before routing to any writer.
          if ("i".equals(record.getField("_op_type"))) {
            record.setField("_op_type", "c");
          }

          if (usePositionalDeletes && trackable != null) {
            CharSequence pathCs = trackable.currentPath(record);
            long pos = trackable.currentRows(record);
            String path = pathCs != null ? pathCs.toString() : null;

            if (currentPath == null || !currentPath.equals(path) || pos != runStartPos + runCount) {
              addRange(filePositionMap, currentPath, runStartIdx, runStartPos, runCount);
              currentPath = path;
              runStartIdx = i;
              runStartPos = pos;
              runCount = 1;
            } else {
              runCount++;
            }
          }

          writer.write(record);
        } catch (Exception ex) {
          LOGGER.error("Failed to write data: {}, exception: {}", record, ex);
          throw ex;
        }
      }
      
      if (usePositionalDeletes) {
        addRange(filePositionMap, currentPath, runStartIdx, runStartPos, runCount);
      }

      if (trackable != null) {
        // Every record of the batch is written and its runs are about to be returned,
        // so the writer's per-batch state - the supersede map - can be released.
        trackable.batchCompleted();
      }
      
      List<io.debezium.server.iceberg.rpc.RecordIngest.FilePositionMap> filePositionMaps = buildPositionMaps(filePositionMap);

      LOGGER.info("Successfully wrote {} events for thread: {} across {} files", events.size(), threadID, filePositionMaps.size());
      return filePositionMaps;

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
               if (filesToCommit.size() > 0) {
                    filesToCommit.get(0).first().add(deleteFile);
               } else {
                    filesToCommit.add(Pair.of(new ArrayList<>(Arrays.asList(deleteFile)), new ArrayList<DataFile>()));
               }
               LOGGER.info("Thread {}: accumulated positional delete file {} (total: {})",
                         threadId, filePath, filesToCommit.get(0).first().size());
          } catch (Exception e) {
               String errorMsg = String.format("Thread %s: failed to register positional delete file %s: %s",
                         threadId, filePath, e.getMessage());
               LOGGER.error(errorMsg, e);
               throw new RuntimeException(e);
          }
     }

     /**
      * Accumulates delete files that are already built, rather than reconstructing their
      * metadata from a path the way the register* methods do.
      *
      * <p>Used by the arrow deletion-vector path, where {@code BaseDVFileWriter} hands
      * back finished {@link DeleteFile}s (content offsets, referenced data file and
      * cardinality all set) that could not be rebuilt from a path alone.
      *
      * @param rewritten vectors the new ones supersede. Removed in the same commit that
      *        adds their replacements, since a data file may carry only one vector.
      */
     public void registerBuiltDeleteFiles(String threadId, List<DeleteFile> deleteFiles,
               List<DeleteFile> rewritten) {
          if (deleteFiles.isEmpty() && rewritten.isEmpty()) {
               return;
          }

          if (filesToCommit.isEmpty()) {
               filesToCommit.add(Pair.of(new ArrayList<DeleteFile>(), new ArrayList<DataFile>()));
          }
          filesToCommit.get(0).first().addAll(deleteFiles);
          rewrittenDeleteFiles.addAll(rewritten);

          LOGGER.info("Thread {}: accumulated {} deletion vector(s), superseding {} existing delete file(s)",
                    threadId, deleteFiles.size(), rewritten.size());
     }

     /**
      * Public because the arrow deletion-vector path needs the partition of the data
      * files this same commit is adding: they are not in table metadata yet, so
      * {@code DeletionVectorConverter} cannot resolve them by scanning.
      */
     public PartitionData partitionDataFromTypedValues(PartitionSpec spec,
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

  private void updateJsonState(Table table, UpdateProperties updateProperties, String threadId, String payload) {
      try {
          String currentValue = table.properties().get(STATE_KEY_2PC);
          ObjectNode rootNode;
          if (currentValue != null) {
              rootNode = (ObjectNode) mapper.readTree(currentValue);
          } else {
              rootNode = mapper.createObjectNode();
          }

          if (payload != null && !payload.isEmpty()) {
              JsonNode payloadNode = mapper.readTree(payload);
              rootNode.put(STATE_FIELD_LATEST_THREAD_ID, threadId);
              if (payloadNode.isObject()) {
                  // One-level merge payload into root node
                  mergePayloadIntoRoot(rootNode, payloadNode);
              }
          } else {
              // No payload => backfill/snapshot style: append threadId to full_refresh_committed_ids
              // and mark that the first CDC sync must use equality deletes (overlap window open).
              com.fasterxml.jackson.databind.node.ArrayNode committedIds;
              if (rootNode.has(STATE_FIELD_FULL_REFRESH_COMMITTED_IDS) && rootNode.get(STATE_FIELD_FULL_REFRESH_COMMITTED_IDS).isArray()) {
                  committedIds = (com.fasterxml.jackson.databind.node.ArrayNode) rootNode.get(STATE_FIELD_FULL_REFRESH_COMMITTED_IDS);
              } else {
                  committedIds = rootNode.putArray(STATE_FIELD_FULL_REFRESH_COMMITTED_IDS);
              }
              committedIds.add(threadId);
              rootNode.put(STATE_FIELD_DEDUP_INSERTS, true);
          }

          updateProperties.set(STATE_KEY_2PC, mapper.writeValueAsString(rootNode));
      } catch (JsonProcessingException e) {
          LOGGER.error("Failed to update JSON state for key: " + STATE_KEY_2PC, e);
          throw new RuntimeException("Failed to update JSON state", e);
      }
  }

  // Some drivers (e.g. Kafka) can have multiple writers updating metadata for the same stream.
  // Perform a one-level merge to preserve fields written by other writers.
  private void mergePayloadIntoRoot(ObjectNode rootNode, JsonNode payloadNode) {
      payloadNode.fields().forEachRemaining(entry -> {
          String incomingStateKey = entry.getKey();
          ObjectNode incomingStateValue = parseJSONObject(entry.getValue());
          ObjectNode storedStateValue = parseJSONObject(rootNode.get(incomingStateKey));

          if (incomingStateValue != null && storedStateValue != null) {
              storedStateValue.setAll(incomingStateValue);
              rootNode.put(incomingStateKey, storedStateValue.toString());
          } else {
              rootNode.set(incomingStateKey, entry.getValue());
          }
      });
  }

  private ObjectNode parseJSONObject(JsonNode node) {
      if (node == null || !node.isTextual()) return null;
      try {
          JsonNode parsedNode = mapper.readTree(node.asText());
          return parsedNode.isObject() ? (ObjectNode) parsedNode : null;
      } catch (JsonProcessingException ignored) {
          return null;
      }
  }

  public String getCommitState(Table table) {      
      String propertyValue = null;
      if (table != null) {
          propertyValue = table.properties().get(STATE_KEY_2PC);
      }
      return propertyValue;
  }
}
