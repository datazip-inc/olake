package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.StructLikeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload;
import io.debezium.server.iceberg.tableIndex.PreviousDeleteLoader;

/**
 * Turns the positions the arrow writer streams over gRPC into Iceberg v3 deletion
 * vectors.
 *
 * <p>Format version 3 rejects Parquet positional delete files, so under
 * {@link DeleteMode#DELETION_VECTOR} the caller writes no delete file at all and sends
 * {@code (data file, positions)} instead - the form the data already had on its side.
 * Nothing is written to storage until {@link #complete()}: positions accumulate in
 * roaring bitmaps inside {@code BaseDVFileWriter}, so a sync's whole delete set stays
 * small in memory regardless of how many batches arrive.
 *
 * <p>Partitions are read against {@code table.spec()} throughout, as in
 * {@code EqualityDeleteMigrator}: a table OLake owns carries a single spec, since a
 * partition change is a stream delta and such streams are dropped and rebuilt rather
 * than evolved in place. Tables repartitioned outside OLake are not supported.
 *
 * <p>One instance per session, created on the first batch. Not thread-safe; a session
 * is driven by a single caller thread, like every other writer-scoped object here.
 *
 * <p>A sync that never commits simply drops this object: nothing has reached storage
 * yet, because {@code BaseDVFileWriter} creates its Puffin file inside {@code close()}.
 * Closing an abandoned writer would therefore produce the orphan blob it looks like it
 * is preventing.
 */
public final class ArrowDeletionVectorWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowDeletionVectorWriter.class);

  private final Table table;
  private final DVFileWriter writer;
  /**
   * Partitions of data files this session wrote, sent by the caller with each batch.
   *
   * <p>DIVERGES FROM THE ROWS PATH, deliberately, and the two should be reconciled.
   * {@code PositionalDeltaWriter} routes a delete against a file from an EARLIER sync to
   * the partition of the NEW record instead of the old file's own - see the NOTE on its
   * {@code write()}. It has no choice: {@code RowLocation} carries only path and
   * position, so nothing upstream knows the old partition. Here the caller ships the
   * partition for its own files and the rest come from {@link #scanPartitions()}, which
   * is correct but costs a {@code planFiles()} per commit.
   *
   * <p>Pick one: teach the rows path the same resolution (fixes a real bug for {@code pos},
   * where deletes are partition-matched), or drop this and match the rows path (cheaper,
   * and vectors are matched by PATH not partition - {@code DeleteFileIndex.dvByPath} - so
   * delete correctness survives; manifest metadata and partition pruning would not).
   */
  private final Map<String, StructLike> knownPartitions = new HashMap<>();
  /** Partitions resolved from table metadata, cached across batches. */
  private final Map<String, StructLike> scannedPartitions = new HashMap<>();
  private boolean scanned;
  private long positions;

  public ArrowDeletionVectorWriter(Table table) {
    this.table = table;
    // Deletion vectors are Puffin, not the table's data format. An OutputFileFactory
    // takes its extension from the format it was built with, so the session's own
    // factory would name these blobs ".parquet".
    OutputFileFactory dvFileFactory = IcebergUtil.getTableOutputFileFactory(table, FileFormat.PUFFIN);
    // A vector REPLACES a data file's previous vector rather than adding to it, so it
    // must be seeded with what was already deleted or this commit resurrects those
    // rows. Never pass a null loader here.
    this.writer = new BaseDVFileWriter(dvFileFactory, new PreviousDeleteLoader(table));
  }

  /** Folds one batch of positions into the vectors being built. */
  public void add(ArrowPayload.DeletionVectorBatch batch, IcebergTableOperator operator) {
    PartitionSpec spec = table.spec();
    for (ArrowPayload.DeletionVectorBatch.Entry entry : batch.getEntriesList()) {
      String dataFilePath = entry.getDataFilePath();

      if (entry.getPartitionKnown()) {
        // Written by this session, so it is not in table metadata yet and no scan
        // could find it. The caller is the only source for its partition.
        knownPartitions.computeIfAbsent(dataFilePath,
            path -> operator.partitionDataFromTypedValues(spec, entry.getPartitionValuesList()));
      }

      // Resolved even when unpartitioned, so a delete against a file the table does
      // not hold is still rejected rather than silently dropped.
      StructLike resolved = partitionOf(dataFilePath);
      StructLike partition = spec.isUnpartitioned() ? null : resolved;
      for (long position : entry.getPositionsList()) {
        writer.delete(dataFilePath, position, spec, partition);
        positions++;
      }
    }
  }

  /**
   * Closes the Puffin file and reports what was written.
   *
   * @return the vectors, plus the vectors they supersede - the latter MUST be removed
   *         in the same commit that adds the former, since a data file may carry only
   *         one vector.
   */
  public DeleteWriteResult complete() throws IOException {
    writer.close();
    DeleteWriteResult result = writer.result();
    LOGGER.info("encoded {} positions of {} as {} deletion vector(s), superseding {} existing delete file(s)",
        positions, table.name(), result.deleteFiles().size(), result.rewrittenDeleteFiles().size());
    return result;
  }

  private StructLike partitionOf(String dataFilePath) {
    StructLike known = knownPartitions.get(dataFilePath);
    if (known != null) {
      return known;
    }

    if (!scanned) {
      scanPartitions();
    }

    StructLike resolved = scannedPartitions.get(dataFilePath);
    if (resolved == null) {
      // Every referenced file is either committed already or written by this session.
      // Neither means the delete points at a file that does not exist, which would
      // silently drop the delete if it were tolerated.
      throw new IllegalStateException(String.format(
          "cannot resolve the partition of data file %s; it is neither in %s nor among "
              + "the files this session has written",
          dataFilePath, table.name()));
    }
    return resolved;
  }

  /** Partition of every data file the table currently holds. Runs once per session. */
  private void scanPartitions() {
    scanned = true;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String path = task.file().location();
        if (scannedPartitions.containsKey(path)) {
          // planFiles can split one data file across several tasks.
          continue;
        }
        // The scan reuses its partition object between tasks, so it is copied before
        // being held.
        scannedPartitions.put(path, StructLikeUtil.copy(task.file().partition()));
      }
    } catch (IOException e) {
      throw new java.io.UncheckedIOException("failed to resolve data file partitions of " + table.name(), e);
    }
  }
}
