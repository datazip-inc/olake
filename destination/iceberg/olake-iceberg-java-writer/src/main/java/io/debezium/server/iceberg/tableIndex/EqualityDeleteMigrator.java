package io.debezium.server.iceberg.tableIndex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.tableoperator.DeleteMode;

/**
 * Replaces a table's equality delete files with positional delete files that
 * remove exactly the same rows.
 *
 * <p>Iceberg has no way to mark a delete file as orphaned, so the exchange is
 * done as a single {@link RewriteFiles} commit: the equality deletes leave the
 * table in the same operation that introduces their positional equivalents,
 * which means a reader never observes the rows as undeleted.
 *
 * <p>Which equality delete applies to which data file is not inferred here.
 * Iceberg resolves that when planning the scan, taking partition and sequence
 * numbers into account, and this class simply honours the pairing it reports.
 */
public final class EqualityDeleteMigrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(EqualityDeleteMigrator.class);

  private EqualityDeleteMigrator() {
  }

  /** Outcome of a migration. */
  public static final class Result {
    public final int rewrittenDeleteFiles;
    public final long positionalDeletesWritten;

    private Result(int rewrittenDeleteFiles, long positionalDeletesWritten) {
      this.rewrittenDeleteFiles = rewrittenDeleteFiles;
      this.positionalDeletesWritten = positionalDeletesWritten;
    }
  }

  /**
   * Whether the current snapshot still carries equality-delete records.
   *
   * <p>Prefers the snapshot summary ({@code total-equality-deletes}), which is
   * already in table metadata and does not need {@code planFiles()}. If that
   * field is missing, delete manifests are read instead — still cheaper than
   * planning a full table scan, which is what pairs deletes to data files.
   */
  public static boolean hasEqualityDeletes(Table table) throws IOException {
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return false;
    }

    String totalEqDeletes = current.summary().get(SnapshotSummary.TOTAL_EQ_DELETES_PROP);
    if (totalEqDeletes != null) {
      return Long.parseLong(totalEqDeletes) > 0L;
    }

    return false;
  }

  /** Rewrites the table's equality deletes into positional delete files. */
  public static Result migrate(Table table, String identifierField, OutputFileFactory fileFactory) throws Exception {
    return migrate(table, identifierField, fileFactory, DeleteMode.POSITION);
  }

  /**
   * Rewrites the table's equality deletes into {@code targetMode}'s representation
   * ({@link DeleteMode#POSITION} or {@link DeleteMode#DELETION_VECTOR}) in one atomic
   * commit, so a table switching off equality deletes lands on its target encoding
   * directly instead of eq -> pos -> dv.
   */
  public static Result migrate(Table table, String identifierField, OutputFileFactory fileFactory,
      DeleteMode targetMode) throws Exception {
    table.refresh();

    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return new Result( 0, 0L);
    }

    if (!hasEqualityDeletes(table)) {
      LOGGER.info("{} has no equality deletes to migrate", table.name());
      return new Result( 0, 0L);
    }

    Map<String, DataFileDeletes> affected = planAffectedDataFiles(table);
    if (affected.isEmpty()) {
      LOGGER.info("{} has no equality deletes to migrate", table.name());
      return new Result( 0, 0L);
    }

    Schema projection = TableIndexScanner.identifierProjection(table, identifierField);
    Map<String, Set<String>> keysByDeleteFile = new HashMap<>();
    Map<String, PartitionGroup> groups = new LinkedHashMap<>();
    Set<DeleteFile> replaced = new LinkedHashSet<>();
    long posConvCount = 0L;

    for (DataFileDeletes entry : affected.values()) {
      LOGGER.debug("data file affected {} with delete records: {}", entry.dataFile.location(), entry.equalityDeletes.toArray());
      Set<String> deletedKeys = new HashSet<>();
      for (DeleteFile delete : entry.equalityDeletes) {
        replaced.add(delete);
        deletedKeys.addAll(keysByDeleteFile.computeIfAbsent(delete.location(),
            path -> readKeys(table, delete, projection, identifierField)));
      }

      PartitionGroup group = groups.computeIfAbsent(
          table.spec().partitionToPath(entry.dataFile.partition()),
          path -> new PartitionGroup(entry.dataFile.partition()));
      posConvCount += collectPositions(table, entry.dataFile, projection, identifierField, deletedKeys, group);
    }

    WrittenDeletes deletes = writeDeletes(table, fileFactory, groups, targetMode);

    RewriteFiles rewrite = table.newRewrite();
    for (DeleteFile deleteFile : replaced) {
      rewrite.deleteFile(deleteFile);
    }
    // Vectors the new ones supersede leave in the same commit that adds their
    // replacements; a data file may carry only one vector. Empty for the positional
    // path, where delete files accumulate instead of replacing one another.
    for (DeleteFile deleteFile : deletes.rewritten) {
      rewrite.deleteFile(deleteFile);
    }
    for (DeleteFile deleteFile : deletes.written) {
      rewrite.addFile(deleteFile);
    }

    rewrite.validateFromSnapshot(current.snapshotId());
    rewrite.commit();

    LOGGER.info("migrated {} equality delete files of {} into {} {} delete file(s) covering {} rows, "
        + "superseding {} existing delete file(s)",
        replaced.size(), table.name(), deletes.written.size(), targetMode.wireName(), posConvCount,
        deletes.rewritten.size());

    return new Result(replaced.size(), posConvCount);
  }

  /**
   * Delete files a migration produced, plus the ones it supersedes. Only the deletion
   * vector path can supersede anything: a vector replaces a data file's previous vector,
   * whereas Parquet positional delete files accumulate.
   */
  private static final class WrittenDeletes {
    private final List<DeleteFile> written;
    private final List<DeleteFile> rewritten;

    private WrittenDeletes(List<DeleteFile> written, List<DeleteFile> rewritten) {
      this.written = written;
      this.rewritten = rewritten;
    }
  }

  private static WrittenDeletes writeDeletes(Table table, OutputFileFactory fileFactory,
      Map<String, PartitionGroup> groups, DeleteMode targetMode) throws IOException {
    if (targetMode == DeleteMode.DELETION_VECTOR) {
      return writeDeletionVectors(table, fileFactory, groups);
    }
    return new WrittenDeletes(writePositionDeletes(table, fileFactory, groups), List.of());
  }

  /**
   * One Puffin vector per data file, each published as the union of the positions this
   * migration produces and whatever was already deleted from that data file.
   *
   * <p>The merge is what makes this safe to run on a table that already holds position
   * deletes - one migrated before, or one that ran in pos/dv mode before acquiring fresh
   * equality deletes another way. A vector REPLACES a data file's previous one rather
   * than adding to it, so writing only this migration's positions would resurrect every
   * row an earlier commit deleted. {@link PreviousDeleteLoader} reads the old state
   * (Parquet positional deletes and Puffin vectors alike) so the new vector supersedes
   * it correctly.
   *
   * <p>The vectors it supersedes come back in {@code rewrittenDeleteFiles} and MUST be
   * removed in the same commit that adds the new ones, or the table is left with two
   * vectors for one data file.
   */
  private static WrittenDeletes writeDeletionVectors(Table table, OutputFileFactory fileFactory,
      Map<String, PartitionGroup> groups) throws IOException {

    PartitionSpec spec = table.spec();

    // ONE writer for the WHOLE migration, not one per partition (contrast with
    // writePositionDeletes below): the spec allows many vectors inside one Puffin
    // file with no restriction on which data files they reference, and one writer
    // per partition would recreate the small-files problem vectors exist to remove.
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, new PreviousDeleteLoader(table));
    try {
      for (PartitionGroup group : groups.values()) {
        if (group.positions.isEmpty()) {
          LOGGER.info("No positions to write for partition {}", group.partition);
          continue;
        }
        StructLike partition = spec.isUnpartitioned() ? null : group.partition;
        for (RowPosition row : group.positions) {
          writer.delete(row.path, row.position, spec, partition);
        }
      }
    } finally {
      writer.close();
    }

    DeleteWriteResult result = writer.result();
    return new WrittenDeletes(result.deleteFiles(), result.rewrittenDeleteFiles());
  }

  /** Data files that currently have at least one equality delete applied to them. */
  private static Map<String, DataFileDeletes> planAffectedDataFiles(Table table) throws IOException {
    Map<String, DataFileDeletes> affected = new LinkedHashMap<>();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        List<DeleteFile> equalityDeletes = new ArrayList<>();
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == FileContent.EQUALITY_DELETES) {
            equalityDeletes.add(delete);
          }
        }
        if (equalityDeletes.isEmpty()) {
          continue;
        }

        // planFiles can split a file into several tasks; merge their delete lists.
        affected.computeIfAbsent(task.file().location(), path -> new DataFileDeletes(task.file()))
            .equalityDeletes.addAll(equalityDeletes);
      }
    }
    return affected;
  }

  private static Set<String> readKeys(Table table, DeleteFile delete, Schema projection, String identifierField) {
    if (delete.format() != FileFormat.PARQUET) {
      throw new UnsupportedOperationException(
          "equality delete migration supports parquet delete files only, found " + delete.format());
    }

    Set<String> keys = new HashSet<>();
    try (CloseableIterable<Object> rows =
        TableIndexScanner.openParquet(table, delete.location(), projection)) {
      for (Object row : rows) {
        Object key = TableIndexScanner.getFieldValue(row, identifierField);
        if (key != null) {
          keys.add(key.toString());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to read equality delete file " + delete.location(), e);
    }

    return keys;
  }

  /** Records a positional delete for every row of the file whose key was deleted. */
  private static long collectPositions(Table table, DataFile dataFile, Schema projection, String identifierField,
      Set<String> deletedKeys, PartitionGroup group) throws IOException {
    if (deletedKeys.isEmpty()) {
      return 0L;
    }

    String path = dataFile.location();
    long position = 0L;
    long matched = 0L;

    try (CloseableIterable<Object> rows = TableIndexScanner.openRows(table, dataFile, projection)) {
      for (Object row : rows) {
        Object key = TableIndexScanner.getFieldValue(row, identifierField);
        if (key != null && deletedKeys.contains(key.toString())) {
          group.positions.add(new RowPosition(path, position));
          matched++;
        }
        position++;
      }
    }

    return matched;
  }

  private static List<DeleteFile> writePositionDeletes(Table table, OutputFileFactory fileFactory,
      Map<String, PartitionGroup> groups) throws IOException {
    GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(table);
    FileFormat format = IcebergUtil.getTableFileFormat(table);
    PartitionSpec spec = table.spec();
    List<DeleteFile> written = new ArrayList<>();

    for (PartitionGroup group : groups.values()) {
      if (group.positions.isEmpty()) {
        LOGGER.info("No positions to write for partition {}", group.partition);
        continue;
      }

      // Iceberg expects positional delete files sorted by path then position.
      group.positions.sort(Comparator.comparing((RowPosition row) -> row.path).thenComparingLong(row -> row.position));

      StructLike partition = spec.isUnpartitioned() ? null : group.partition;
      EncryptedOutputFile output = spec.isUnpartitioned()
          ? fileFactory.newOutputFile()
          : fileFactory.newOutputFile(spec, partition);

      PositionDeleteWriter<Record> writer = appenderFactory.newPosDeleteWriter(output, format, partition);
      PositionDelete<Record> positionDelete = PositionDelete.create();
      try {
        for (RowPosition row : group.positions) {
          writer.write(positionDelete.set(row.path, row.position, null));
        }
      } finally {
        writer.close();
      }

      written.addAll(writer.result().deleteFiles());
    }

    return written;
  }

  private static final class DataFileDeletes {
    private final DataFile dataFile;
    private final Set<DeleteFile> equalityDeletes = new LinkedHashSet<>();

    private DataFileDeletes(DataFile dataFile) {
      this.dataFile = dataFile;
    }
  }

  private static final class PartitionGroup {
    private final StructLike partition;
    private final List<RowPosition> positions = new ArrayList<>();

    private PartitionGroup(StructLike partition) {
      this.partition = partition;
    }
  }

  private static final class RowPosition {
    private final String path;
    private final long position;

    private RowPosition(String path, long position) {
      this.path = path;
      this.position = position;
    }
  }
}
