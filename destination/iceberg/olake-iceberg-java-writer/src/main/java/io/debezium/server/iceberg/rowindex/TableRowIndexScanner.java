package io.debezium.server.iceberg.rowindex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovers, for every row of a table, the value of the identifier column together
 * with the {@code (file, ordinal)} pair that an Iceberg positional delete
 * addresses. This is what lets a caller express a delete positionally instead of
 * by equality.
 *
 * <p>Rows are emitted oldest first, ordered by data sequence number and then by
 * path. A caller that overwrites entries as they arrive therefore ends up holding
 * the newest version of every identifier, which is the row a subsequent update or
 * delete has to evict. Rows already covered by a positional delete are skipped,
 * so the result describes live rows rather than the table's whole history.
 */
public final class TableRowIndexScanner {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableRowIndexScanner.class);
  private static final BitSet EMPTY_POSITIONS = new BitSet(0);

  private TableRowIndexScanner() {
  }

  /** Receives one identifier-to-location mapping at a time. */
  public interface EntryConsumer {
    /**
     * Announces the snapshot the following entries belong to. Called before the
     * first entry so a consumer that ships entries incrementally can stamp every
     * batch with the snapshot rather than only the last one.
     */
    void begin(long snapshotId);

    void accept(String identifier, String filePath, long position) throws Exception;
  }

  /** Outcome of a scan request. */
  public static final class ScanResult {
    public final long snapshotId;
    public final long entries;

    private ScanResult(long snapshotId, long entries) {
      this.snapshotId = snapshotId;
      this.entries = entries;
    }
  }

  /**
   * Streams the identifier and location of the table's rows to {@code consumer}.
   *
   * @param fromSnapshotId when non-null, only files added after this snapshot are
   *                       read.
   */
  public static ScanResult scan(Table table, String identifierField, Long fromSnapshotId, EntryConsumer consumer)
      throws Exception {
    table.refresh();

    Snapshot current = table.currentSnapshot();
    if (current == null) {
      // Nothing has been committed yet, so there is nothing to index.
      consumer.begin(0L);
      return new ScanResult(0L, 0L);
    }

    List<DataFile> addedFiles;

    if (fromSnapshotId == null) {
      addedFiles = currentDataFiles(table);
    } else if (fromSnapshotId == current.snapshotId()) {
      addedFiles = List.of();
    } else {
      List<DataFile> changes = dataFileChangesSince(table, fromSnapshotId, current.snapshotId());
      if (changes == null) {
        throw new IllegalStateException(String.format(
            "cannot catch up stream index for table %s: snapshot %d is not an ancestor of current snapshot %d",
            table.name(), fromSnapshotId, current.snapshotId()));
      }
      addedFiles = changes;
    }

    consumer.begin(current.snapshotId());
    Schema projection = identifierProjection(table, identifierField);
    Map<String, BitSet> deletedPositions = deletedPositions(table);
    long entries = 0L;

    for (DataFile file : addedFiles) {
      BitSet deleted = deletedPositions.getOrDefault(file.location(), EMPTY_POSITIONS);
      entries += emitFile(table, file, projection, identifierField, deleted, consumer);
    }

    LOGGER.info("table index scan of {} covered {} added files, {} row entries, up to snapshot {}",
        table.name(), addedFiles.size(), entries, current.snapshotId());

    return new ScanResult(current.snapshotId(), entries);
  }

  /**
   * Positions already removed by positional delete files, keyed by data file path.
   * Skipping these keeps the index proportional to the number of live rows rather
   * than to everything the table has ever held.
   */ 
  private static Map<String, BitSet> deletedPositions(Table table) throws IOException {
    Map<String, BitSet> byFile = new HashMap<>();
    Schema pathPos = DeleteSchemaUtil.pathPosSchema();

    for (DeleteFile delete : deleteFiles(table, FileContent.POSITION_DELETES)) {
      try (CloseableIterable<Object> rows = openParquet(table, delete.location(), pathPos)) {
        for (Object row : rows) {
          Object path = getFieldValue(row, MetadataColumns.DELETE_FILE_PATH.name());
          Object position = getFieldValue(row, MetadataColumns.DELETE_FILE_POS.name());
          if (path == null || position == null) {
            continue;
          }
          long ordinal = position instanceof Number n ? n.longValue() : Long.parseLong(position.toString());
          if (ordinal > Integer.MAX_VALUE) {
            // No realistic data file holds this many rows.
            continue;
          }
          byFile.computeIfAbsent(path.toString(), key -> new BitSet()).set((int) ordinal);
        }
      }
    }

    return byFile;
  }

  /** Every data file visible in the table's current snapshot, oldest first. */
  private static List<DataFile> currentDataFiles(Table table) throws IOException {
    Map<String, DataFile> unique = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      // planFiles may split one file into several tasks; index each file once.
      for (FileScanTask task : tasks) {
        unique.putIfAbsent(task.file().location(), task.file());
      }
    }
    return sortedOldestFirst(unique.values());
  }

  /**
   * What changed between two snapshots, returning added data files
   */
  private static List<DataFile> dataFileChangesSince(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Snapshot> range = snapshotRange(table, fromSnapshotId, toSnapshotId);
    if (range == null) {
      LOGGER.info("snapshot {} is no longer an ancestor of {} in {}, a full table index scan is required",
          fromSnapshotId, toSnapshotId, table.name());
      return null;
    }

    Map<String, DataFile> added = new LinkedHashMap<>();
    try {
      for (Snapshot snapshot : range) {
        for (DataFile file : snapshot.addedDataFiles(table.io())) {
          added.putIfAbsent(file.location(), file);
        }
      }
    } catch (RuntimeException e) {
      // Reading a snapshot's manifests can fail once expiry has collected them.
      LOGGER.info("cannot read the data file changes of {} between snapshots {} and {}, "
          + "a full table index scan is required", table.name(), fromSnapshotId, toSnapshotId, e);
      return null;
    }

    return sortedOldestFirst(added.values());
  }

  /**
   * Snapshots after {@code fromSnapshotId} up to and including
   * {@code toSnapshotId}, oldest first, or null when {@code fromSnapshotId} is
   * not an ancestor of {@code toSnapshotId}.
   */
  private static List<Snapshot> snapshotRange(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Snapshot> newestFirst = new ArrayList<>();
    Long cursor = toSnapshotId;

    while (cursor != null) {
      if (cursor == fromSnapshotId) {
        Collections.reverse(newestFirst);
        return newestFirst;
      }

      Snapshot snapshot = table.snapshot(cursor);
      if (snapshot == null) {
        return null;
      }
      newestFirst.add(snapshot);
      cursor = snapshot.parentId();
    }

    return null;
  }

  private static List<DataFile> sortedOldestFirst(Collection<DataFile> files) {
    List<DataFile> sorted = new ArrayList<>(files);
    // Oldest first so that a caller overwriting entries keeps the newest version
    // of each identifier. Path breaks ties for a deterministic result.
    sorted.sort(Comparator
        .comparingLong((DataFile file) -> file.dataSequenceNumber() == null ? 0L : file.dataSequenceNumber())
        .thenComparing(file -> file.location()));
    return sorted;
  }

  static Schema identifierProjection(Table table, String identifierField) {
    NestedField field = table.schema().findField(identifierField);
    if (field == null) {
      throw new IllegalStateException(
          "table " + table.name() + " has no identifier column " + identifierField + " to index rows by");
    }
    return new Schema(field);
  }

  /**
   * Reads one data file and emits an entry per live row, ordinal counted from zero.
   * Returns the number of entries emitted.
   */
  private static long emitFile(Table table, DataFile file, Schema projection, String identifierField,
      BitSet deleted, EntryConsumer consumer) throws Exception {
    String path = file.location();
    long position = 0L;
    long emitted = 0L;

    try (CloseableIterable<Object> rows = openRows(table, file, projection)) {
      for (Object row : rows) {
        Object identifier = getFieldValue(row, identifierField);
        if (identifier != null && !isDeleted(deleted, position)) {
          consumer.accept(identifier.toString(), path, position);
          emitted++;
        }
        position++;
      }
    }

    return emitted;
  }

  /** BitSet indexes by int, so ordinals beyond its range count as live. */
  private static boolean isDeleted(BitSet deleted, long position) {
    return position <= Integer.MAX_VALUE && deleted.get((int) position);
  }

  /** Extracts a field value from either an Iceberg Record or an Avro GenericRecord. */
  public static Object getFieldValue(Object row, String fieldName) {
    if (row instanceof Record r) {
      return r.getField(fieldName);
    } else if (row instanceof GenericRecord g) {
      return g.get(fieldName);
    }
    return null;
  }

  /**
   * Opens a data file for a sequential read. Reading the whole file in order is
   * what makes the running ordinal equal Iceberg's row position.
   */
  static CloseableIterable<Object> openRows(Table table, DataFile file, Schema projection) {
    if (file.format() != FileFormat.PARQUET) {
      throw new UnsupportedOperationException(
          "table index scanning supports parquet data files only, found " + file.format() + " in " + table.name());
    }

    try {
      return openParquet(table, file.location(), projection);
    } catch (UncheckedIOException e) {
      throw new UncheckedIOException("failed to read data file " + file.location(), e.getCause());
    }
  }

  /** Opens any Iceberg-written parquet file projected down to {@code projection}. */
  static CloseableIterable<Object> openParquet(Table table, String path, Schema projection) {
    return Parquet.read(table.io().newInputFile(path))
        .project(projection)
        .reuseContainers()
        .build();
  }

  /** Distinct delete files of the given content type visible in the current snapshot. */
  public static Set<DeleteFile> deleteFiles(Table table, FileContent content) throws IOException {
    Set<DeleteFile> found = new LinkedHashSet<>();
    Set<String> seen = new HashSet<>();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == content && seen.add(delete.location())) {
            found.add(delete);
          }
        }
      }
    }

    return found;
  }
}
