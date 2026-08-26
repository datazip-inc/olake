package io.debezium.server.iceberg.tableIndex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;

import com.google.common.collect.Maps;

/**
 * Positions already deleted from a data file, so a new deletion vector can be published
 * as the union of what it supersedes and what was superseded before.
 *
 * <p>Iceberg allows one vector per data file. Writing a fresh vector holding only the
 * positions from the current batch would resurrect every row an earlier commit already
 * deleted from that file, so the writer has to read the old vector (or old Parquet
 * positional deletes, for a table part way through migrating off them) first.
 *
 * <p>Backed by {@link BaseDeleteLoader}, which reads both Parquet positional delete
 * files and Puffin deletion vectors transparently - do not hand-roll Puffin parsing,
 * {@code org.apache.iceberg.DVUtil} is package-private in Iceberg 1.10.2.
 *
 * <p>The table is planned once, on first use, and reused for the life of this loader:
 * one instance is created per writer session, and the plan describes the snapshot the
 * caller's table index was built against - the same snapshot the commit refuses to move
 * past. Not thread-safe; callers hold one instance per writing thread, same as every
 * other writer-scoped object in this package.
 */
public final class PreviousDeleteLoader implements Function<String, PositionDeleteIndex> {

  private final Table table;
  private Map<String, List<DeleteFile>> deletesByDataFile;
  private BaseDeleteLoader loader;

  public PreviousDeleteLoader(Table table) {
    this.table = table;
  }

  @Override
  public PositionDeleteIndex apply(String dataFilePath) {
    if (deletesByDataFile == null) {
      deletesByDataFile = planDeletes(table);
      loader = new BaseDeleteLoader(file -> table.io().newInputFile(file.location()));
    }

    List<DeleteFile> existing = deletesByDataFile.get(dataFilePath);
    if (existing == null || existing.isEmpty()) {
      // Nothing deleted from this file yet - BaseDVFileWriter treats a null return as
      // "no previous vector", which is correct here.
      return null;
    }
    return loader.loadPositionDeletes(existing, dataFilePath);
  }

  /**
   * ONE manifest scan. Iceberg's planner already works out which delete files apply to
   * which data files (partitions, sequence numbers taken into account); this records
   * the pairing it reports rather than re-deriving it.
   */
  static Map<String, List<DeleteFile>> planDeletes(Table table) {
    Map<String, List<DeleteFile>> byPath = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (task.deletes().isEmpty()) {
          continue;
        }
        String dataFilePath = task.file().location();
        for (DeleteFile delete : task.deletes()) {
          // Equality deletes cannot contribute to a position bitmap - they do not
          // address rows by position - and passing one to loadPositionDeletes() would
          // either be ignored or throw depending on the loader implementation. Filter
          // them out explicitly rather than relying on that.
          if (delete.content() != FileContent.POSITION_DELETES) {
            continue;
          }
          // planFiles can split one data file across several tasks; merge their lists.
          byPath.computeIfAbsent(dataFilePath, path -> new ArrayList<>()).add(delete);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to plan existing deletes of " + table.name(), e);
    }
    return byPath;
  }
}
