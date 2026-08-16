package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;

import com.google.common.collect.Maps;

/**
 * Supplies the positions already deleted from a data file, so a new deletion vector can
 * be published as the union of what it supersedes and what was superseded before.
 *
 * <p>Iceberg allows one vector per data file. Writing a fresh vector holding only this
 * commit's positions would resurrect every row an earlier commit had deleted, so the
 * writer has to read the old vector first. Only files this writer actually deletes from
 * are loaded, and the table is planned once on first use rather than per lookup.
 *
 * <p>The plan is deliberately taken once and reused: it describes the snapshot the
 * caller's row index was built against, which is the same snapshot the commit refuses
 * to move past.
 */
final class PreviousDeleteLoader implements Function<String, PositionDeleteIndex> {

  private final Table table;
  private Map<String, List<DeleteFile>> deletesByDataFile;
  private BaseDeleteLoader loader;

  PreviousDeleteLoader(Table table) {
    this.table = table;
  }

  @Override
  public PositionDeleteIndex apply(String dataFilePath) {
    if (deletesByDataFile == null) {
      deletesByDataFile = planDeletes();
      loader = new BaseDeleteLoader(file -> table.io().newInputFile(file.location()));
    }

    List<DeleteFile> existing = deletesByDataFile.get(dataFilePath);
    if (existing == null || existing.isEmpty()) {
      return null;
    }
    // Handles both Puffin vectors and positional delete files, so a table part way
    // through a migration still reports every position already deleted.
    return loader.loadPositionDeletes(existing, dataFilePath);
  }

  private Map<String, List<DeleteFile>> planDeletes() {
    Map<String, List<DeleteFile>> byPath = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if (task.deletes().isEmpty()) {
          continue;
        }
        // planFiles can split one file across several tasks; merge their delete lists.
        byPath.computeIfAbsent(task.file().location(), path -> new java.util.ArrayList<>())
            .addAll(task.deletes());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to plan existing deletes of " + table.name(), e);
    }
    return byPath;
  }
}
