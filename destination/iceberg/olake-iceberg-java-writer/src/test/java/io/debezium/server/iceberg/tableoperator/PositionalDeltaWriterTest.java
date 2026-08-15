package io.debezium.server.iceberg.tableoperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.ContentFileUtil;
import io.debezium.server.iceberg.IcebergUtil;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage for {@link PositionalDeltaWriter} against a real Iceberg table on
 * a local Hadoop catalog: rows are written, committed through {@link RowDelta}, and read
 * back through a normal scan, so Iceberg itself decides whether each positional delete
 * applied.
 */
class PositionalDeltaWriterTest {

  private static final Schema SCHEMA = new Schema(
      List.of(
          Types.NestedField.required(1, "_olake_id", Types.StringType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "region", Types.StringType.get()),
          Types.NestedField.optional(4, "_op_type", Types.StringType.get())),
      Set.of(1));

  @TempDir
  Path warehouse;

  private HadoopCatalog catalog;
  private final AtomicInteger partitionId = new AtomicInteger();

  @BeforeEach
  void setUp() {
    catalog = new HadoopCatalog(new Configuration(), warehouse.toAbsolutePath().toString());
  }

  @AfterEach
  void tearDown() throws IOException {
    catalog.close();
  }

  // ---------------------------------------------------------------- scenarios

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void unpartitionedInsertsLandAsPlainDataFiles(DeleteMode mode) throws Exception {
    Table table = createTable("unpartitioned_inserts", PartitionSpec.unpartitioned(), mode);

    WriteResult result = write(table, List.of(
        insert("a", "Alice", "in"),
        insert("b", "Bob", "in"),
        insert("c", "Carol", "eu")), mode);

    assertEquals(1, result.dataFiles().length, "one data file for an unpartitioned table");
    assertEquals(0, result.deleteFiles().length, "inserts alone must not produce delete files");

    commit(table, result);
    assertEquals(Set.of("a", "b", "c"), liveIds(table));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void positionalDeleteSupersedesACommittedRow(DeleteMode mode) throws Exception {
    Table table = createTable("supersede_committed", PartitionSpec.unpartitioned(), mode);

    // sync 1: three rows land, and we note where "b" went
    WriteResult first = write(table, List.of(
        insert("a", "Alice", "in"),
        insert("b", "Bob", "in"),
        insert("c", "Carol", "eu")), mode);
    commit(table, first);

    String dataFile = first.dataFiles()[0].location();

    // sync 2: "b" is updated, superseding position 1 of that file — exactly what the
    // row index feeds the writer
    WriteResult second = write(table, List.of(
        update("b", "Bobby", "in", dataFile, 1L)), mode);
    assertEquals(1, second.deleteFiles().length, "one positional delete file");
    assertEquals(FileContent.POSITION_DELETES, second.deleteFiles()[0].content());
    commit(table, second);

    assertEquals(Set.of("a", "b", "c"), liveIds(table), "no row lost, none duplicated");
    assertEquals(1, countById(table, "b"), "the superseded version of b must be gone");
    assertEquals("Bobby", nameOf(table, "b"));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void sameKeyUpdatedTwiceInOneBatchLeavesOneLiveRow(DeleteMode mode) throws Exception {
    Table table = createTable("same_batch_updates", PartitionSpec.unpartitioned(), mode);

    WriteResult first = write(table, List.of(insert("k", "v1", "in")), mode);
    commit(table, first);
    String dataFile = first.dataFiles()[0].location();

    // Both updates carry the SAME superseded location, because the caller's index is
    // only refreshed once the batch is answered. The writer has to notice that it
    // wrote "k" itself a moment ago and supersede that row too.
    WriteResult second = write(table, List.of(
        update("k", "v2", "in", dataFile, 0L),
        update("k", "v3", "in", dataFile, 0L)), mode);
    commit(table, second);

    assertEquals(1, countById(table, "k"), "only the newest version of k may survive");
    assertEquals("v3", nameOf(table, "k"));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void sameKeyUpdatedThreeTimesInOneBatchLeavesOneLiveRow(DeleteMode mode) throws Exception {
    Table table = createTable("same_batch_updates_thrice", PartitionSpec.unpartitioned(), mode);

    WriteResult first = write(table, List.of(insert("k", "v1", "in"), insert("other", "x", "in")), mode);
    commit(table, first);
    String dataFile = first.dataFiles()[0].location();

    WriteResult second = write(table, List.of(
        update("k", "v2", "in", dataFile, 0L),
        update("k", "v3", "in", dataFile, 0L),
        update("k", "v4", "in", dataFile, 0L)), mode);
    commit(table, second);

    assertEquals(1, countById(table, "k"));
    assertEquals("v4", nameOf(table, "k"));
    assertEquals(1, countById(table, "other"), "an untouched row must be unaffected");
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void sameKeyUpdatedAcrossBatchesLeavesOneLiveRow(DeleteMode mode) throws Exception {
    Table table = createTable("cross_batch_updates", PartitionSpec.unpartitioned(), mode);

    WriteResult first = write(table, List.of(insert("k", "v1", "in")), mode);
    commit(table, first);

    // batch 2 supersedes the committed row; batch 3 supersedes batch 2's row, which is
    // where the caller's index now points
    WriteResult second = write(table, List.of(
        update("k", "v2", "in", first.dataFiles()[0].location(), 0L)), mode);
    commit(table, second);

    WriteResult third = write(table, List.of(
        update("k", "v3", "in", second.dataFiles()[0].location(), 0L)), mode);
    commit(table, third);

    assertEquals(1, countById(table, "k"));
    assertEquals("v3", nameOf(table, "k"));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void partitionedFanoutRoutesInterleavedRecords(DeleteMode mode) throws Exception {
    Table table = createTable("partitioned_fanout",
        PartitionSpec.builderFor(SCHEMA).identity("region").build(), mode);

    // deliberately interleaved so consecutive records hit different partition writers
    WriteResult result = write(table, List.of(
        insert("a", "Alice", "in"),
        insert("b", "Bob", "eu"),
        insert("c", "Carol", "in"),
        insert("d", "Dan", "us"),
        insert("e", "Eve", "eu")), mode);

    assertEquals(3, result.dataFiles().length, "one data file per partition");
    assertEquals(0, result.deleteFiles().length);

    commit(table, result);
    assertEquals(Set.of("a", "b", "c", "d", "e"), liveIds(table));

    Map<String, Long> perPartition = new HashMap<>();
    for (DataFile file : result.dataFiles()) {
      perPartition.put(file.partition().get(0, String.class), file.recordCount());
    }
    assertEquals(Map.of("in", 2L, "eu", 2L, "us", 1L), perPartition);
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void partitionedPositionalDeleteSupersedesWithinTheSamePartition(DeleteMode mode) throws Exception {
    Table table = createTable("partitioned_supersede",
        PartitionSpec.builderFor(SCHEMA).identity("region").build(), mode);

    WriteResult first = write(table, List.of(
        insert("a", "Alice", "in"),
        insert("b", "Bob", "eu"),
        insert("c", "Carol", "in")), mode);
    commit(table, first);

    Map<String, DataFile> byRegion = filesByRegion(first);
    // "c" is the second row written into the "in" partition
    DataFile inFile = byRegion.get("in");
    assertEquals(2L, inFile.recordCount());

    WriteResult second = write(table, List.of(
        update("c", "Caroline", "in", inFile.location(), 1L)), mode);
    assertEquals(1, second.deleteFiles().length);
    assertEquals("in", second.deleteFiles()[0].partition().get(0, String.class),
        "the delete file must carry the partition of the data file it targets");
    commit(table, second);

    assertEquals(Set.of("a", "b", "c"), liveIds(table));
    assertEquals(1, countById(table, "c"));
    assertEquals("Caroline", nameOf(table, "c"));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void partitionedSameKeyUpdatedTwiceInOneBatch(DeleteMode mode) throws Exception {
    Table table = createTable("partitioned_same_batch",
        PartitionSpec.builderFor(SCHEMA).identity("region").build(), mode);

    WriteResult first = write(table, List.of(insert("k", "v1", "eu"), insert("j", "w1", "in")), mode);
    commit(table, first);
    DataFile euFile = filesByRegion(first).get("eu");

    WriteResult second = write(table, List.of(
        update("k", "v2", "eu", euFile.location(), 0L),
        update("j", "w2", "in", filesByRegion(first).get("in").location(), 0L),
        update("k", "v3", "eu", euFile.location(), 0L)), mode);
    commit(table, second);

    assertEquals(1, countById(table, "k"));
    assertEquals("v3", nameOf(table, "k"));
    assertEquals(1, countById(table, "j"));
    assertEquals("w2", nameOf(table, "j"));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void tenThousandRowBatchWithRepeatedKeys(DeleteMode mode) throws Exception {
    Table table = createTable("large_batch", PartitionSpec.unpartitioned(), mode);

    // seed 1000 distinct keys
    List<RecordWrapper> seed = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      seed.add(insert("k" + i, "v0", i % 4 == 0 ? "in" : "eu"));
    }
    WriteResult first = write(table, seed, mode);
    commit(table, first);
    assertEquals(1000, liveIds(table).size());

    String dataFile = first.dataFiles()[0].location();

    // 10k updates over the same 1000 keys: every key is superseded ten times, nine of
    // those against a row this very batch produced
    List<RecordWrapper> batch = new ArrayList<>();
    for (int round = 0; round < 10; round++) {
      for (int i = 0; i < 1000; i++) {
        batch.add(update("k" + i, "v" + round, i % 4 == 0 ? "in" : "eu", dataFile, (long) i));
      }
    }
    assertEquals(10_000, batch.size());

    WriteResult second = write(table, batch, mode);
    commit(table, second);

    Set<String> live = liveIds(table);
    assertEquals(1000, live.size(), "10k updates over 1000 keys must leave 1000 live rows");
    assertEquals(1000, countRows(table), "no duplicates may survive");
    assertEquals("v9", nameOf(table, "k7"), "the last write of each key wins");
    assertEquals("v9", nameOf(table, "k0"));
  }

  @Test
  void writeRunsDescribeWhereEachRowLanded() throws Exception {
    Table table = createTable("write_runs", PartitionSpec.unpartitioned());

    IcebergTableOperator operator = new IcebergTableOperator(true, true);
    List<RecordWrapper> events = List.of(
        insert("a", "Alice", "in"),
        insert("b", "Bob", "in"),
        insert("c", "Carol", "in"));

    List<io.debezium.server.iceberg.rpc.RecordIngest.WriteRun> runs =
        operator.addToTablePerSchema("t1", table, events);

    assertEquals(1, runs.size(), "contiguous writes into one file collapse to one run");
    assertEquals(0, runs.get(0).getBatchStartIdx());
    assertEquals(0L, runs.get(0).getStartPosition());
    assertEquals(3, runs.get(0).getCount());

    operator.completeWriter();
    assertTrue(runs.get(0).getFilePath().endsWith(".parquet"));
  }

  @Test
  void writeRunsBreakPerPartitionOnInterleavedInput() throws Exception {
    Table table = createTable("write_runs_partitioned",
        PartitionSpec.builderFor(SCHEMA).identity("region").build());

    IcebergTableOperator operator = new IcebergTableOperator(true, true);
    List<RecordWrapper> events = List.of(
        insert("a", "Alice", "in"),
        insert("b", "Bob", "eu"),
        insert("c", "Carol", "in"));

    List<io.debezium.server.iceberg.rpc.RecordIngest.WriteRun> runs =
        operator.addToTablePerSchema("t1", table, events);
    operator.completeWriter();

    // every record maps to exactly one (path, position) pair, whichever way the runs
    // were cut
    Map<Integer, String> pathByIdx = new LinkedHashMap<>();
    Map<Integer, Long> posByIdx = new LinkedHashMap<>();
    for (io.debezium.server.iceberg.rpc.RecordIngest.WriteRun run : runs) {
      for (int i = 0; i < run.getCount(); i++) {
        pathByIdx.put(run.getBatchStartIdx() + i, run.getFilePath());
        posByIdx.put(run.getBatchStartIdx() + i, run.getStartPosition() + i);
      }
    }

    assertEquals(3, pathByIdx.size(), "every record must be covered exactly once");
    assertEquals(pathByIdx.get(0), pathByIdx.get(2), "both 'in' rows share a file");
    assertFalse(pathByIdx.get(0).equals(pathByIdx.get(1)), "'eu' lands elsewhere");
    assertEquals(0L, posByIdx.get(0));
    assertEquals(0L, posByIdx.get(1));
    assertEquals(1L, posByIdx.get(2), "the second 'in' row is at offset 1 of its file");
  }

  @Test
  void equalityModeStillUsesEqualityDeletes() throws Exception {
    Table table = createTable("equality_mode", PartitionSpec.unpartitioned());

    IcebergTableWriterFactory factory = new IcebergTableWriterFactory();
    factory.upsert = true;
    factory.keepDeletes = true;
    factory.usePositionalDeletes = false;

    var writer = factory.create(table);
    assertFalse(writer instanceof PositionalDeltaWriter, "equality mode keeps the old writer");

    // "u" takes the equality-delete branch
    writer.write(update("a", "Alice", "in", null, null));
    WriteResult result = writer.complete();

    assertEquals(1, result.deleteFiles().length);
    assertEquals(FileContent.EQUALITY_DELETES, result.deleteFiles()[0].content());
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void rollingToANewFileKeepsPositionsConsistent(DeleteMode mode) throws Exception {
    Table table = createTable("rolling_files", PartitionSpec.unpartitioned(), mode);

    // a tiny target size forces the data writer to roll mid-batch
    List<RecordWrapper> rows = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      rows.add(insert("k" + i, "value-" + i, "in"));
    }

    WriteResult result = write(table, rows, 4096L, mode);
    assertTrue(result.dataFiles().length > 1, "expected the writer to roll to new files");

    long total = 0;
    for (DataFile file : result.dataFiles()) {
      total += file.recordCount();
    }
    assertEquals(5000, total, "every row must land exactly once across the rolled files");

    commit(table, result);
    assertEquals(5000, countRows(table));
    assertEquals(5000, liveIds(table).size());
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void updatesSpanningARollAreStillSuperseded(DeleteMode mode) throws Exception {
    Table table = createTable("rolling_updates", PartitionSpec.unpartitioned(), mode);

    List<RecordWrapper> seed = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      seed.add(insert("k" + i, "v0", "in"));
    }
    WriteResult first = write(table, seed, 4096L, mode);
    commit(table, first);
    assertTrue(first.dataFiles().length > 1, "seed should span several files");

    // Supersede every seeded row using the file/offset it actually landed at, which is
    // what the row index would hold. Walk the files in write order.
    List<RecordWrapper> updates = new ArrayList<>();
    int seen = 0;
    for (DataFile file : first.dataFiles()) {
      for (long pos = 0; pos < file.recordCount(); pos++) {
        updates.add(update("k" + seen, "v1", "in", file.location(), pos));
        seen++;
      }
    }
    assertEquals(2000, seen);

    WriteResult second = write(table, updates, 4096L, mode);
    commit(table, second);

    assertEquals(2000, countRows(table), "no superseded row may survive a roll");
    assertEquals("v1", nameOf(table, "k1999"));
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void partitionChangingUpdateSupersedesTheOldRow(DeleteMode mode) throws Exception {
    // The delete is written into the NEW record's partition, but FILE granularity plus
    // full file_path bounds make it file-scoped, so Iceberg matches it to the old row's
    // data file by path and the partition it was filed under does not matter.
    Table table = createTable("partition_change",
        PartitionSpec.builderFor(SCHEMA).identity("region").build(), mode);

    WriteResult first = write(table, List.of(insert("m", "Mo", "in"), insert("n", "Ned", "in")), mode);
    commit(table, first);
    DataFile inFile = filesByRegion(first).get("in");

    // the row moves from region=in to region=eu
    WriteResult second = write(table, List.of(
        update("m", "Mo2", "eu", inFile.location(), 0L)), mode);
    assertEquals("eu", second.deleteFiles()[0].partition().get(0, String.class),
        "the delete is filed under the new record's partition");
    assertEquals(inFile.location(), ContentFileUtil.referencedDataFile(second.deleteFiles()[0]).toString(),
        "and is file-scoped, so it is matched by path rather than by partition");
    commit(table, second);

    assertEquals(1, countById(table, "m"), "the superseded row must not survive the move");
    assertEquals("Mo2", nameOf(table, "m"));
    assertEquals("eu", regionOf(table, "m"));
    assertEquals(2, countRows(table), "the untouched row in region=in stays");
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void writeResultReportsTheDataFilesItsDeletesDependOn(DeleteMode mode) throws Exception {
    // The commit path needs these to ask Iceberg for validateDataFilesExist, which is
    // what turns a concurrent compaction into a refused commit rather than positional
    // deletes that silently resolve to nothing.
    Table table = createTable("referenced_files", PartitionSpec.unpartitioned(), mode);

    WriteResult first = write(table, List.of(insert("a", "A", "in"), insert("b", "B", "in")), mode);
    commit(table, first);
    String dataFile = first.dataFiles()[0].location();

    WriteResult second = write(table, List.of(update("a", "A2", "in", dataFile, 0L)), mode);

    assertEquals(1, second.referencedDataFiles().length,
        "the delta must report the file its positional delete points into");
    assertEquals(dataFile, second.referencedDataFiles()[0].toString());
  }

  @ParameterizedTest
  @EnumSource(names = {"POSITION", "DELETION_VECTOR"})
  void referencedFilesCoverEveryPartitionTouchedByDeletes(DeleteMode mode) throws Exception {
    Table table = createTable("referenced_files_partitioned",
        PartitionSpec.builderFor(SCHEMA).identity("region").build(), mode);

    WriteResult first = write(table, List.of(insert("a", "A", "in"), insert("b", "B", "eu")), mode);
    commit(table, first);
    Map<String, DataFile> byRegion = filesByRegion(first);

    WriteResult second = write(table, List.of(
        update("a", "A2", "in", byRegion.get("in").location(), 0L),
        update("b", "B2", "eu", byRegion.get("eu").location(), 0L)), mode);

    Set<String> referenced = new HashSet<>();
    for (CharSequence path : second.referencedDataFiles()) {
      referenced.add(path.toString());
    }
    assertEquals(Set.of(byRegion.get("in").location(), byRegion.get("eu").location()), referenced);
  }

  @Test
  void validationRefusesDeletesWhoseDataFileWasRewritten() throws Exception {
    // The scenario assertRowIndexCurrent cannot cover: a concurrent rewrite lands after
    // the pre-check but before the catalog commit. Without these validations the commit
    // succeeds and the positional deletes resolve to nothing.
    Table table = createTable("stale_reference", PartitionSpec.unpartitioned());

    WriteResult first = write(table, List.of(insert("a", "A", "in"), insert("b", "B", "in")));
    commit(table, first);
    long baseSnapshot = table.currentSnapshot().snapshotId();
    DataFile original = first.dataFiles()[0];

    // a concurrent compaction rewrites the file our positions point into
    table.newDelete().deleteFile(original).commit();
    table.refresh();

    WriteResult second = write(table, List.of(
        update("a", "A2", "in", original.location(), 0L)));

    RowDelta rowDelta = table.newRowDelta();
    for (DataFile file : second.dataFiles()) {
      rowDelta.addRows(file);
    }
    for (DeleteFile file : second.deleteFiles()) {
      rowDelta.addDeletes(file);
    }
    rowDelta.validateFromSnapshot(baseSnapshot);
    rowDelta.validateDeletedFiles();
    rowDelta.validateDataFilesExist(Arrays.asList(second.referencedDataFiles()));

    assertThrows(ValidationException.class, rowDelta::commit,
        "committing deletes against a rewritten data file must be refused");
  }

  @Test
  void supersedeStateIsScopedToOneBatchButStillCorrectAcrossThem() throws Exception {
    // Exercises the real batch loop: per-batch state is released once the write runs
    // are handed back, and the next batch resolves the same key through the delete
    // path the caller supplies. Both batches share one uncommitted writer session.
    Table table = createTable("batch_scoped_supersede", PartitionSpec.unpartitioned());
    IcebergTableOperator operator = new IcebergTableOperator(true, true);

    List<io.debezium.server.iceberg.rpc.RecordIngest.WriteRun> runs1 =
        operator.addToTablePerSchema("t1", table, List.of(insert("k", "v1", "in")));
    String path1 = runs1.get(0).getFilePath();
    long pos1 = runs1.get(0).getStartPosition();

    // Second batch: the caller now knows where v1 landed, so it addresses that row.
    // Both updates carry the same location, as the legacy path always does.
    List<io.debezium.server.iceberg.rpc.RecordIngest.WriteRun> runs2 =
        operator.addToTablePerSchema("t1", table, List.of(
            update("k", "v2", "in", path1, pos1),
            update("k", "v3", "in", path1, pos1)));
    assertFalse(runs2.isEmpty());

    operator.commitThread("t1", null, table, null);
    table.refresh();

    assertEquals(1, countById(table, "k"), "only the newest version may survive");
    assertEquals("v3", nameOf(table, "k"));
    assertEquals(1, countRows(table));
  }

  @Test
  void deletionVectorMergesWithTheOneAlreadyOnTheDataFile() throws Exception {
    // The case that makes or breaks vectors: a data file carries one vector, so a
    // later commit deleting another row from it must publish the union and retire the
    // old vector. Getting this wrong resurrects everything deleted earlier.
    Table table = createTable("dv_merge", PartitionSpec.unpartitioned(), DeleteMode.DELETION_VECTOR);

    WriteResult seed = write(table, List.of(
        insert("a", "A", "in"), insert("b", "B", "in"), insert("c", "C", "in")),
        DeleteMode.DELETION_VECTOR);
    commit(table, seed);
    String dataFile = seed.dataFiles()[0].location();

    WriteResult first = write(table, List.of(update("a", "A2", "in", dataFile, 0L)),
        DeleteMode.DELETION_VECTOR);
    assertEquals(0, first.rewrittenDeleteFiles().length, "no prior vector to retire yet");
    commit(table, first);
    assertEquals(3, countRows(table));

    // second sync deletes a different row of the same data file
    WriteResult second = write(table, List.of(update("b", "B2", "in", dataFile, 1L)),
        DeleteMode.DELETION_VECTOR);
    assertEquals(1, second.rewrittenDeleteFiles().length,
        "the data file's previous vector must be retired");
    commit(table, second);

    assertEquals(3, countRows(table), "a must not come back when b is deleted");
    assertEquals("A2", nameOf(table, "a"));
    assertEquals("B2", nameOf(table, "b"));
    assertEquals("C", nameOf(table, "c"));
  }

  @Test
  void aDataFileEndsUpWithExactlyOneDeletionVector() throws Exception {
    // Three commits each delete a different row of the same seed file, so each one has
    // to merge into the vector the previous commit left behind rather than add another.
    Table table = createTable("dv_single", PartitionSpec.unpartitioned(), DeleteMode.DELETION_VECTOR);

    WriteResult seed = write(table, List.of(
        insert("a", "A", "in"), insert("b", "B", "in"), insert("c", "C", "in")),
        DeleteMode.DELETION_VECTOR);
    commit(table, seed);
    String dataFile = seed.dataFiles()[0].location();

    String[] keys = {"a", "b", "c"};
    for (int round = 0; round < 3; round++) {
      WriteResult next = write(table, List.of(
          update(keys[round], keys[round] + "-v2", "in", dataFile, (long) round)),
          DeleteMode.DELETION_VECTOR);
      assertEquals(round == 0 ? 0 : 1, next.rewrittenDeleteFiles().length,
          "every commit after the first must retire the file's previous vector");
      commit(table, next);
    }

    List<DeleteFile> live = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        live.addAll(task.deletes());
      }
    }
    assertEquals(1, live.size(), "exactly one vector may remain for the data file");
    assertTrue(ContentFileUtil.isDV(live.get(0)));

    assertEquals(3, countRows(table), "three rows superseded, three rewritten");
    assertEquals("a-v2", nameOf(table, "a"));
    assertEquals("b-v2", nameOf(table, "b"));
    assertEquals("c-v2", nameOf(table, "c"));
  }

  @Test
  void equalityDeletesMigrateIntoDeletionVectors() throws Exception {
    // Equality deletes are legal on v3, so a table can be created for vectors and
    // still arrive carrying them from an earlier sync.
    Table table = createTable("eq_to_dv", PartitionSpec.unpartitioned(), DeleteMode.DELETION_VECTOR);

    IcebergTableWriterFactory equality = new IcebergTableWriterFactory();
    equality.upsert = true;
    equality.keepDeletes = true;
    equality.deleteMode = DeleteMode.EQUALITY;

    // Equality deletes only apply to data files from earlier snapshots, so the rows
    // have to be committed before the delete that supersedes them.
    var seedWriter = equality.create(table);
    seedWriter.write(insert("a", "A", "in"));
    seedWriter.write(insert("b", "B", "in"));
    seedWriter.write(insert("c", "C0", "in"));
    commit(table, seedWriter.complete());

    var writer = equality.create(table);
    writer.write(update("c", "C", "in", null, null));   // equality-deletes then writes
    commit(table, writer.complete());

    boolean hadEqualityDeletes = false;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile file : task.deletes()) {
          hadEqualityDeletes |= file.content() == FileContent.EQUALITY_DELETES;
        }
      }
    }
    assertTrue(hadEqualityDeletes, "the table must start with equality deletes");

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId.incrementAndGet(), 1L)
            .format(FileFormat.PARQUET).build();
    io.debezium.server.iceberg.rowindex.EqualityDeleteMigrator.Result result =
        io.debezium.server.iceberg.rowindex.EqualityDeleteMigrator.migrate(
            table, "_olake_id", fileFactory, DeleteMode.DELETION_VECTOR);
    table.refresh();

    assertTrue(result.rewrittenDeleteFiles > 0, "the equality deletes must be rewritten");

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile file : task.deletes()) {
          assertNotEquals(FileContent.EQUALITY_DELETES, file.content(),
              "no equality delete may survive the migration");
          assertTrue(ContentFileUtil.isDV(file), "deletes must now be vectors");
        }
      }
    }
    assertEquals(Set.of("a", "b", "c"), liveIds(table));
    assertEquals(3, countRows(table));
  }

  // ---------------------------------------------------------------- helpers

  /** Deletion vectors are a v3 construct; positional deletes stay on v2. */
  private Table createTable(String name, PartitionSpec spec) {
    return createTable(name, spec, DeleteMode.POSITION);
  }

  private Table createTable(String name, PartitionSpec spec, DeleteMode mode) {
    return catalog.buildTable(TableIdentifier.of("test", name + "_" + mode.wireName()), SCHEMA)
        .withPartitionSpec(spec)
        .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(mode.minimumFormatVersion()))
        .create();
  }

  private WriteResult write(Table table, List<RecordWrapper> records) throws IOException {
    return write(table, records, 128 * 1024 * 1024L, DeleteMode.POSITION);
  }

  private WriteResult write(Table table, List<RecordWrapper> records, DeleteMode mode) throws IOException {
    return write(table, records, 128 * 1024 * 1024L, mode);
  }

  private WriteResult write(Table table, List<RecordWrapper> records, long targetFileSize)
      throws IOException {
    return write(table, records, targetFileSize, DeleteMode.POSITION);
  }

  private WriteResult write(Table table, List<RecordWrapper> records, long targetFileSize, DeleteMode mode)
      throws IOException {
    // The production factory sets write.metadata.metrics.column.file_path=full, which
    // is what makes FILE-granularity delete files file-scoped. Using a bare
    // GenericAppenderFactory here would truncate the bounds and change how Iceberg
    // matches deletes to data files.
    GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(table);
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId.incrementAndGet(), 1L)
            .format(FileFormat.PARQUET)
            .build();

    PositionalDeleteSink sink = mode == DeleteMode.DELETION_VECTOR
        ? new PositionalDeleteSink.DeletionVectors(fileFactory, new PreviousDeleteLoader(table))
        : new PositionalDeleteSink.PositionalFiles(
            FileFormat.PARQUET, appenderFactory, fileFactory, DeleteGranularity.FILE);

    PositionalDeltaWriter writer = new PositionalDeltaWriter(
        table.spec(), FileFormat.PARQUET, appenderFactory, fileFactory, table.io(),
        targetFileSize, table.schema(), true, sink);

    for (RecordWrapper record : records) {
      writer.write(record);
    }
    return writer.complete();
  }

  private void commit(Table table, WriteResult result) {
    RowDelta rowDelta = table.newRowDelta();
    if (table.currentSnapshot() != null) {
      // Mirrors the production commit: conflicts are judged from the snapshot this
      // write was planned against. Without it Iceberg scans from the first snapshot
      // and reads the previous commit's own vector as a concurrent addition.
      rowDelta.validateFromSnapshot(table.currentSnapshot().snapshotId());
    }
    for (DataFile file : result.dataFiles()) {
      rowDelta.addRows(file);
    }
    for (DeleteFile file : result.deleteFiles()) {
      rowDelta.addDeletes(file);
    }
    // A data file may carry only one vector, so a replaced one has to be retired.
    for (DeleteFile file : result.rewrittenDeleteFiles()) {
      rowDelta.removeDeletes(file);
    }
    rowDelta.commit();
    table.refresh();
  }

  private Map<String, DataFile> filesByRegion(WriteResult result) {
    Map<String, DataFile> byRegion = new HashMap<>();
    for (DataFile file : result.dataFiles()) {
      byRegion.put(file.partition().get(0, String.class), file);
    }
    return byRegion;
  }

  private Set<String> liveIds(Table table) throws IOException {
    Set<String> ids = new HashSet<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        ids.add((String) row.getField("_olake_id"));
      }
    }
    return ids;
  }

  private int countRows(Table table) throws IOException {
    int count = 0;
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record ignored : rows) {
        count++;
      }
    }
    return count;
  }

  private int countById(Table table, String id) throws IOException {
    int count = 0;
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        if (id.equals(row.getField("_olake_id"))) {
          count++;
        }
      }
    }
    return count;
  }

  private String regionOf(Table table, String id) throws IOException {
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        if (id.equals(row.getField("_olake_id"))) {
          return (String) row.getField("region");
        }
      }
    }
    return null;
  }

  private String nameOf(Table table, String id) throws IOException {
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record row : rows) {
        if (id.equals(row.getField("_olake_id"))) {
          return (String) row.getField("name");
        }
      }
    }
    return null;
  }

  private RecordWrapper insert(String id, String name, String region) {
    return wrap(id, name, region, "c", Operation.CREATE, null, null);
  }

  private RecordWrapper update(String id, String name, String region, String deletePath, Long deletePos) {
    return wrap(id, name, region, "u", Operation.UPDATE, deletePath, deletePos);
  }

  private RecordWrapper wrap(String id, String name, String region, String opType,
      Operation op, String deletePath, Long deletePos) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("_olake_id", id);
    record.setField("name", name);
    record.setField("region", region);
    record.setField("_op_type", opType);
    RecordWrapper wrapped = new RecordWrapper(record, op, deletePath, deletePos);
    assertNotNull(wrapped.getField("_olake_id"));
    return wrapped;
  }
}
