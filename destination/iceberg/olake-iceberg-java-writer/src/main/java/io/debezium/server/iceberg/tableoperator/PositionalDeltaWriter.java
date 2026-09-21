package io.debezium.server.iceberg.tableoperator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;

import com.google.common.collect.Maps;

/**
 * Writes rows as data records plus positional deletes, without touching equality
 * deletes at all.
 *
 * <p>Both halves are held directly: the data side is {@link BaseTaskWriter}'s own
 * {@code RollingFileWriter}, whose {@code currentPath()} / {@code currentRows()} are
 * public, and the delete side is a {@link SortingPositionOnlyDeleteWriter} per
 * partition. Nothing here reaches into Iceberg internals, which is what the previous
 * implementation needed reflection for: it extended {@code BaseEqualityDeltaWriter},
 * whose data and pos-delete writers are private.
 *
 * <p>A partition key is computed for every row, so an unpartitioned table simply has
 * one entry keyed on the empty struct and there is no separate code path for it. The
 * only place the distinction survives is the {@code null} partition Iceberg requires
 * when constructing writers against an unpartitioned spec.
 */
public class PositionalDeltaWriter extends BaseTaskWriter<Record> implements PositionTrackableWriter {

  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<Record> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final boolean keepDeletes;
  private final DeleteGranularity deleteGranularity;

  private final PartitionKey partitionKeyTemplate;
  private final InternalRecordWrapper wrapper;
  private final List<String> identifierFieldNames;

  private final Map<PartitionKey, RollingFileWriter> dataWriters = Maps.newHashMap();
  private final Map<PartitionKey, SortingPositionOnlyDeleteWriter<Record>> deleteWriters = Maps.newHashMap();
  private final PositionDelete<Record> positionDelete = PositionDelete.create();

  /**
   * Where each identifier written by this writer currently lives.
   *
   * <p>The caller's table index only learns positions once a batch has been answered, so
   * within a single batch it hands every update of the same key the same superseded
   * location. Without this map the second update would leave both new rows live.
   * Iceberg's equality delta writer solves the same problem with {@code insertedRowMap};
   * this keys on the identifier values instead of a copied {@code StructLike}, which for
   * OLake's single {@code _olake_id} column is one map entry per key rather than a
   * projected struct.
   *
   * <p>Cleared by {@link #batchCompleted()} rather than at commit, because the blind
   * spot lasts exactly one batch: once the caller has been told where these rows
   * landed, the next batch addresses them through the normal delete path. Holding them
   * for the whole writer session would grow the map to every row written before the
   * commit, most of which - a backfill writes each key once - can never be superseded.
   */
  private final Map<Object, PathOffset> insertedRows = Maps.newHashMap();

  // addToTablePerSchema samples currentPath() and currentRows() before calling
  // write(), so the same record is routed three times in a row. Caching on
  // reference identity collapses that back to one partition-key evaluation.
  private Record lastRouted;
  private RollingFileWriter lastDataWriter;

  PositionalDeltaWriter(PartitionSpec spec,
                        FileFormat format,
                        FileAppenderFactory<Record> appenderFactory,
                        OutputFileFactory fileFactory,
                        FileIO io,
                        long targetFileSize,
                        Schema schema,
                        boolean keepDeletes,
                        DeleteGranularity deleteGranularity) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.keepDeletes = keepDeletes;
    this.deleteGranularity = deleteGranularity;
    this.partitionKeyTemplate = new PartitionKey(spec, schema);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());

    this.identifierFieldNames = new ArrayList<>();
    for (Integer fieldId : schema.identifierFieldIds()) {
      Types.NestedField field = schema.findField(fieldId);
      if (field != null) {
        identifierFieldNames.add(field.name());
      }
    }
  }

  @Override
  public void write(Record row) throws IOException {
    RecordWrapper wrapped = (RecordWrapper) row;
    PartitionKey key = routeKey(row);

    if (wrapped.hasPositionalDelete()) {
      // NOTE: the delete is routed to the partition of the NEW record, because that is
      // all the caller supplies today. When a row's partition values change, the
      // superseded row lives in a different partition and Iceberg will not apply this
      // delete to it. Fixing that needs the old row's partition to travel with the row
      // index entry; the routing here is already per-partition, so only the key changes.
      deleteWriter(key).write(
          positionDelete.set(wrapped.deleteFilePath(), wrapped.deletePosition(), null));
    }

    Object identifier = identifierOf(row);

    if (!willWrite(row)) {
      // Hard delete: drop any version this writer already produced for the key, so the
      // positional delete above is not left racing a row we wrote ourselves.
      supersedePrevious(identifier);
      return;
    }

    RollingFileWriter dataWriter = dataWriter(row);

    if (identifier == null) {
      dataWriter.write(row);
      return;
    }

    // Resolving the partition's delete writer now, rather than copying a PartitionKey
    // into every entry, keeps this to one object per row written.
    PathOffset landing = new PathOffset(
        deleteWriter(key), dataWriter.currentPath().toString(), dataWriter.currentRows());
    dataWriter.write(row);

    PathOffset previous = insertedRows.put(identifier, landing);
    if (previous != null) {
      // Same key written twice by this writer: the earlier row is superseded.
      previous.deleteWriter.write(positionDelete.set(previous.path, previous.position, null));
    }
  }

  /**
   * Whether {@link #write} will append a data record for this row. Callers that record
   * where rows land must not count a row this returns false for: nothing is written, so
   * the position they sampled belongs to whichever row comes next.
   */
  @Override
  public boolean willWrite(Record record) {
    // Hard delete: the positional delete removes the row and no tombstone is kept.
    return keepDeletes || ((RecordWrapper) record).op() != Operation.DELETE;
  }

  /**
   * Releases the per-batch supersede state. Safe once the caller has been handed the
   * write runs for this batch: from then on its table index addresses these rows itself.
   */
  @Override
  public void batchCompleted() {
    insertedRows.clear();
  }

  @Override
  public CharSequence currentPath(Record record) {
    return dataWriter(record).currentPath();
  }

  @Override
  public long currentRows(Record record) {
    return dataWriter(record).currentRows();
  }

  private void supersedePrevious(Object identifier) throws IOException {
    if (identifier == null) {
      return;
    }
    PathOffset previous = insertedRows.remove(identifier);
    if (previous != null) {
      previous.deleteWriter.write(positionDelete.set(previous.path, previous.position, null));
    }
  }

  private Object identifierOf(Record row) {
    if (identifierFieldNames.isEmpty()) {
      return null;
    }
    if (identifierFieldNames.size() == 1) {
      return row.getField(identifierFieldNames.get(0));
    }
    List<Object> key = new ArrayList<>(identifierFieldNames.size());
    for (String name : identifierFieldNames) {
      key.add(row.getField(name));
    }
    return key;
  }

  private PartitionKey routeKey(Record row) {
    partitionKeyTemplate.partition(wrapper.wrap(row));
    return partitionKeyTemplate;
  }

  private RollingFileWriter dataWriter(Record row) {
    if (row == lastRouted) {
      return lastDataWriter;
    }

    RollingFileWriter writer = dataWriter(routeKey(row));
    lastRouted = row;
    lastDataWriter = writer;
    return writer;
  }

  private RollingFileWriter dataWriter(PartitionKey key) {
    RollingFileWriter writer = dataWriters.get(key);
    if (writer == null) {
      // the template is mutated on every route, so the map must own a copy
      PartitionKey copiedKey = key.copy();
      writer = new RollingFileWriter(partitionOrNull(copiedKey));
      dataWriters.put(copiedKey, writer);
    }
    return writer;
  }

  private SortingPositionOnlyDeleteWriter<Record> deleteWriter(PartitionKey key) {
    SortingPositionOnlyDeleteWriter<Record> writer = deleteWriters.get(key);
    if (writer == null) {
      PartitionKey copiedKey = key.copy();
      StructLike partition = partitionOrNull(copiedKey);
      writer = new SortingPositionOnlyDeleteWriter<>(
          () -> appenderFactory.newPosDeleteWriter(newDeleteOutputFile(partition), format, partition),
          deleteGranularity);
      deleteWriters.put(copiedKey, writer);
    }
    return writer;
  }

  private EncryptedOutputFile newDeleteOutputFile(StructLike partition) {
    return partition == null ? fileFactory.newOutputFile() : fileFactory.newOutputFile(spec, partition);
  }

  /** Iceberg rejects a non-null partition on an unpartitioned spec, and vice versa. */
  private StructLike partitionOrNull(PartitionKey key) {
    return spec.isUnpartitioned() ? null : key;
  }

  @Override
  public WriteResult complete() throws IOException {
    // super.complete() closes this writer, which flushes every delete writer, so the
    // results below are only readable afterwards.
    WriteResult dataResult = super.complete();

    WriteResult.Builder builder = WriteResult.builder()
        .addDataFiles(dataResult.dataFiles())
        .addDeleteFiles(dataResult.deleteFiles())
        .addReferencedDataFiles(dataResult.referencedDataFiles());

    for (SortingPositionOnlyDeleteWriter<Record> writer : deleteWriters.values()) {
      DeleteWriteResult result = writer.result();
      builder.addDeleteFiles(result.deleteFiles());
      builder.addReferencedDataFiles(result.referencedDataFiles());
    }

    return builder.build();
  }

  @Override
  public void close() throws IOException {
    lastRouted = null;
    lastDataWriter = null;
    insertedRows.clear();

    // RollingFileWriter inherits close() from a package-private base, so a method
    // reference to it cannot be linked from here; close them with a plain loop.
    IOException failure = null;
    for (RollingFileWriter writer : dataWriters.values()) {
      failure = closeQuietly(writer, failure);
    }
    dataWriters.clear();

    // Deliberately not cleared: complete() reads each writer's result after close.
    for (SortingPositionOnlyDeleteWriter<Record> writer : deleteWriters.values()) {
      failure = closeQuietly(writer, failure);
    }

    if (failure != null) {
      throw failure;
    }
  }

  /** Closes one writer, keeping the first failure so the rest still get closed. */
  private static IOException closeQuietly(Closeable writer, IOException failure) {
    try {
      writer.close();
      return failure;
    } catch (IOException e) {
      if (failure == null) {
        return e;
      }
      failure.addSuppressed(e);
      return failure;
    }
  }

  /** Where a row this writer produced landed. */
  private static final class PathOffset {
    private final SortingPositionOnlyDeleteWriter<Record> deleteWriter;
    private final String path;
    private final long position;

    private PathOffset(SortingPositionOnlyDeleteWriter<Record> deleteWriter, String path, long position) {
      this.deleteWriter = deleteWriter;
      this.path = path;
      this.position = position;
    }
  }
}
