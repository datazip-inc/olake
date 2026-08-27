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
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;

import com.google.common.collect.Maps;

/**
 * Writes rows as data records plus positional deletes, without touching equality
 * deletes at all. Serves both {@link DeleteMode#POSITION} and
 * {@link DeleteMode#DELETION_VECTOR} - they differ only in how {@link #deleteSink}
 * encodes a superseded position, never in how this class decides which position to
 * supersede.
 *
 * <p>The data side is {@link BaseTaskWriter}'s own {@code RollingFileWriter}, whose
 * {@code currentPath()} / {@code currentRows()} are public. Nothing here reaches into
 * Iceberg internals, which is what the reflection-based implementation this replaced
 * needed: it extended {@code BaseEqualityDeltaWriter}, whose data and pos-delete
 * writers are private.
 *
 * <p>A partition key is computed for every row, so an unpartitioned table simply has
 * one entry keyed on the empty struct and there is no separate code path for it. The
 * only place the distinction survives is the {@code null} partition Iceberg requires
 * when constructing writers against an unpartitioned spec.
 */
public class PositionalDeltaWriter extends BaseTaskWriter<Record> implements PositionTrackableWriter {

  private final PartitionSpec spec;
  private final boolean keepDeletes;
  private final PositionalDeleteSink deleteSink;

  private final PartitionKey partitionKeyTemplate;
  private final InternalRecordWrapper wrapper;
  private final List<String> identifierFieldNames;

  private final Map<PartitionKey, RollingFileWriter> dataWriters = Maps.newHashMap();

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
                        PositionalDeleteSink deleteSink) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.spec = spec;
    this.keepDeletes = keepDeletes;
    this.deleteSink = deleteSink;
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
    // Transient use only - the sink's own routing (per-partition map, or nothing at
    // all for a shared deletion-vector writer) copies this if it needs to keep it.
    StructLike partition = partitionOrNull(key);

    if (wrapped.hasPositionalDelete()) {
      // TODO: routed to the NEW record's partition - the caller's index carries no
      // partition for the old row. Wrong when an update moves a row between partitions;
      // see the TODO on ArrowDeletionVectorWriter.add.
      deleteSink.delete(wrapped.deleteFilePath(), wrapped.deletePosition(), spec, partition);
    }

    Object identifier = identifierOf(row);

    if (!willWrite(row)) {
      // Hard delete: drop any version this writer already produced for the key, so the
      // positional delete above is not left racing a row we wrote ourselves.
      supersedePrevious(identifier);
      return;
    }

    RollingFileWriter dataWriter = dataWriterFor(key);
    lastRouted = row;
    lastDataWriter = dataWriter;

    if (identifier == null) {
      dataWriter.write(row);
      return;
    }

    // copiedPartitionOrNull(): PathOffset outlives this call (held in insertedRows
    // until the batch completes), so it needs a stable copy, not the mutable template.
    PathOffset landing = new PathOffset(
        copiedPartitionOrNull(key), dataWriter.currentPath().toString(), dataWriter.currentRows());
    dataWriter.write(row);

    PathOffset previous = insertedRows.put(identifier, landing);
    if (previous != null) {
      // Same key written twice by this writer: the earlier row is superseded.
      deleteSink.delete(previous.path, previous.position, spec, previous.partition);
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
      deleteSink.delete(previous.path, previous.position, spec, previous.partition);
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

  /**
   * currentPath()/currentRows() are sampled by the caller before write() is called for
   * the same record, so this is invoked three times in a row for one row. Caching on
   * record identity collapses that back to one partition-key evaluation and one map
   * lookup; write() itself only runs once per record, so it calls
   * {@link #dataWriterFor(PartitionKey)} directly instead.
   */
  private RollingFileWriter dataWriter(Record row) {
    if (row == lastRouted) {
      return lastDataWriter;
    }

    RollingFileWriter writer = dataWriterFor(routeKey(row));
    lastRouted = row;
    lastDataWriter = writer;
    return writer;
  }

  private RollingFileWriter dataWriterFor(PartitionKey key) {
    RollingFileWriter writer = dataWriters.get(key);
    if (writer == null) {
      // the template is mutated on every route, so the map must own a copy
      PartitionKey copiedKey = key.copy();
      writer = new RollingFileWriter(partitionOrNull(copiedKey));
      dataWriters.put(copiedKey, writer);
    }
    return writer;
  }

  /** Iceberg rejects a non-null partition on an unpartitioned spec, and vice versa. */
  private StructLike partitionOrNull(PartitionKey key) {
    return spec.isUnpartitioned() ? null : key;
  }

  /** Same as {@link #partitionOrNull}, but a stable copy safe to hold beyond this call. */
  private StructLike copiedPartitionOrNull(PartitionKey key) {
    return spec.isUnpartitioned() ? null : key.copy();
  }

  @Override
  public WriteResult complete() throws IOException {
    // super.complete() closes this writer, which flushes the delete sink, so its
    // result is only readable afterwards.
    WriteResult dataResult = super.complete();

    WriteResult.Builder builder = WriteResult.builder()
        .addDataFiles(dataResult.dataFiles())
        .addDeleteFiles(dataResult.deleteFiles())
        .addReferencedDataFiles(dataResult.referencedDataFiles());

    deleteSink.addTo(builder);

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

    // Closed last, and its result is not discarded: complete() reads it right after.
    failure = closeQuietly(deleteSink, failure);

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
    private final StructLike partition;
    private final String path;
    private final long position;

    private PathOffset(StructLike partition, String path, long position) {
      this.partition = partition;
      this.path = path;
      this.position = position;
    }
  }
}
