package io.debezium.server.iceberg.tableoperator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.SortingPositionOnlyDeleteWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.CharSequenceSet;

import com.google.common.collect.Maps;

/**
 * Where a writer sends "the row at this offset of this file is superseded".
 *
 * <p>The two representations differ in more than encoding. Positional delete files are
 * append-only: every commit adds another file addressing whatever it supersedes, and a
 * data file can accumulate many of them. A deletion vector is one bitmap per data file,
 * so a commit that deletes more rows from a file must publish a vector holding the
 * union of the old and new positions and retire the old one. That is why the deletion
 * vector implementation needs a loader for the file's existing deletes and reports
 * rewritten files, while the positional one does neither.
 */
interface PositionalDeleteSink extends Closeable {

  /** Marks one row superseded. Partition is null on an unpartitioned spec. */
  void delete(String path, long position, PartitionSpec spec, StructLike partition) throws IOException;

  /** Delete files produced, plus the data files they reference and any files they replace. */
  DeleteWriteResult result();

  /** Adds this sink's output to a write result under construction. */
  default void addTo(WriteResult.Builder builder) {
    DeleteWriteResult result = result();
    builder.addDeleteFiles(result.deleteFiles());
    builder.addReferencedDataFiles(result.referencedDataFiles());
    builder.addRewrittenDeleteFiles(result.rewrittenDeleteFiles());
  }

  /** Stand-in for a sink that never wrote anything. */
  DeleteWriteResult EMPTY = new DeleteWriteResult(List.of(), CharSequenceSet.empty(), List.of());

  /** Map key for a partition, since an unpartitioned spec routes on a null partition. */
  Object UNPARTITIONED = new Object();

  static Object partitionKey(StructLike partition) {
    return partition == null ? UNPARTITIONED : partition;
  }

  /**
   * Writes positional delete files, one per data file they reference.
   *
   * <p>Iceberg wants a delete file's positions sorted by path then offset, which CDC
   * order does not give us, so each partition gets a writer that buffers and sorts on
   * close. Keeping one delete file per referenced data file is what lets Iceberg treat
   * them as file-scoped and match them to data files by path rather than by partition.
   */
  final class PositionalFiles implements PositionalDeleteSink {
    private final FileFormat format;
    private final FileAppenderFactory<Record> appenderFactory;
    private final OutputFileFactory fileFactory;
    private final DeleteGranularity granularity;
    private final Map<Object, SortingPositionOnlyDeleteWriter<Record>> writers = Maps.newHashMap();
    private final PositionDelete<Record> positionDelete = PositionDelete.create();
    private DeleteWriteResult aggregated;

    PositionalFiles(FileFormat format,
                    FileAppenderFactory<Record> appenderFactory,
                    OutputFileFactory fileFactory,
                    DeleteGranularity granularity) {
      this.format = format;
      this.appenderFactory = appenderFactory;
      this.fileFactory = fileFactory;
      this.granularity = granularity;
    }

    @Override
    public void delete(String path, long position, PartitionSpec spec, StructLike partition) {
      writerFor(spec, partition).write(positionDelete.set(path, position, null));
    }

    private SortingPositionOnlyDeleteWriter<Record> writerFor(PartitionSpec spec, StructLike partition) {
      return writers.computeIfAbsent(
          partitionKey(partition),
          ignored -> new SortingPositionOnlyDeleteWriter<>(
              () -> appenderFactory.newPosDeleteWriter(
                  partition == null
                      ? fileFactory.newOutputFile()
                      : fileFactory.newOutputFile(spec, partition),
                  format, partition),
              granularity));
    }

    @Override
    public void close() throws IOException {
      IOException failure = null;
      for (SortingPositionOnlyDeleteWriter<Record> writer : writers.values()) {
        try {
          writer.close();
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }

      List<DeleteFile> files = new ArrayList<>();
      CharSequenceSet referenced = CharSequenceSet.empty();
      for (SortingPositionOnlyDeleteWriter<Record> writer : writers.values()) {
        DeleteWriteResult result = writer.result();
        files.addAll(result.deleteFiles());
        referenced.addAll(result.referencedDataFiles());
      }
      aggregated = new DeleteWriteResult(files, referenced, List.of());
    }

    @Override
    public DeleteWriteResult result() {
      return aggregated == null ? EMPTY : aggregated;
    }
  }

  /**
   * Writes v3 deletion vectors, one Puffin blob per data file.
   *
   * <p>Iceberg permits a single vector per data file, so a file that already has one
   * must be republished with the union of old and new positions and the old vector
   * retired. {@code previousDeletes} supplies those existing positions; the writer
   * reports the retired files through {@code rewrittenDeleteFiles}, which the commit
   * has to remove or the table ends up with two vectors for one data file.
   */
  final class DeletionVectors implements PositionalDeleteSink {
    private final DVFileWriter writer;
    private DeleteWriteResult result;

    DeletionVectors(OutputFileFactory fileFactory, Function<String, PositionDeleteIndex> previousDeletes) {
      this.writer = new BaseDVFileWriter(fileFactory, previousDeletes);
    }

    @Override
    public void delete(String path, long position, PartitionSpec spec, StructLike partition) {
      writer.delete(path, position, spec, partition);
    }

    @Override
    public void close() throws IOException {
      writer.close();
      result = writer.result();
    }

    @Override
    public DeleteWriteResult result() {
      return result == null ? EMPTY : result;
    }
  }
}
