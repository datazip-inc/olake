package io.debezium.server.iceberg.tableoperator;

import java.io.IOException;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
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

  /**
   * Folds one batch of positions into the vectors being built.
   *
   * <p>TODO: stamp the vector with the REFERENCED data file's partition, not the one
   * being written. They differ only when an update moves a row between partitions.
   * Vectors match by path ({@code DeleteFileIndex.dvByPath}) so the delete still
   * applies, but the manifest partition is wrong. Matches the rows path for now; fix
   * both together.
   */
  public void add(ArrowPayload.DeletionVectorBatch batch, IcebergTableOperator operator) {
    PartitionSpec spec = table.spec();
    for (ArrowPayload.DeletionVectorBatch.Entry entry : batch.getEntriesList()) {
      String dataFilePath = entry.getDataFilePath();
      // Iceberg rejects a non-null partition on an unpartitioned spec, and vice versa.
      StructLike partition = spec.isUnpartitioned()
          ? null
          : operator.partitionDataFromTypedValues(spec, entry.getPartitionValuesList());

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
}
