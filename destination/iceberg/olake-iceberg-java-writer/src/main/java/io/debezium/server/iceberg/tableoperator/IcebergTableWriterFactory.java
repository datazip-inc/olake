package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;

import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

/**
 * Iceberg Table Writer Factory to get TaskWriter for the table. upsert modes used to return correct writer.
 *
 * Fields are plain (no @ConfigProperty) because the shared JVM hosts many operators
 * with possibly different upsert flags; each operator sets these explicitly.
 */
public class IcebergTableWriterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableWriterFactory.class);
  public boolean upsert = true;
  public boolean keepDeletes = true;
  public DeleteMode deleteMode = DeleteMode.EQUALITY;

  // One positional delete file per referenced data file. Matches the granularity the
  // equality path has always used. PARTITION trades reader-side skipping for far fewer
  // delete files, which matters once deletes can reference arbitrary historical files.
  private static final DeleteGranularity DELETE_GRANULARITY = DeleteGranularity.PARTITION;

  public BaseTaskWriter<Record> create(Table icebergTable) {

    // file format of the table parquet, orc ...
    FileFormat format = IcebergUtil.getTableFileFormat(icebergTable);
    // appender factory
    GenericAppenderFactory appenderFactory = IcebergUtil.getTableAppender(icebergTable);
    OutputFileFactory fileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, format);
    // equality Field Ids
    long targetFileSize =
        PropertyUtil.propertyAsLong(
            icebergTable.properties(), WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    if (!upsert) {
      // RUNNING APPEND MODE
      return appendWriter(icebergTable, format, appenderFactory, fileFactory, targetFileSize);
    } else if (icebergTable.schema().identifierFieldIds().isEmpty()) {
      // ITS UPSERT MODE BUT!!!!! TABLE DON'T HAVE identifierFieldIds(Primary Key)
      if (upsert) {
        LOGGER.info("Table don't have Pk defined upsert is not possible falling back to append!");
      }
      return appendWriter(icebergTable, format, appenderFactory, fileFactory, targetFileSize);
    } else {
      // ITS UPSERT MODE AND TABLE HAS identifierFieldIds(Primary Key)
      // USE DELTA WRITERS
      return deltaWriter(icebergTable, format, appenderFactory, fileFactory, targetFileSize);
    }
  }

  private BaseTaskWriter<Record> appendWriter(Table icebergTable, FileFormat format, GenericAppenderFactory appenderFactory, OutputFileFactory fileFactory, long targetFileSize) {

    if (icebergTable.spec().isUnpartitioned()) {
      // table is un partitioned use un partitioned append writer
      return new OlakeUnpartitionedWriter(
          icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), targetFileSize);

    } else {
        return new OlakePartitionedFanoutWriter(
          icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), targetFileSize, icebergTable.schema());
    }
  }

  private BaseTaskWriter<Record> deltaWriter(Table icebergTable, FileFormat format, GenericAppenderFactory appenderFactory, OutputFileFactory fileFactory, long targetFileSize) {

    if (deleteMode.addressesPositions()) {
      // One writer for both layouts: an unpartitioned table is a single entry keyed
      // on the empty partition struct, so there is no partitioned/unpartitioned split.
      // pos vs dv is entirely the sink's concern from here - the writer never branches.
      return new PositionalDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          targetFileSize, icebergTable.schema(), keepDeletes,
          deleteSink(icebergTable, format, appenderFactory, fileFactory));
    }

    Set<Integer> identifierFieldIds = icebergTable.schema().identifierFieldIds();
    if (icebergTable.spec().isUnpartitioned()) {
      // running with upsert mode + un partitioned table
      return new UnpartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          targetFileSize, icebergTable.schema(), identifierFieldIds, keepDeletes);
    } else {
      // running with upsert mode + partitioned table
      return new PartitionedDeltaWriter(icebergTable.spec(), format, appenderFactory, fileFactory,
          icebergTable.io(),
          targetFileSize, icebergTable.schema(), identifierFieldIds, keepDeletes);
    }
  }

  private PositionalDeleteSink deleteSink(Table icebergTable, FileFormat format,
      GenericAppenderFactory appenderFactory, OutputFileFactory fileFactory) {
    if (deleteMode == DeleteMode.DELETION_VECTOR) {
      // Deletion vectors are Puffin, not the table's data format. OutputFileFactory
      // takes its file extension from the format it was built with, so reusing
      // fileFactory here would name Puffin blobs ".parquet".
      OutputFileFactory dvFileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, FileFormat.PUFFIN);
      // A vector replaces the data file's previous one, so it has to be seeded with
      // the positions already deleted or this commit would resurrect them.
      return new PositionalDeleteSink.DeletionVectors(
          dvFileFactory, new io.debezium.server.iceberg.tableIndex.PreviousDeleteLoader(icebergTable));
    }
    return new PositionalDeleteSink.PositionalFiles(format, appenderFactory, fileFactory, DELETE_GRANULARITY);
  }

}
