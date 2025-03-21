package io.debezium.server.iceberg.tableoperator;

import io.debezium.server.iceberg.IcebergUtil;
import jakarta.enterprise.context.Dependent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

/**
 * Iceberg Table Writer Factory to get TaskWriter for the table. upsert modes used to return correct writer.
 */
@Dependent
public class IcebergTableWriterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableWriterFactory.class);
  @ConfigProperty(name = "debezium.sink.iceberg.upsert", defaultValue = "true")
  boolean upsert;
  @ConfigProperty(name = "debezium.sink.iceberg.upsert-keep-deletes", defaultValue = "true")
  boolean keepDeletes;

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
      return new UnpartitionedWriter<>(
          icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), targetFileSize);

    } else {
      // table is partitioned use partitioned append writer
      return new PartitionedAppendWriter(
          icebergTable.spec(), format, appenderFactory, fileFactory, icebergTable.io(), targetFileSize, icebergTable.schema());
    }
  }

  private BaseTaskWriter<Record> deltaWriter(Table icebergTable, FileFormat format, GenericAppenderFactory appenderFactory, OutputFileFactory fileFactory, long targetFileSize) {

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
}
