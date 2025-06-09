package io.debezium.server.iceberg.tableoperator;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;

public class PartitionedAppendWriter extends PartitionedWriter<Record> {
  private final PartitionKey partitionKey;
  final InternalRecordWrapper wrapper;

  public PartitionedAppendWriter(PartitionSpec spec, FileFormat format,
                                 FileAppenderFactory<Record> appenderFactory,
                                 OutputFileFactory fileFactory, FileIO io, long targetFileSize,
                                 Schema schema) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.partitionKey = new PartitionKey(spec, schema);
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
  }

  @Override
  protected PartitionKey partition(Record row) {
    partitionKey.partition(wrapper.wrap(row));
    return partitionKey;
  }
}
