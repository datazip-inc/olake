package io.debezium.server.iceberg.tableoperator;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

public class OlakePartitionedFanoutWriter extends BaseTaskWriter<Record> implements PositionTrackableWriter {

    private final Map<PartitionKey, RollingFileWriter> writers = Maps.newHashMap();
    private final PartitionKey partitionKeyTemplate;
    private final InternalRecordWrapper wrapper;

    public OlakePartitionedFanoutWriter(PartitionSpec spec,
                                        FileFormat format,
                                        FileAppenderFactory<Record> appenderFactory,
                                        OutputFileFactory fileFactory,
                                        FileIO io,
                                        long targetFileSize,
                                        Schema schema) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.partitionKeyTemplate = new PartitionKey(spec, schema);
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
    }

    private PartitionKey partition(Record row) {
        partitionKeyTemplate.partition(wrapper.wrap(row));
        return partitionKeyTemplate;
    }

    private RollingFileWriter route(Record row) {
        PartitionKey partitionKey = partition(row);

        RollingFileWriter writer = writers.get(partitionKey);
        if (writer == null) {
            PartitionKey copiedKey = partitionKey.copy();
            writer = new RollingFileWriter(copiedKey);
            writers.put(copiedKey, writer);
        }
        return writer;
    }

    @Override
    public CharSequence currentPath(Record record) {
        return route(record).currentPath();
    }

    @Override
    public long currentRows(Record record) {
        return route(record).currentRows();
    }

    @Override
    public void write(Record row) throws IOException {
        route(row).write(row);
    }

    @Override
    public void close() throws IOException {
        if (!writers.isEmpty()) {
            for (PartitionKey key : writers.keySet()) {
                writers.get(key).close();
            }
            writers.clear();
        }
    }
}
