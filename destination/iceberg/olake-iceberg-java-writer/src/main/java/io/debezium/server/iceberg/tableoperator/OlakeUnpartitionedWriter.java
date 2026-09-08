package io.debezium.server.iceberg.tableoperator;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import java.io.IOException;

public class OlakeUnpartitionedWriter extends BaseTaskWriter<Record> implements PositionTrackableWriter {

    private final RollingFileWriter currentWriter;

    public OlakeUnpartitionedWriter(PartitionSpec spec,
                                    FileFormat format,
                                    FileAppenderFactory<Record> appenderFactory,
                                    OutputFileFactory fileFactory,
                                    FileIO io,
                                    long targetFileSize) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.currentWriter = new RollingFileWriter(null);
    }

    @Override
    public CharSequence currentPath(Record record) {
        return currentWriter.currentPath();
    }

    @Override
    public long currentRows(Record record) {
        return currentWriter.currentRows();
    }

    @Override
    public void write(Record record) throws IOException {
        currentWriter.write(record);
    }

    @Override
    public void close() throws IOException {
        currentWriter.close();
    }
}
