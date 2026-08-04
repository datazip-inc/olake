package io.debezium.server.iceberg.tableoperator;

import org.apache.iceberg.data.Record;

/**
 * Interface that allows a TaskWriter to report the file path and row offset
 * where the next written record will land. Must be sampled BEFORE calling
 * write(record), because the counter increments and the file may roll
 * inside the write method.
 */
public interface PositionTrackableWriter {
    CharSequence currentPath(Record record);
    long currentRows(Record record);
}
