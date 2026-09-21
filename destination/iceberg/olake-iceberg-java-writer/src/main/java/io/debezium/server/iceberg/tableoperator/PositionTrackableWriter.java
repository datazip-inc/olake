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

    /**
     * Whether write(record) will append a data record for this row. A caller
     * recording where rows land must skip any row this returns false for: nothing
     * is appended, so the position it sampled belongs to the row that follows.
     */
    default boolean willWrite(Record record) {
        return true;
    }

    /**
     * Signals that every record of a batch has been written and its write runs are
     * about to be returned. Writers holding per-batch state may release it here.
     */
    default void batchCompleted() {
    }
}
