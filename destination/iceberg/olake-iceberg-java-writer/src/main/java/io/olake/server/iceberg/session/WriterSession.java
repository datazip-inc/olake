package io.olake.server.iceberg.session;

import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFileFactory;

import io.olake.server.iceberg.util.IcebergUtil;
import io.olake.server.iceberg.writer.legacy.operator.IcebergTableOperator;
import io.olake.server.iceberg.rpc.RecordIngest.WriterMode;

public class WriterSession {
    public static final String OLAKE_ID_FIELD = "_olake_id";

    private final Table icebergTable;
    private final IcebergTableOperator op;
    private final boolean upsert;
    private final OutputFileFactory fileFactory;
    private final WriterMode mode;

    public WriterSession(Table icebergTable, boolean upsert, WriterMode mode) {
        this.icebergTable = icebergTable;
        this.upsert = upsert;
        this.op = new IcebergTableOperator(upsert);
        this.mode = mode;
        this.fileFactory = mode == WriterMode.ARROW
                ? IcebergUtil.getTableOutputFileFactory(icebergTable, IcebergUtil.getTableFileFormat(icebergTable))
                : null;
    }

    public Table getIcebergTable() {
        return icebergTable;
    }

    public IcebergTableOperator getOp() {
        return op;
    }

    public boolean isUpsert() {
        return upsert;
    }

    /** Returns _olake_id for upsert legacy writes; empty for append mode. */
    public String getIdentifierField() {
        if (!upsert) {
            return "";
        }
        return OLAKE_ID_FIELD;
    }

    public boolean isCreateIdentifierFields() {
        return mode == WriterMode.LEGACY && upsert;
    }

    public OutputFileFactory getFileFactory() {
        return fileFactory;
    }

    public WriterMode getMode() {
        return mode;
    }
}
