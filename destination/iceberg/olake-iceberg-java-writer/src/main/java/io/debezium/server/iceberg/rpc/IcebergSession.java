package io.debezium.server.iceberg.rpc;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFileFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.tableoperator.ArrowDeletionVectorWriter;
import io.debezium.server.iceberg.tableoperator.DeleteMode;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;

public class IcebergSession {
    public final Table icebergTable;
    public final IcebergTableOperator op;
    public final OutputFileFactory fileFactory;
    public final String identifierField;
    public final boolean upsert;
    public final DeleteMode deleteMode;
    /**
     * Built lazily by the arrow path on the first DELETION_VECTORS batch and closed at
     * REGISTER_AND_COMMIT. Null in every other mode, and until the first batch arrives.
     */
    public ArrowDeletionVectorWriter dvWriter;

    public IcebergSession(Table icebergTable, boolean upsert, String identifierField, DeleteMode deleteMode) {
        this.icebergTable = icebergTable;
        this.op = new IcebergTableOperator(upsert, deleteMode);
        this.identifierField = identifierField;
        this.upsert = upsert;
        this.deleteMode = deleteMode;

        FileFormat fileFormat = IcebergUtil.getTableFileFormat(icebergTable);
        this.fileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, fileFormat);
    }

    public boolean createIdentifierFields() {
        return identifierField != null && !identifierField.isEmpty();
    }
}