package io.debezium.server.iceberg.rpc;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFileFactory;

import io.debezium.server.iceberg.IcebergUtil;
import io.debezium.server.iceberg.tableoperator.IcebergTableOperator;

public class IcebergSession {
    public final Table icebergTable;
    public final IcebergTableOperator op;
    public final OutputFileFactory fileFactory;
    public final String identifierField;
    public final boolean upsert;

    public IcebergSession(Table icebergTable, boolean upsert, String identifierField) {
        this.icebergTable = icebergTable;
        this.op = new IcebergTableOperator(upsert);
        this.identifierField = identifierField;
        this.upsert = upsert;
        
        FileFormat fileFormat = IcebergUtil.getTableFileFormat(icebergTable);
        this.fileFactory = IcebergUtil.getTableOutputFileFactory(icebergTable, fileFormat);
    }

    public boolean createIdentifierFields() {
        return identifierField != null && !identifierField.isEmpty();
    }
}