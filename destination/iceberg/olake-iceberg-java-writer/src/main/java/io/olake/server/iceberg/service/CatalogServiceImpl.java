package io.olake.server.iceberg.service;

import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.olake.server.iceberg.util.IcebergUtil;
import io.olake.server.iceberg.middleware.GrpcMiddleware;
import io.olake.server.iceberg.rpc.CatalogServiceGrpc;
import io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest;
import io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse;
import io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest;
import io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse;
import io.grpc.stub.StreamObserver;

public class CatalogServiceImpl extends CatalogServiceGrpc.CatalogServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogServiceImpl.class);

    private final Catalog icebergCatalog;

    public CatalogServiceImpl(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public void checkConnection(CheckConnectionRequest request, StreamObserver<CheckConnectionResponse> responseObserver) {
        GrpcMiddleware.processStateless("Catalog", responseObserver, requestId -> {
            String namespace = request.getNamespace();
            String tableName = request.getTableName();
            if (namespace == null || namespace.isBlank() || tableName == null || tableName.isBlank()) {
                throw new Exception("namespace and table_name are required for connection check");
            }

            LOGGER.info("{} Running connection check for {}.{}", requestId, namespace, tableName);
            IcebergUtil.checkCatalogConnection(icebergCatalog, namespace, tableName);

            CheckConnectionResponse response = CheckConnectionResponse.newBuilder()
                    .setResult("Connection successful")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void dropTables(DropTablesRequest request, StreamObserver<DropTablesResponse> responseObserver) {
        GrpcMiddleware.processStateless("Catalog", responseObserver, requestId -> {
            int droppedCount = 0;
            for (String table : request.getTablesList()) {
                String[] parts = table.split("\\.", 2);
                if (parts.length == 2) {
                    try {
                        boolean dropped = IcebergUtil.dropIcebergTable(parts[0], parts[1], icebergCatalog);
                        if (dropped) droppedCount++;
                    } catch (Exception e) {
                        LOGGER.warn("{} Failed to drop table {}: {}", requestId, table, e.getMessage());
                    }
                }
            }
            String message = "Successfully dropped " + droppedCount + " tables";
            LOGGER.info("{} Dropped {} tables in bulk", requestId, droppedCount);
            
            DropTablesResponse response = DropTablesResponse.newBuilder().setResult(message).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }
}
