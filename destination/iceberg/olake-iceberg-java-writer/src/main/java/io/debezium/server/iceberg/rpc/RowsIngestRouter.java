package io.debezium.server.iceberg.rpc;

import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single registered gRPC service that multiplexes many writer threads onto one
 * Java process. Each unique thread_id gets its own {@link OlakeRowsIngester}
 * worker (kept in a thread-keyed map); the worker holds today's single-writer
 * state (its own Table, IcebergTableOperator with isolated writer/filesToCommit).
 *
 * The per-writer config (namespace / upsert / partition fields) that used to be
 * baked into process-launch config now arrives in the request metadata and is
 * read once when the worker is lazily created.
 */
@Dependent
public class RowsIngestRouter extends RecordIngestServiceGrpc.RecordIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowsIngestRouter.class);

    private final Catalog icebergCatalog;
    private final ConcurrentHashMap<String, OlakeRowsIngester> workers = new ConcurrentHashMap<>();

    public RowsIngestRouter(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public void sendRecords(IcebergPayload request, StreamObserver<RecordIngest.RecordIngestResponse> responseObserver) {
        IcebergPayload.Metadata metadata = request.getMetadata();
        String threadId = metadata.getThreadId();

        if (threadId == null || threadId.isEmpty()) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Thread id not present in metadata").asRuntimeException());
            return;
        }

        OlakeRowsIngester worker = workers.computeIfAbsent(threadId, tid -> {
            List<Map<String, String>> partitionTransforms = new ArrayList<>();
            for (IcebergPayload.PartitionField pf : metadata.getPartitionFieldsList()) {
                Map<String, String> entry = new HashMap<>();
                entry.put("field", pf.getField());
                entry.put("transform", pf.getTransform());
                partitionTransforms.add(entry);
            }
            LOGGER.info("Creating row writer worker for thread: {} (namespace={}, upsert={}, partitions={})",
                    tid, metadata.getNamespace(), metadata.getUpsert(), partitionTransforms.size());
            return new OlakeRowsIngester(metadata.getUpsert(), metadata.getNamespace(), icebergCatalog,
                    partitionTransforms);
        });

        worker.sendRecords(request, responseObserver);

        // After a COMMIT the worker's accumulated state is fully flushed; drop the
        // entry so the long-lived process does not retain per-thread state forever.
        if (request.getType() == IcebergPayload.PayloadType.COMMIT) {
            workers.remove(threadId);
            LOGGER.debug("Removed row writer worker for thread: {}", threadId);
        }
    }
}
