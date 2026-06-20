package io.debezium.server.iceberg.rpc;

import io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.Dependent;

import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Single registered gRPC service that multiplexes arrow-mode writer threads onto
 * one Java process. Each unique thread_id gets its own {@link OlakeArrowIngester}
 * worker holding today's single-writer state (its own Table, IcebergTableOperator,
 * OutputFileFactory). Per-writer config (namespace / upsert) is read from request
 * metadata when the worker is lazily created.
 */
@Dependent
public class ArrowIngestRouter extends ArrowIngestServiceGrpc.ArrowIngestServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowIngestRouter.class);

    private final Catalog icebergCatalog;
    private final ConcurrentHashMap<String, OlakeArrowIngester> workers = new ConcurrentHashMap<>();

    public ArrowIngestRouter(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public void icebergAPI(ArrowPayload request, StreamObserver<RecordIngest.ArrowIngestResponse> responseObserver) {
        ArrowPayload.Metadata metadata = request.getMetadata();
        String threadId = metadata.getThreadId();

        if (threadId == null || threadId.isEmpty()) {
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Thread id not present in metadata").asRuntimeException());
            return;
        }

        OlakeArrowIngester worker = workers.computeIfAbsent(threadId, tid -> {
            LOGGER.info("Creating arrow writer worker for thread: {} (namespace={}, upsert={})",
                    tid, metadata.getNamespace(), metadata.getUpsert());
            return new OlakeArrowIngester(metadata.getUpsert(), metadata.getNamespace(), icebergCatalog);
        });

        worker.icebergAPI(request, responseObserver);

        // REGISTER_AND_COMMIT is the terminal call for a thread; release its state.
        if (request.getType() == ArrowPayload.PayloadType.REGISTER_AND_COMMIT) {
            workers.remove(threadId);
            LOGGER.debug("Removed arrow writer worker for thread: {}", threadId);
        }
    }
}
