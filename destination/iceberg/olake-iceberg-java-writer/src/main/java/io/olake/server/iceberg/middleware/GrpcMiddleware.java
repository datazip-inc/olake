package io.olake.server.iceberg.middleware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.stub.StreamObserver;
import io.olake.server.iceberg.session.SessionManager;
import io.olake.server.iceberg.session.WriterSession;

public class GrpcMiddleware {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMiddleware.class);

    public interface Action {
        void execute(String requestId) throws Exception;
    }

    public interface SessionAction {
        void execute(String requestId, WriterSession session) throws Exception;
    }

    public static <Res> void processStateless(String component, StreamObserver<Res> responseObserver, Action action) {
        String requestId = String.format("[%s-%d-%d]", component, Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();
        try {
            action.execute(requestId);
            LOGGER.info("{} Total time taken: {} ms", requestId, (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            String errorMessage = String.format("%s Failed to process request: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(errorMessage).asRuntimeException());
        }
    }

    public static <Res> void processSession(
            String component,
            String threadId,
            StreamObserver<Res> responseObserver,
            SessionManager sessionManager,
            SessionAction action) {

        String requestId = String.format("[%s-%d-%d]", component, Thread.currentThread().getId(), System.nanoTime());
        long startTime = System.currentTimeMillis();

        try {
            if (threadId == null || threadId.isEmpty()) {
                throw new Exception("Thread id not present in request");
            }
            WriterSession session = sessionManager.getSession(threadId);
            action.execute(requestId, session);
            LOGGER.info("{} Total time taken: {} ms", requestId, (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            String errorMessage = String.format("%s Failed to process request: %s", requestId, e.getMessage());
            LOGGER.error(errorMessage, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(errorMessage).asRuntimeException());
        }
    }
}
