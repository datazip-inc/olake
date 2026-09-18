package io.debezium.server.iceberg.rpc;

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload;
import io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest;
import io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse;
import io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch;
import io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest;
import io.debezium.server.iceberg.tableIndex.EqualityDeleteMigrator;
import io.debezium.server.iceberg.tableIndex.TableIndexScanner;
import io.debezium.server.iceberg.tableoperator.DeleteMode;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Serves the table index that the Go side keeps on local disk so it can express
 * deletes positionally.
 *
 * <p>Entries are streamed in batches rather than returned in one message because
 * a table can hold hundreds of millions of rows; batching keeps both sides at
 * flat memory. Like the arrow ingester, this service reuses the session created
 * by the GET_OR_CREATE_TABLE handshake instead of loading its own table handle.
 */
public class OlakeTableIndexer extends TableIndexServiceGrpc.TableIndexServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(OlakeTableIndexer.class);

  /** Entries per streamed message, a balance between round trips and message size. */
  private static final int BATCH_SIZE = 10_000;

  private final ConcurrentMap<String, IcebergSession> sessions;

  public OlakeTableIndexer(ConcurrentMap<String, IcebergSession> sessions) {
    this.sessions = sessions;
  }

  @Override
  public void scanTableForIndexing(TableIndexScanRequest request, StreamObserver<TableIndexScanBatch> responseObserver) {
    long startTime = System.currentTimeMillis();

    try {
      IcebergSession session = requireSession(request.getThreadId());
      Long fromSnapshotId = request.hasFromSnapshotId() ? request.getFromSnapshotId() : null;

      BatchEmitter emitter = new BatchEmitter(responseObserver);
      TableIndexScanner.ScanResult result = TableIndexScanner.scan(
          session.icebergTable, session.identifierField, fromSnapshotId, emitter);

      emitter.finish();

      responseObserver.onCompleted();
      LOGGER.info("streamed table index of {} entries for thread {} in {} ms",
          result.entries, request.getThreadId(), System.currentTimeMillis() - startTime);
    } catch (ScanAbandoned e) {
      LOGGER.warn("table index scan for thread {} abandoned by the caller after {} ms",
          request.getThreadId(), System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      String message = String.format("failed to scan table index for thread %s: %s",
          request.getThreadId(), e.getMessage());
      LOGGER.error(message, e);
      responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
    }
  }

  @Override
  public void migrateEqualityDeletes(MigrateEqualityDeletesRequest request,
      StreamObserver<MigrateEqualityDeletesResponse> responseObserver) {
    try {
      IcebergSession session = requireSession(request.getThreadId());
      // target_mode wins when the caller set it; UNSPECIFIED falls back to the
      // session's own mode, since deleteMode.addressesPositions() is what gated this RPC.
      IcebergPayload.DeleteMode requestedMode = request.getTargetMode();
      DeleteMode targetMode =
          requestedMode == IcebergPayload.DeleteMode.DELETE_MODE_UNSPECIFIED
              ? session.deleteMode
              : DeleteMode.resolve(requestedMode);
      EqualityDeleteMigrator.Result result = EqualityDeleteMigrator.migrate(
          session.icebergTable, session.identifierField, session.fileFactory, targetMode);

      responseObserver.onNext(MigrateEqualityDeletesResponse.newBuilder()
          .setRewrittenDeleteFiles(result.rewrittenDeleteFiles)
          .setPositionalDeletesWritten(result.positionalDeletesWritten)
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = String.format("failed to migrate equality deletes for thread %s: %s",
          request.getThreadId(), e.getMessage());
      LOGGER.error(message, e);
      responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
    }
  }

  private IcebergSession requireSession(String threadId) throws Exception {
    if (threadId == null || threadId.isEmpty()) {
      throw new Exception("Thread id not present in table index request");
    }

    IcebergSession session = sessions.get(threadId);
    if (session == null) {
      throw new Exception("No active session for thread " + threadId
          + "; GET_OR_CREATE_TABLE must be called before scanning the table index");
    }
    return session;
  }

  /** Raised to unwind a scan whose caller has gone away. */
  private static final class ScanAbandoned extends RuntimeException {
    private ScanAbandoned() {
      super("table index scan cancelled by the caller", null, false, false);
    }
  }

  /** Accumulates scan entries and ships them once a batch is full. */
  private static final class BatchEmitter implements TableIndexScanner.EntryConsumer {
    private static final long READY_WAIT_MILLIS = 500;
    private static final long STALL_REPORT_MILLIS = 30_000;

    private final StreamObserver<TableIndexScanBatch> responseObserver;
    private final ServerCallStreamObserver<TableIndexScanBatch> call;
    private final Object readyLock = new Object();
    private TableIndexScanBatch.Builder batch = TableIndexScanBatch.newBuilder();
    private long snapshotId;
    private boolean sentAnything;

    @SuppressWarnings("unchecked")
    private BatchEmitter(StreamObserver<TableIndexScanBatch> responseObserver) {
      this.responseObserver = responseObserver;
      this.call = responseObserver instanceof ServerCallStreamObserver
          ? (ServerCallStreamObserver<TableIndexScanBatch>) responseObserver
          : null;

      if (call != null) {
        call.setOnReadyHandler(this::wakeUp);
        call.setOnCancelHandler(this::wakeUp);
      }
    }

    @Override
    public void begin(long snapshotId) {
      this.snapshotId = snapshotId;
      batch.setSnapshotId(snapshotId);
    }

    @Override
    public void accept(String identifier, String filePath, long position) {
      batch.addEntries(TableIndexScanBatch.Entry.newBuilder()
          .setOlakeId(identifier)
          .setFilePath(filePath)
          .setPosition(position));

      if (batch.getEntriesCount() >= BATCH_SIZE) {
        send();
      }
    }

    /**
     * Ships the trailing partial batch. An empty table still sends one batch so
     * the caller always learns which snapshot to checkpoint against.
     */
    private void finish() {
      if (batch.getEntriesCount() > 0 || !sentAnything) {
        send();
      }
    }

    private void send() {
      awaitReady();
      responseObserver.onNext(batch.build());
      batch = TableIndexScanBatch.newBuilder().setSnapshotId(snapshotId);
      sentAnything = true;
    }

    private void awaitReady() {
      if (call == null) {
        return;
      }

      long waitedMillis = 0;
      synchronized (readyLock) {
        while (true) {
          if (call.isCancelled()) {
            throw new ScanAbandoned();
          }
          if (call.isReady()) {
            return;
          }
          try {
            readyLock.wait(READY_WAIT_MILLIS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ScanAbandoned();
          }

          waitedMillis += READY_WAIT_MILLIS;
          if (waitedMillis % STALL_REPORT_MILLIS == 0) {
            LOGGER.warn("table index scan has waited {} ms for the caller to consume; "
                + "still streaming, {} entries buffered", waitedMillis, batch.getEntriesCount());
          }
        }
      }
    }

    private void wakeUp() {
      synchronized (readyLock) {
        readyLock.notifyAll();
      }
    }
  }
}
