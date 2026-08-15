package io.debezium.server.iceberg.rpc;

import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.server.iceberg.rowindex.EqualityDeleteMigrator;
import io.debezium.server.iceberg.rowindex.TableRowIndexScanner;
import io.debezium.server.iceberg.tableoperator.DeleteMode;
import io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest;
import io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse;
import io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch;
import io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest;
import io.grpc.stub.StreamObserver;

/**
 * Serves the row index that the Go side keeps on local disk so it can express
 * deletes positionally.
 *
 * <p>Entries are streamed in batches rather than returned in one message because
 * a table can hold hundreds of millions of rows; batching keeps both sides at
 * flat memory. Like the arrow ingester, this service reuses the session created
 * by the GET_OR_CREATE_TABLE handshake instead of loading its own table handle.
 */
public class OlakeRowIndexer extends RowIndexServiceGrpc.RowIndexServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(OlakeRowIndexer.class);

  /** Entries per streamed message, a balance between round trips and message size. */
  private static final int BATCH_SIZE = 10_000;

  private final ConcurrentMap<String, IcebergSession> sessions;

  public OlakeRowIndexer(ConcurrentMap<String, IcebergSession> sessions) {
    this.sessions = sessions;
  }

  @Override
  public void scanRowIndex(RowIndexScanRequest request, StreamObserver<RowIndexScanBatch> responseObserver) {
    long startTime = System.currentTimeMillis();

    try {
      IcebergSession session = requireSession(request.getThreadId());
      Long fromSnapshotId = request.hasFromSnapshotId() ? request.getFromSnapshotId() : null;

      BatchEmitter emitter = new BatchEmitter(responseObserver);
      TableRowIndexScanner.ScanResult result = TableRowIndexScanner.scan(
          session.icebergTable, session.identifierField, fromSnapshotId, emitter);

      if (result.requiresFullScan) {
        // Tell the caller to discard its index; nothing useful was streamed.
        responseObserver.onNext(RowIndexScanBatch.newBuilder()
            .setSnapshotId(result.snapshotId)
            .setRequiresFullScan(true)
            .build());
      } else {
        emitter.finish();
      }

      responseObserver.onCompleted();
      LOGGER.info("streamed row index of {} entries for thread {} in {} ms",
          result.entries, request.getThreadId(), System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      String message = String.format("failed to scan row index for thread %s: %s",
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
      // Empty target means positional, which is what callers predating vectors expect.
      DeleteMode targetMode = DeleteMode.resolve(request.getTargetMode(), true);
      EqualityDeleteMigrator.Result result = EqualityDeleteMigrator.migrate(
          session.icebergTable, session.identifierField, session.fileFactory, targetMode);

      responseObserver.onNext(MigrateEqualityDeletesResponse.newBuilder()
          .setSnapshotId(result.snapshotId)
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
      throw new Exception("Thread id not present in row index request");
    }

    IcebergSession session = sessions.get(threadId);
    if (session == null) {
      throw new Exception("No active session for thread " + threadId
          + "; GET_OR_CREATE_TABLE must be called before scanning the row index");
    }
    return session;
  }

  /** Accumulates scan entries and ships them once a batch is full. */
  private static final class BatchEmitter implements TableRowIndexScanner.EntryConsumer {
    private final StreamObserver<RowIndexScanBatch> responseObserver;
    private RowIndexScanBatch.Builder batch = RowIndexScanBatch.newBuilder();
    private long snapshotId;
    private boolean sentAnything;

    private BatchEmitter(StreamObserver<RowIndexScanBatch> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void begin(long snapshotId) {
      this.snapshotId = snapshotId;
      batch.setSnapshotId(snapshotId);
    }

    @Override
    public void accept(String identifier, String filePath, long position, boolean deleted) {
      batch.addEntries(RowIndexScanBatch.Entry.newBuilder()
          .setOlakeId(identifier)
          .setFilePath(filePath)
          .setPosition(position)
          .setDeleted(deleted));

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
      responseObserver.onNext(batch.build());
      batch = RowIndexScanBatch.newBuilder().setSnapshotId(snapshotId);
      sentAnything = true;
    }
  }
}
