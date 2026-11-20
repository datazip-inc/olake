package io.olake.iceberg.rpc;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.iceberg.Table;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.olake.iceberg.rpc.RecordIngest.IcebergPayload;
import io.olake.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest;
import io.olake.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsBatch;
import io.olake.iceberg.rpc.RecordIngest.ReadRowsRequest;
import io.olake.iceberg.rpc.RecordIngest.TableIndexScanBatch;
import io.olake.iceberg.rpc.RecordIngest.TableIndexScanRequest;
import io.olake.iceberg.tableIndex.EqualityDeleteMigrator;
import io.olake.iceberg.tableIndex.RowReader;
import io.olake.iceberg.tableIndex.TableIndexScanner;
import io.olake.iceberg.tableoperator.DeleteMode;
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
 *
 * <p>It also reads rows back at the locations the index holds (ReadRows), to refill
 * values a CDC change could not carry.
 */
public class OlakeTableIndexer extends TableIndexServiceGrpc.TableIndexServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(OlakeTableIndexer.class);

  /** Entries per streamed message, a balance between round trips and message size. */
  private static final int BATCH_SIZE = 10_000;

  /** A ReadRows message is sent at 64 rows or 16 MB, whichever comes first. */
  private static final int READ_BATCH_ROWS = 64;
  private static final int READ_BATCH_BYTES = 16 * 1024 * 1024;

  /**
   * Bytes one ReadRows call sends before it stops, checked between rows: the row that passes it
   * is the last sent, whole. The caller's batch holds at most this much source data, so the batch
   * plus what it reads back stays under the 2 GB message limit when written.
   */
  private static final long READ_MAX_BYTES = 1024L * 1024 * 1024;

  /**
   * Runs ReadRows calls, one thread per call. Shared by every stream and bounded, so the pages
   * held by reads in flight stay capped however many streams sync.
   */
  private static final ExecutorService READERS = ThreadPools.newFixedThreadPool("olake-row-reader", 4);

  private final ConcurrentMap<String, IcebergSession> sessions;
  private final long readMaxBytes;

  public OlakeTableIndexer(ConcurrentMap<String, IcebergSession> sessions) {
    this(sessions, READ_MAX_BYTES);
  }

  /** Tests lower the read limit to a few rows. */
  OlakeTableIndexer(ConcurrentMap<String, IcebergSession> sessions, long readMaxBytes) {
    this.sessions = sessions;
    this.readMaxBytes = readMaxBytes;
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
    } catch (CallAbandoned e) {
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

  /**
   * Reads the rows on {@link #READERS} and returns at once: gRPC calls this call's onReady only
   * after this method returns, and a read waits for it when the caller falls behind. The caller
   * matches rows by file and position.
   */
  @Override
  public void readRows(ReadRowsRequest request, StreamObserver<ReadRowsBatch> responseObserver) {
    long startTime = System.currentTimeMillis();
    RowEmitter emitter = new RowEmitter(responseObserver, readMaxBytes);
    IcebergSession session;

    try {
      session = requireSession(request.getThreadId());
      // Rows written earlier in this sync may sit in a file the writer still has open.
      session.op.closeOpenDataFiles(request.getFilesList().stream()
          .map(ReadRowsRequest.FileRows::getFilePath).collect(Collectors.toSet()));
    } catch (Exception e) {
      failReadRows(request, responseObserver, e);
      return;
    }

    READERS.execute(() -> {
      try {
        emitter.read(session.icebergTable, request.getFilesList());
        emitter.finish();
        responseObserver.onCompleted();
        LOGGER.debug("read rows across {} file(s) for thread {} in {} ms",
            request.getFilesCount(), request.getThreadId(), System.currentTimeMillis() - startTime);
      } catch (CallAbandoned e) {
        // the caller cancels a read when its own side fails, which it reports itself
        LOGGER.debug("row read for thread {} abandoned by the caller after {} ms",
            request.getThreadId(), System.currentTimeMillis() - startTime);
      } catch (Throwable e) {
        failReadRows(request, responseObserver, e);
      }
    });
  }

  private static void failReadRows(ReadRowsRequest request, StreamObserver<ReadRowsBatch> responseObserver,
      Throwable failure) {
    String message = String.format("failed to read rows for thread %s: %s", request.getThreadId(), failure.getMessage());
    LOGGER.error(message, failure);
    responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
  }

  private IcebergSession requireSession(String threadId) throws Exception {
    if (threadId == null || threadId.isEmpty()) {
      throw new Exception("Thread id not present in table index request");
    }

    IcebergSession session = sessions.get(threadId);
    if (session == null) {
      throw new Exception("No active session for thread " + threadId
          + "; GET_OR_CREATE_TABLE must be called before using the table index");
    }
    return session;
  }

  /** Raised to unwind a streaming call that is over: its caller has gone away. */
  private static final class CallAbandoned extends RuntimeException {
    private CallAbandoned() {
      super("streaming call cancelled by the caller", null, false, false);
    }
  }

  /**
   * Sends the messages of one server stream, waiting while the caller is not ready to
   * take more so neither side buffers a whole table.
   */
  private static final class Outbound<T> {
    private static final long READY_WAIT_MILLIS = 500;
    private static final long STALL_REPORT_MILLIS = 30_000;

    private final StreamObserver<T> responseObserver;
    private final ServerCallStreamObserver<T> call;
    private final Object readyLock = new Object();

    private Outbound(StreamObserver<T> responseObserver) {
      this.responseObserver = responseObserver;
      this.call = responseObserver instanceof ServerCallStreamObserver<T> serverCall ? serverCall : null;

      if (call != null) {
        call.setOnReadyHandler(this::wakeUp);
        call.setOnCancelHandler(this::wakeUp);
      }
    }

    private void send(T message) {
      awaitReady();
      responseObserver.onNext(message);
    }

    private boolean cancelled() {
      return call != null && call.isCancelled();
    }

    private void awaitReady() {
      if (call == null) {
        return;
      }

      long waitedMillis = 0;
      synchronized (readyLock) {
        while (true) {
          if (call.isCancelled()) {
            throw new CallAbandoned();
          }
          if (call.isReady()) {
            return;
          }
          try {
            readyLock.wait(READY_WAIT_MILLIS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CallAbandoned();
          }

          waitedMillis += READY_WAIT_MILLIS;
          if (waitedMillis % STALL_REPORT_MILLIS == 0) {
            LOGGER.warn("stream has waited {} ms for the caller to consume; still streaming", waitedMillis);
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

  /** Accumulates scan entries and ships them once a batch is full. */
  private static final class BatchEmitter implements TableIndexScanner.EntryConsumer {
    private final Outbound<TableIndexScanBatch> outbound;
    private TableIndexScanBatch.Builder batch = TableIndexScanBatch.newBuilder();
    private long snapshotId;
    private boolean sentAnything;

    private BatchEmitter(StreamObserver<TableIndexScanBatch> responseObserver) {
      this.outbound = new Outbound<>(responseObserver);
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
      outbound.send(batch.build());
      batch = TableIndexScanBatch.newBuilder().setSnapshotId(snapshotId);
      sentAnything = true;
    }
  }

  /**
   * Groups read rows into messages of at most READ_BATCH_ROWS rows or READ_BATCH_BYTES, and ends
   * the read once the rows sent pass maxBytes: the row that passes it is the last one sent, whole.
   */
  private static final class RowEmitter {
    private final Outbound<ReadRowsBatch> outbound;
    private final long maxBytes;
    private ReadRowsBatch.Builder batch = ReadRowsBatch.newBuilder();
    private int batchBytes;
    private long sentBytes;
    private boolean full; // the rows sent passed maxBytes

    private RowEmitter(StreamObserver<ReadRowsBatch> responseObserver, long maxBytes) {
      this.outbound = new Outbound<>(responseObserver);
      this.maxBytes = maxBytes;
    }

    /** Reads the files' rows, a file at a time, until the rows sent pass maxBytes. */
    private void read(Table table, List<ReadRowsRequest.FileRows> files) throws IOException {
      for (ReadRowsRequest.FileRows file : files) {
        if (stop()) {
          return; // the rows left are read by a later call
        }
        try (RowReader reader = new RowReader(table, file.getFilePath(), file.getRowsList())) {
          reader.read(this::accept, this::stop);
        }
      }
    }

    /** Whether to stop reading: the rows sent passed maxBytes. Unwinds the read once the caller is gone. */
    private boolean stop() {
      if (outbound.cancelled()) {
        throw new CallAbandoned();
      }
      return full;
    }

    private void accept(ReadRowsBatch.Row row) {
      batch.addRows(row);
      batchBytes += row.getSerializedSize();
      full = sentBytes + batchBytes > maxBytes;
      if (full || batch.getRowsCount() >= READ_BATCH_ROWS || batchBytes >= READ_BATCH_BYTES) {
        send();
      }
    }

    private void finish() {
      if (batch.getRowsCount() > 0) {
        send();
      }
    }

    private void send() {
      outbound.send(batch.build());
      sentBytes += batchBytes;
      batch = ReadRowsBatch.newBuilder();
      batchBytes = 0;
    }
  }
}
