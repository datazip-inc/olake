package io.olake.iceberg.rpc;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest;
import io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse;
import io.olake.iceberg.rpc.ToastRead.ReadRowsBatch;
import io.olake.iceberg.rpc.ToastRead.ReadRowsRequest;
import io.olake.iceberg.tableIndex.RowReader;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Reads column values of stored rows by (data file, row position), for values Postgres left
 * out of an UPDATE. Uses the session created by GET_OR_CREATE_TABLE.
 */
public class OlakeToastReader extends ToastReadServiceGrpc.ToastReadServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(OlakeToastReader.class);

  /** Sent for a column the data file does not carry; must match Go's constants.UnavailableValue. */
  private static final String UNAVAILABLE_VALUE = "__olake_unavailable_value__";

  /** A reply message is sent at 64 rows or 16 MB, whichever comes first. */
  private static final int BATCH_ROWS = 64;
  private static final int BATCH_BYTES = 16 * 1024 * 1024;

  private final ConcurrentMap<String, IcebergSession> sessions;

  public OlakeToastReader(ConcurrentMap<String, IcebergSession> sessions) {
    this.sessions = sessions;
  }

  @Override
  public void readRows(ReadRowsRequest request, StreamObserver<ReadRowsBatch> responseObserver) {
    long startTime = System.currentTimeMillis();

    try {
      IcebergSession session = requireSession(request.getThreadId());
      List<String> columns = request.getColumnsList();
      BatchEmitter emitter = new BatchEmitter(responseObserver);
      long rows = 0;

      for (ReadRowsRequest.FileRows file : request.getFilesList()) {
        RowReader.read(session.icebergTable, file.getFilePath(), file.getPositionsList(), columns, emitter::accept);
        rows += file.getPositionsCount();
      }

      emitter.finish();
      responseObserver.onCompleted();
      LOGGER.info("read {} column(s) of up to {} row(s) across {} file(s) for thread {} in {} ms",
          columns.size(), rows, request.getFilesCount(), request.getThreadId(), System.currentTimeMillis() - startTime);
    } catch (Exception e) {
      String message = String.format("failed to read rows for thread %s: %s", request.getThreadId(), e.getMessage());
      LOGGER.error(message, e);
      responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
    }
  }

  @Override
  public void flushOpenFiles(FlushOpenFilesRequest request, StreamObserver<FlushOpenFilesResponse> responseObserver) {
    try {
      IcebergSession session = requireSession(request.getThreadId());
      // Same call schema evolution uses: files close and stay in this thread's commit,
      // so their rows can be read now.
      session.op.completeWriter();

      responseObserver.onNext(FlushOpenFilesResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      String message = String.format("failed to flush open files for thread %s: %s", request.getThreadId(), e.getMessage());
      LOGGER.error(message, e);
      responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(message).asRuntimeException());
    }
  }

  private IcebergSession requireSession(String threadId) throws Exception {
    if (threadId == null || threadId.isEmpty()) {
      throw new Exception("Thread id not present in toast read request");
    }

    IcebergSession session = sessions.get(threadId);
    if (session == null) {
      throw new Exception("No active session for thread " + threadId
          + "; GET_OR_CREATE_TABLE must be called before reading rows");
    }
    return session;
  }

  /** Groups rows into reply messages and waits while the client is not ready. */
  private static final class BatchEmitter {
    private static final long READY_WAIT_MILLIS = 500;

    private final ServerCallStreamObserver<ReadRowsBatch> call;
    private final Object readyLock = new Object();
    private ReadRowsBatch.Builder batch = ReadRowsBatch.newBuilder();
    private int batchBytes;

    private BatchEmitter(StreamObserver<ReadRowsBatch> responseObserver) {
      // a gRPC server always hands a server-call observer, which reports client readiness
      this.call = (ServerCallStreamObserver<ReadRowsBatch>) responseObserver;
      call.setOnReadyHandler(this::wakeUp);
      call.setOnCancelHandler(this::wakeUp);
    }

    private void accept(String filePath, RowReader.Row row) {
      ReadRowsBatch.Row.Builder builder = ReadRowsBatch.Row.newBuilder()
          .setFilePath(filePath)
          .setPosition(row.position);
      for (Object value : row.values) {
        ToastRead.ColumnValue columnValue = toColumnValue(value);
        batchBytes += columnValue.getSerializedSize();
        builder.addValues(columnValue);
      }

      batch.addRows(builder);
      if (batch.getRowsCount() >= BATCH_ROWS || batchBytes >= BATCH_BYTES) {
        send();
      }
    }

    private void finish() {
      if (batch.getRowsCount() > 0) {
        send();
      }
    }

    private void send() {
      awaitReady();
      call.onNext(batch.build());
      batch = ReadRowsBatch.newBuilder();
      batchBytes = 0;
    }

    private void awaitReady() {
      synchronized (readyLock) {
        while (!call.isReady()) {
          if (call.isCancelled()) {
            throw new IllegalStateException("row read cancelled by the caller");
          }
          try {
            readyLock.wait(READY_WAIT_MILLIS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("row read interrupted");
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

  /** An unset value means the stored column is NULL. */
  private static ToastRead.ColumnValue toColumnValue(Object value) {
    ToastRead.ColumnValue.Builder builder = ToastRead.ColumnValue.newBuilder();
    if (value == null) {
      return builder.build();
    }

    if (value == RowReader.ABSENT) {
      // Go keeps its placeholder instead of writing a NULL nobody stored.
      return builder.setStringValue(UNAVAILABLE_VALUE).build();
    }

    if (value instanceof String text) {
      return builder.setStringValue(text).build();
    } else if (value instanceof byte[] bytes) {
      return builder.setBytesValue(ByteString.copyFrom(bytes)).build();
    } else if (value instanceof Long number) {
      return builder.setLongValue(number).build();
    } else if (value instanceof Double number) {
      return builder.setDoubleValue(number).build();
    } else if (value instanceof Boolean flag) {
      return builder.setBoolValue(flag).build();
    }

    return builder.setStringValue(value.toString()).build();
  }
}
