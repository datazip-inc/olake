package io.debezium.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * RowIndexService populates and maintains the caller's identifier -&gt; row location
 * index, which is what lets deletes be expressed as positional rather than
 * equality deletes.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.0)",
    comments = "Source: record_ingest.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class RowIndexServiceGrpc {

  private RowIndexServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "io.debezium.server.iceberg.rpc.RowIndexService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch> getScanRowIndexMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanRowIndex",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch> getScanRowIndexMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest, io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch> getScanRowIndexMethod;
    if ((getScanRowIndexMethod = RowIndexServiceGrpc.getScanRowIndexMethod) == null) {
      synchronized (RowIndexServiceGrpc.class) {
        if ((getScanRowIndexMethod = RowIndexServiceGrpc.getScanRowIndexMethod) == null) {
          RowIndexServiceGrpc.getScanRowIndexMethod = getScanRowIndexMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest, io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanRowIndex"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch.getDefaultInstance()))
              .setSchemaDescriptor(new RowIndexServiceMethodDescriptorSupplier("ScanRowIndex"))
              .build();
        }
      }
    }
    return getScanRowIndexMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> getMigrateEqualityDeletesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "MigrateEqualityDeletes",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> getMigrateEqualityDeletesMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest, io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> getMigrateEqualityDeletesMethod;
    if ((getMigrateEqualityDeletesMethod = RowIndexServiceGrpc.getMigrateEqualityDeletesMethod) == null) {
      synchronized (RowIndexServiceGrpc.class) {
        if ((getMigrateEqualityDeletesMethod = RowIndexServiceGrpc.getMigrateEqualityDeletesMethod) == null) {
          RowIndexServiceGrpc.getMigrateEqualityDeletesMethod = getMigrateEqualityDeletesMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest, io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MigrateEqualityDeletes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RowIndexServiceMethodDescriptorSupplier("MigrateEqualityDeletes"))
              .build();
        }
      }
    }
    return getMigrateEqualityDeletesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RowIndexServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RowIndexServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RowIndexServiceStub>() {
        @java.lang.Override
        public RowIndexServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RowIndexServiceStub(channel, callOptions);
        }
      };
    return RowIndexServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RowIndexServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RowIndexServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RowIndexServiceBlockingStub>() {
        @java.lang.Override
        public RowIndexServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RowIndexServiceBlockingStub(channel, callOptions);
        }
      };
    return RowIndexServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RowIndexServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RowIndexServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RowIndexServiceFutureStub>() {
        @java.lang.Override
        public RowIndexServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RowIndexServiceFutureStub(channel, callOptions);
        }
      };
    return RowIndexServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * RowIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public interface AsyncService {

    /**
     * <pre>
     * ScanRowIndex streams the identifier and location of live rows in the table.
     * </pre>
     */
    default void scanRowIndex(io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanRowIndexMethod(), responseObserver);
    }

    /**
     * <pre>
     * MigrateEqualityDeletes rewrites the table's existing equality delete files
     * into equivalent positional delete files in a single atomic commit.
     * </pre>
     */
    default void migrateEqualityDeletes(io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMigrateEqualityDeletesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RowIndexService.
   * <pre>
   * RowIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static abstract class RowIndexServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RowIndexServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RowIndexService.
   * <pre>
   * RowIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static final class RowIndexServiceStub
      extends io.grpc.stub.AbstractAsyncStub<RowIndexServiceStub> {
    private RowIndexServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RowIndexServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RowIndexServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ScanRowIndex streams the identifier and location of live rows in the table.
     * </pre>
     */
    public void scanRowIndex(io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getScanRowIndexMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * MigrateEqualityDeletes rewrites the table's existing equality delete files
     * into equivalent positional delete files in a single atomic commit.
     * </pre>
     */
    public void migrateEqualityDeletes(io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMigrateEqualityDeletesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RowIndexService.
   * <pre>
   * RowIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static final class RowIndexServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RowIndexServiceBlockingStub> {
    private RowIndexServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RowIndexServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RowIndexServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ScanRowIndex streams the identifier and location of live rows in the table.
     * </pre>
     */
    public java.util.Iterator<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch> scanRowIndex(
        io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getScanRowIndexMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * MigrateEqualityDeletes rewrites the table's existing equality delete files
     * into equivalent positional delete files in a single atomic commit.
     * </pre>
     */
    public io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse migrateEqualityDeletes(io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMigrateEqualityDeletesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RowIndexService.
   * <pre>
   * RowIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static final class RowIndexServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<RowIndexServiceFutureStub> {
    private RowIndexServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RowIndexServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RowIndexServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * MigrateEqualityDeletes rewrites the table's existing equality delete files
     * into equivalent positional delete files in a single atomic commit.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> migrateEqualityDeletes(
        io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMigrateEqualityDeletesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SCAN_ROW_INDEX = 0;
  private static final int METHODID_MIGRATE_EQUALITY_DELETES = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SCAN_ROW_INDEX:
          serviceImpl.scanRowIndex((io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch>) responseObserver);
          break;
        case METHODID_MIGRATE_EQUALITY_DELETES:
          serviceImpl.migrateEqualityDeletes((io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getScanRowIndexMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanRequest,
              io.debezium.server.iceberg.rpc.RecordIngest.RowIndexScanBatch>(
                service, METHODID_SCAN_ROW_INDEX)))
        .addMethod(
          getMigrateEqualityDeletesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest,
              io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse>(
                service, METHODID_MIGRATE_EQUALITY_DELETES)))
        .build();
  }

  private static abstract class RowIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RowIndexServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.debezium.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RowIndexService");
    }
  }

  private static final class RowIndexServiceFileDescriptorSupplier
      extends RowIndexServiceBaseDescriptorSupplier {
    RowIndexServiceFileDescriptorSupplier() {}
  }

  private static final class RowIndexServiceMethodDescriptorSupplier
      extends RowIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RowIndexServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RowIndexServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RowIndexServiceFileDescriptorSupplier())
              .addMethod(getScanRowIndexMethod())
              .addMethod(getMigrateEqualityDeletesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
