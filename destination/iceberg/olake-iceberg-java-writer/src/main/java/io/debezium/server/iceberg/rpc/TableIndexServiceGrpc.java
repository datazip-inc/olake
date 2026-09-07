package io.debezium.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * TableIndexService populates and maintains the caller's identifier -&gt; row location
 * index, which is what lets deletes be expressed as positional rather than
 * equality deletes.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: record_ingest.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TableIndexServiceGrpc {

  private TableIndexServiceGrpc() {}

  public static final String SERVICE_NAME = "io.debezium.server.iceberg.rpc.TableIndexService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch> getScanTableForIndexingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanTableForIndexing",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch> getScanTableForIndexingMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest, io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch> getScanTableForIndexingMethod;
    if ((getScanTableForIndexingMethod = TableIndexServiceGrpc.getScanTableForIndexingMethod) == null) {
      synchronized (TableIndexServiceGrpc.class) {
        if ((getScanTableForIndexingMethod = TableIndexServiceGrpc.getScanTableForIndexingMethod) == null) {
          TableIndexServiceGrpc.getScanTableForIndexingMethod = getScanTableForIndexingMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest, io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanTableForIndexing"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch.getDefaultInstance()))
              .setSchemaDescriptor(new TableIndexServiceMethodDescriptorSupplier("ScanTableForIndexing"))
              .build();
        }
      }
    }
    return getScanTableForIndexingMethod;
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
    if ((getMigrateEqualityDeletesMethod = TableIndexServiceGrpc.getMigrateEqualityDeletesMethod) == null) {
      synchronized (TableIndexServiceGrpc.class) {
        if ((getMigrateEqualityDeletesMethod = TableIndexServiceGrpc.getMigrateEqualityDeletesMethod) == null) {
          TableIndexServiceGrpc.getMigrateEqualityDeletesMethod = getMigrateEqualityDeletesMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest, io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "MigrateEqualityDeletes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableIndexServiceMethodDescriptorSupplier("MigrateEqualityDeletes"))
              .build();
        }
      }
    }
    return getMigrateEqualityDeletesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TableIndexServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TableIndexServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TableIndexServiceStub>() {
        @java.lang.Override
        public TableIndexServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TableIndexServiceStub(channel, callOptions);
        }
      };
    return TableIndexServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TableIndexServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TableIndexServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TableIndexServiceBlockingStub>() {
        @java.lang.Override
        public TableIndexServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TableIndexServiceBlockingStub(channel, callOptions);
        }
      };
    return TableIndexServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TableIndexServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TableIndexServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TableIndexServiceFutureStub>() {
        @java.lang.Override
        public TableIndexServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TableIndexServiceFutureStub(channel, callOptions);
        }
      };
    return TableIndexServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * TableIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static abstract class TableIndexServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * ScanTableForIndexing streams the identifier and location of live rows in the table.
     * </pre>
     */
    public void scanTableForIndexing(io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanTableForIndexingMethod(), responseObserver);
    }

    /**
     * <pre>
     * MigrateEqualityDeletes rewrites the table's existing equality delete files
     * into equivalent positional delete files in a single atomic commit.
     * </pre>
     */
    public void migrateEqualityDeletes(io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMigrateEqualityDeletesMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getScanTableForIndexingMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest,
                io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch>(
                  this, METHODID_SCAN_TABLE_FOR_INDEXING)))
          .addMethod(
            getMigrateEqualityDeletesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesRequest,
                io.debezium.server.iceberg.rpc.RecordIngest.MigrateEqualityDeletesResponse>(
                  this, METHODID_MIGRATE_EQUALITY_DELETES)))
          .build();
    }
  }

  /**
   * <pre>
   * TableIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static final class TableIndexServiceStub extends io.grpc.stub.AbstractAsyncStub<TableIndexServiceStub> {
    private TableIndexServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TableIndexServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TableIndexServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ScanTableForIndexing streams the identifier and location of live rows in the table.
     * </pre>
     */
    public void scanTableForIndexing(io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getScanTableForIndexingMethod(), getCallOptions()), request, responseObserver);
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
   * <pre>
   * TableIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static final class TableIndexServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<TableIndexServiceBlockingStub> {
    private TableIndexServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TableIndexServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TableIndexServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ScanTableForIndexing streams the identifier and location of live rows in the table.
     * </pre>
     */
    public java.util.Iterator<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch> scanTableForIndexing(
        io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getScanTableForIndexingMethod(), getCallOptions(), request);
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
   * <pre>
   * TableIndexService populates and maintains the caller's identifier -&gt; row location
   * index, which is what lets deletes be expressed as positional rather than
   * equality deletes.
   * </pre>
   */
  public static final class TableIndexServiceFutureStub extends io.grpc.stub.AbstractFutureStub<TableIndexServiceFutureStub> {
    private TableIndexServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TableIndexServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TableIndexServiceFutureStub(channel, callOptions);
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

  private static final int METHODID_SCAN_TABLE_FOR_INDEXING = 0;
  private static final int METHODID_MIGRATE_EQUALITY_DELETES = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TableIndexServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TableIndexServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SCAN_TABLE_FOR_INDEXING:
          serviceImpl.scanTableForIndexing((io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanRequest) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.TableIndexScanBatch>) responseObserver);
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

  private static abstract class TableIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TableIndexServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.debezium.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TableIndexService");
    }
  }

  private static final class TableIndexServiceFileDescriptorSupplier
      extends TableIndexServiceBaseDescriptorSupplier {
    TableIndexServiceFileDescriptorSupplier() {}
  }

  private static final class TableIndexServiceMethodDescriptorSupplier
      extends TableIndexServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TableIndexServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (TableIndexServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TableIndexServiceFileDescriptorSupplier())
              .addMethod(getScanTableForIndexingMethod())
              .addMethod(getMigrateEqualityDeletesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
