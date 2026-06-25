package io.olake.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ============================================
 * Catalog Service (Stateless)
 * ============================================
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: record_ingest.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class CatalogServiceGrpc {

  private CatalogServiceGrpc() {}

  public static final String SERVICE_NAME = "io.olake.server.iceberg.rpc.CatalogService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest,
      io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse> getCheckConnectionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CheckConnection",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest,
      io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse> getCheckConnectionMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest, io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse> getCheckConnectionMethod;
    if ((getCheckConnectionMethod = CatalogServiceGrpc.getCheckConnectionMethod) == null) {
      synchronized (CatalogServiceGrpc.class) {
        if ((getCheckConnectionMethod = CatalogServiceGrpc.getCheckConnectionMethod) == null) {
          CatalogServiceGrpc.getCheckConnectionMethod = getCheckConnectionMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest, io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CheckConnection"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CatalogServiceMethodDescriptorSupplier("CheckConnection"))
              .build();
        }
      }
    }
    return getCheckConnectionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest,
      io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse> getDropTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropTables",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest,
      io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse> getDropTablesMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest, io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse> getDropTablesMethod;
    if ((getDropTablesMethod = CatalogServiceGrpc.getDropTablesMethod) == null) {
      synchronized (CatalogServiceGrpc.class) {
        if ((getDropTablesMethod = CatalogServiceGrpc.getDropTablesMethod) == null) {
          CatalogServiceGrpc.getDropTablesMethod = getDropTablesMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest, io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CatalogServiceMethodDescriptorSupplier("DropTables"))
              .build();
        }
      }
    }
    return getDropTablesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CatalogServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CatalogServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CatalogServiceStub>() {
        @java.lang.Override
        public CatalogServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CatalogServiceStub(channel, callOptions);
        }
      };
    return CatalogServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CatalogServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CatalogServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CatalogServiceBlockingStub>() {
        @java.lang.Override
        public CatalogServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CatalogServiceBlockingStub(channel, callOptions);
        }
      };
    return CatalogServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CatalogServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<CatalogServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<CatalogServiceFutureStub>() {
        @java.lang.Override
        public CatalogServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new CatalogServiceFutureStub(channel, callOptions);
        }
      };
    return CatalogServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ============================================
   * Catalog Service (Stateless)
   * ============================================
   * </pre>
   */
  public static abstract class CatalogServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void checkConnection(io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCheckConnectionMethod(), responseObserver);
    }

    /**
     */
    public void dropTables(io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropTablesMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCheckConnectionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest,
                io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse>(
                  this, METHODID_CHECK_CONNECTION)))
          .addMethod(
            getDropTablesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest,
                io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse>(
                  this, METHODID_DROP_TABLES)))
          .build();
    }
  }

  /**
   * <pre>
   * ============================================
   * Catalog Service (Stateless)
   * ============================================
   * </pre>
   */
  public static final class CatalogServiceStub extends io.grpc.stub.AbstractAsyncStub<CatalogServiceStub> {
    private CatalogServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CatalogServiceStub(channel, callOptions);
    }

    /**
     */
    public void checkConnection(io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCheckConnectionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropTables(io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropTablesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * ============================================
   * Catalog Service (Stateless)
   * ============================================
   * </pre>
   */
  public static final class CatalogServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<CatalogServiceBlockingStub> {
    private CatalogServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CatalogServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse checkConnection(io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCheckConnectionMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse dropTables(io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropTablesMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * ============================================
   * Catalog Service (Stateless)
   * ============================================
   * </pre>
   */
  public static final class CatalogServiceFutureStub extends io.grpc.stub.AbstractFutureStub<CatalogServiceFutureStub> {
    private CatalogServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new CatalogServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse> checkConnection(
        io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCheckConnectionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse> dropTables(
        io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropTablesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CHECK_CONNECTION = 0;
  private static final int METHODID_DROP_TABLES = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CatalogServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CatalogServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CHECK_CONNECTION:
          serviceImpl.checkConnection((io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CheckConnectionResponse>) responseObserver);
          break;
        case METHODID_DROP_TABLES:
          serviceImpl.dropTables((io.olake.server.iceberg.rpc.RecordIngest.DropTablesRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.DropTablesResponse>) responseObserver);
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

  private static abstract class CatalogServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CatalogServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.olake.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CatalogService");
    }
  }

  private static final class CatalogServiceFileDescriptorSupplier
      extends CatalogServiceBaseDescriptorSupplier {
    CatalogServiceFileDescriptorSupplier() {}
  }

  private static final class CatalogServiceMethodDescriptorSupplier
      extends CatalogServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CatalogServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (CatalogServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CatalogServiceFileDescriptorSupplier())
              .addMethod(getCheckConnectionMethod())
              .addMethod(getDropTablesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
