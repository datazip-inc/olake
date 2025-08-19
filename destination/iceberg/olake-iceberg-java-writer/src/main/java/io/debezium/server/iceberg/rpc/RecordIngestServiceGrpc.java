package io.debezium.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class RecordIngestServiceGrpc {

  private RecordIngestServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "io.debezium.server.iceberg.rpc.RecordIngestService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload,
      io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse> getSendRecordsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendRecords",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload,
      io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse> getSendRecordsMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload, io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse> getSendRecordsMethod;
    if ((getSendRecordsMethod = RecordIngestServiceGrpc.getSendRecordsMethod) == null) {
      synchronized (RecordIngestServiceGrpc.class) {
        if ((getSendRecordsMethod = RecordIngestServiceGrpc.getSendRecordsMethod) == null) {
          RecordIngestServiceGrpc.getSendRecordsMethod = getSendRecordsMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload, io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendRecords"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse.getDefaultInstance()))
              .setSchemaDescriptor(new RecordIngestServiceMethodDescriptorSupplier("SendRecords"))
              .build();
        }
      }
    }
    return getSendRecordsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RecordIngestServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceStub>() {
        @java.lang.Override
        public RecordIngestServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RecordIngestServiceStub(channel, callOptions);
        }
      };
    return RecordIngestServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static RecordIngestServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceBlockingV2Stub>() {
        @java.lang.Override
        public RecordIngestServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RecordIngestServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return RecordIngestServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RecordIngestServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceBlockingStub>() {
        @java.lang.Override
        public RecordIngestServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RecordIngestServiceBlockingStub(channel, callOptions);
        }
      };
    return RecordIngestServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RecordIngestServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<RecordIngestServiceFutureStub>() {
        @java.lang.Override
        public RecordIngestServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new RecordIngestServiceFutureStub(channel, callOptions);
        }
      };
    return RecordIngestServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void sendRecords(io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendRecordsMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service RecordIngestService.
   */
  public static abstract class RecordIngestServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return RecordIngestServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service RecordIngestService.
   */
  public static final class RecordIngestServiceStub
      extends io.grpc.stub.AbstractAsyncStub<RecordIngestServiceStub> {
    private RecordIngestServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordIngestServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RecordIngestServiceStub(channel, callOptions);
    }

    /**
     */
    public void sendRecords(io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendRecordsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service RecordIngestService.
   */
  public static final class RecordIngestServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<RecordIngestServiceBlockingV2Stub> {
    private RecordIngestServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordIngestServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RecordIngestServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse sendRecords(io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getSendRecordsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service RecordIngestService.
   */
  public static final class RecordIngestServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<RecordIngestServiceBlockingStub> {
    private RecordIngestServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordIngestServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RecordIngestServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse sendRecords(io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendRecordsMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service RecordIngestService.
   */
  public static final class RecordIngestServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<RecordIngestServiceFutureStub> {
    private RecordIngestServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RecordIngestServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new RecordIngestServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse> sendRecords(
        io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendRecordsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_RECORDS = 0;

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
        case METHODID_SEND_RECORDS:
          serviceImpl.sendRecords((io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse>) responseObserver);
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
          getSendRecordsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              io.debezium.server.iceberg.rpc.RecordIngest.IcebergPayload,
              io.debezium.server.iceberg.rpc.RecordIngest.RecordIngestResponse>(
                service, METHODID_SEND_RECORDS)))
        .build();
  }

  private static abstract class RecordIngestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RecordIngestServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.debezium.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("RecordIngestService");
    }
  }

  private static final class RecordIngestServiceFileDescriptorSupplier
      extends RecordIngestServiceBaseDescriptorSupplier {
    RecordIngestServiceFileDescriptorSupplier() {}
  }

  private static final class RecordIngestServiceMethodDescriptorSupplier
      extends RecordIngestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    RecordIngestServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (RecordIngestServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RecordIngestServiceFileDescriptorSupplier())
              .addMethod(getSendRecordsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
