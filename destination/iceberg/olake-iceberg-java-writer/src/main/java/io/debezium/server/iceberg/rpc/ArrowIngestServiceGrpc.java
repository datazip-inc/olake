package io.debezium.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: record_ingest.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ArrowIngestServiceGrpc {

  private ArrowIngestServiceGrpc() {}

  public static final String SERVICE_NAME = "io.debezium.server.iceberg.rpc.ArrowIngestService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload,
      io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse> getIcebergAPIMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "IcebergAPI",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload,
      io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse> getIcebergAPIMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload, io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse> getIcebergAPIMethod;
    if ((getIcebergAPIMethod = ArrowIngestServiceGrpc.getIcebergAPIMethod) == null) {
      synchronized (ArrowIngestServiceGrpc.class) {
        if ((getIcebergAPIMethod = ArrowIngestServiceGrpc.getIcebergAPIMethod) == null) {
          ArrowIngestServiceGrpc.getIcebergAPIMethod = getIcebergAPIMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload, io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "IcebergAPI"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ArrowIngestServiceMethodDescriptorSupplier("IcebergAPI"))
              .build();
        }
      }
    }
    return getIcebergAPIMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ArrowIngestServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArrowIngestServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArrowIngestServiceStub>() {
        @java.lang.Override
        public ArrowIngestServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArrowIngestServiceStub(channel, callOptions);
        }
      };
    return ArrowIngestServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ArrowIngestServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArrowIngestServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArrowIngestServiceBlockingStub>() {
        @java.lang.Override
        public ArrowIngestServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArrowIngestServiceBlockingStub(channel, callOptions);
        }
      };
    return ArrowIngestServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ArrowIngestServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArrowIngestServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArrowIngestServiceFutureStub>() {
        @java.lang.Override
        public ArrowIngestServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArrowIngestServiceFutureStub(channel, callOptions);
        }
      };
    return ArrowIngestServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class ArrowIngestServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void icebergAPI(io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getIcebergAPIMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getIcebergAPIMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload,
                io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse>(
                  this, METHODID_ICEBERG_API)))
          .build();
    }
  }

  /**
   */
  public static final class ArrowIngestServiceStub extends io.grpc.stub.AbstractAsyncStub<ArrowIngestServiceStub> {
    private ArrowIngestServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArrowIngestServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArrowIngestServiceStub(channel, callOptions);
    }

    /**
     */
    public void icebergAPI(io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getIcebergAPIMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ArrowIngestServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ArrowIngestServiceBlockingStub> {
    private ArrowIngestServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArrowIngestServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArrowIngestServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse icebergAPI(io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getIcebergAPIMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ArrowIngestServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ArrowIngestServiceFutureStub> {
    private ArrowIngestServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArrowIngestServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArrowIngestServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse> icebergAPI(
        io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getIcebergAPIMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_ICEBERG_API = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ArrowIngestServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ArrowIngestServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_ICEBERG_API:
          serviceImpl.icebergAPI((io.debezium.server.iceberg.rpc.RecordIngest.ArrowPayload) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowIngestResponse>) responseObserver);
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

  private static abstract class ArrowIngestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ArrowIngestServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.debezium.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ArrowIngestService");
    }
  }

  private static final class ArrowIngestServiceFileDescriptorSupplier
      extends ArrowIngestServiceBaseDescriptorSupplier {
    ArrowIngestServiceFileDescriptorSupplier() {}
  }

  private static final class ArrowIngestServiceMethodDescriptorSupplier
      extends ArrowIngestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ArrowIngestServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ArrowIngestServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ArrowIngestServiceFileDescriptorSupplier())
              .addMethod(getIcebergAPIMethod())
              .build();
        }
      }
    }
    return result;
  }
}
