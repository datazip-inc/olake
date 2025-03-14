package io.debezium.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The service definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: messaging.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class StringArrayServiceGrpc {

  private StringArrayServiceGrpc() {}

  public static final String SERVICE_NAME = "io.debezium.server.iceberg.rpc.StringArrayService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest,
      io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse> getSendStringArrayMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendStringArray",
      requestType = io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest.class,
      responseType = io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest,
      io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse> getSendStringArrayMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest, io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse> getSendStringArrayMethod;
    if ((getSendStringArrayMethod = StringArrayServiceGrpc.getSendStringArrayMethod) == null) {
      synchronized (StringArrayServiceGrpc.class) {
        if ((getSendStringArrayMethod = StringArrayServiceGrpc.getSendStringArrayMethod) == null) {
          StringArrayServiceGrpc.getSendStringArrayMethod = getSendStringArrayMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest, io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendStringArray"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse.getDefaultInstance()))
              .setSchemaDescriptor(new StringArrayServiceMethodDescriptorSupplier("SendStringArray"))
              .build();
        }
      }
    }
    return getSendStringArrayMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static StringArrayServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StringArrayServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StringArrayServiceStub>() {
        @java.lang.Override
        public StringArrayServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StringArrayServiceStub(channel, callOptions);
        }
      };
    return StringArrayServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static StringArrayServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StringArrayServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StringArrayServiceBlockingStub>() {
        @java.lang.Override
        public StringArrayServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StringArrayServiceBlockingStub(channel, callOptions);
        }
      };
    return StringArrayServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static StringArrayServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<StringArrayServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<StringArrayServiceFutureStub>() {
        @java.lang.Override
        public StringArrayServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new StringArrayServiceFutureStub(channel, callOptions);
        }
      };
    return StringArrayServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * The service definition.
   * </pre>
   */
  public static abstract class StringArrayServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Sends a string array and returns a simple response.
     * </pre>
     */
    public void sendStringArray(io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendStringArrayMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSendStringArrayMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest,
                io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse>(
                  this, METHODID_SEND_STRING_ARRAY)))
          .build();
    }
  }

  /**
   * <pre>
   * The service definition.
   * </pre>
   */
  public static final class StringArrayServiceStub extends io.grpc.stub.AbstractAsyncStub<StringArrayServiceStub> {
    private StringArrayServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StringArrayServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StringArrayServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a string array and returns a simple response.
     * </pre>
     */
    public void sendStringArray(io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendStringArrayMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The service definition.
   * </pre>
   */
  public static final class StringArrayServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<StringArrayServiceBlockingStub> {
    private StringArrayServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StringArrayServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StringArrayServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a string array and returns a simple response.
     * </pre>
     */
    public io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse sendStringArray(io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendStringArrayMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The service definition.
   * </pre>
   */
  public static final class StringArrayServiceFutureStub extends io.grpc.stub.AbstractFutureStub<StringArrayServiceFutureStub> {
    private StringArrayServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected StringArrayServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new StringArrayServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a string array and returns a simple response.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse> sendStringArray(
        io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendStringArrayMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SEND_STRING_ARRAY = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final StringArrayServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(StringArrayServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SEND_STRING_ARRAY:
          serviceImpl.sendStringArray((io.debezium.server.iceberg.rpc.Messaging.StringArrayRequest) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.Messaging.StringArrayResponse>) responseObserver);
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

  private static abstract class StringArrayServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    StringArrayServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.debezium.server.iceberg.rpc.Messaging.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("StringArrayService");
    }
  }

  private static final class StringArrayServiceFileDescriptorSupplier
      extends StringArrayServiceBaseDescriptorSupplier {
    StringArrayServiceFileDescriptorSupplier() {}
  }

  private static final class StringArrayServiceMethodDescriptorSupplier
      extends StringArrayServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    StringArrayServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (StringArrayServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new StringArrayServiceFileDescriptorSupplier())
              .addMethod(getSendStringArrayMethod())
              .build();
        }
      }
    }
    return result;
  }
}
