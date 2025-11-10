package io.debezium.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: record_ingest.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ArrowRecordIngestServiceGrpc {

  private ArrowRecordIngestServiceGrpc() {}

  public static final String SERVICE_NAME = "io.debezium.server.iceberg.rpc.ArrowRecordIngestService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse> getUploadFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UploadFile",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse> getUploadFileMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest, io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse> getUploadFileMethod;
    if ((getUploadFileMethod = ArrowRecordIngestServiceGrpc.getUploadFileMethod) == null) {
      synchronized (ArrowRecordIngestServiceGrpc.class) {
        if ((getUploadFileMethod = ArrowRecordIngestServiceGrpc.getUploadFileMethod) == null) {
          ArrowRecordIngestServiceGrpc.getUploadFileMethod = getUploadFileMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest, io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UploadFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ArrowRecordIngestServiceMethodDescriptorSupplier("UploadFile"))
              .build();
        }
      }
    }
    return getUploadFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse> getGenerateFilenameMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateFilename",
      requestType = io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest.class,
      responseType = io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest,
      io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse> getGenerateFilenameMethod() {
    io.grpc.MethodDescriptor<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest, io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse> getGenerateFilenameMethod;
    if ((getGenerateFilenameMethod = ArrowRecordIngestServiceGrpc.getGenerateFilenameMethod) == null) {
      synchronized (ArrowRecordIngestServiceGrpc.class) {
        if ((getGenerateFilenameMethod = ArrowRecordIngestServiceGrpc.getGenerateFilenameMethod) == null) {
          ArrowRecordIngestServiceGrpc.getGenerateFilenameMethod = getGenerateFilenameMethod =
              io.grpc.MethodDescriptor.<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest, io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateFilename"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ArrowRecordIngestServiceMethodDescriptorSupplier("GenerateFilename"))
              .build();
        }
      }
    }
    return getGenerateFilenameMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ArrowRecordIngestServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArrowRecordIngestServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArrowRecordIngestServiceStub>() {
        @java.lang.Override
        public ArrowRecordIngestServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArrowRecordIngestServiceStub(channel, callOptions);
        }
      };
    return ArrowRecordIngestServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ArrowRecordIngestServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArrowRecordIngestServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArrowRecordIngestServiceBlockingStub>() {
        @java.lang.Override
        public ArrowRecordIngestServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArrowRecordIngestServiceBlockingStub(channel, callOptions);
        }
      };
    return ArrowRecordIngestServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ArrowRecordIngestServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ArrowRecordIngestServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ArrowRecordIngestServiceFutureStub>() {
        @java.lang.Override
        public ArrowRecordIngestServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ArrowRecordIngestServiceFutureStub(channel, callOptions);
        }
      };
    return ArrowRecordIngestServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class ArrowRecordIngestServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void uploadFile(io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUploadFileMethod(), responseObserver);
    }

    /**
     */
    public void generateFilename(io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateFilenameMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getUploadFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest,
                io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse>(
                  this, METHODID_UPLOAD_FILE)))
          .addMethod(
            getGenerateFilenameMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest,
                io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse>(
                  this, METHODID_GENERATE_FILENAME)))
          .build();
    }
  }

  /**
   */
  public static final class ArrowRecordIngestServiceStub extends io.grpc.stub.AbstractAsyncStub<ArrowRecordIngestServiceStub> {
    private ArrowRecordIngestServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArrowRecordIngestServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArrowRecordIngestServiceStub(channel, callOptions);
    }

    /**
     */
    public void uploadFile(io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUploadFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void generateFilename(io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest request,
        io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateFilenameMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ArrowRecordIngestServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ArrowRecordIngestServiceBlockingStub> {
    private ArrowRecordIngestServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArrowRecordIngestServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArrowRecordIngestServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse uploadFile(io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUploadFileMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse generateFilename(io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateFilenameMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ArrowRecordIngestServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ArrowRecordIngestServiceFutureStub> {
    private ArrowRecordIngestServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ArrowRecordIngestServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ArrowRecordIngestServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse> uploadFile(
        io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUploadFileMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse> generateFilename(
        io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateFilenameMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_UPLOAD_FILE = 0;
  private static final int METHODID_GENERATE_FILENAME = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ArrowRecordIngestServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ArrowRecordIngestServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_UPLOAD_FILE:
          serviceImpl.uploadFile((io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadRequest) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFileUploadResponse>) responseObserver);
          break;
        case METHODID_GENERATE_FILENAME:
          serviceImpl.generateFilename((io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameRequest) request,
              (io.grpc.stub.StreamObserver<io.debezium.server.iceberg.rpc.RecordIngest.ArrowFilenameResponse>) responseObserver);
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

  private static abstract class ArrowRecordIngestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ArrowRecordIngestServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.debezium.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ArrowRecordIngestService");
    }
  }

  private static final class ArrowRecordIngestServiceFileDescriptorSupplier
      extends ArrowRecordIngestServiceBaseDescriptorSupplier {
    ArrowRecordIngestServiceFileDescriptorSupplier() {}
  }

  private static final class ArrowRecordIngestServiceMethodDescriptorSupplier
      extends ArrowRecordIngestServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ArrowRecordIngestServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ArrowRecordIngestServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ArrowRecordIngestServiceFileDescriptorSupplier())
              .addMethod(getUploadFileMethod())
              .addMethod(getGenerateFilenameMethod())
              .build();
        }
      }
    }
    return result;
  }
}
