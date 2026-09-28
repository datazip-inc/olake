package io.olake.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ToastReadService serves values a CDC change could not carry (Postgres omits unchanged
 * out-of-line columns from an UPDATE) by reading them out of the rows the destination
 * table already holds. Both calls reuse the session created by GET_OR_CREATE_TABLE.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: toast_read.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ToastReadServiceGrpc {

  private ToastReadServiceGrpc() {}

  public static final String SERVICE_NAME = "io.olake.iceberg.rpc.ToastReadService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.olake.iceberg.rpc.ToastRead.ReadRowsRequest,
      io.olake.iceberg.rpc.ToastRead.ReadRowsBatch> getReadRowsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadRows",
      requestType = io.olake.iceberg.rpc.ToastRead.ReadRowsRequest.class,
      responseType = io.olake.iceberg.rpc.ToastRead.ReadRowsBatch.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<io.olake.iceberg.rpc.ToastRead.ReadRowsRequest,
      io.olake.iceberg.rpc.ToastRead.ReadRowsBatch> getReadRowsMethod() {
    io.grpc.MethodDescriptor<io.olake.iceberg.rpc.ToastRead.ReadRowsRequest, io.olake.iceberg.rpc.ToastRead.ReadRowsBatch> getReadRowsMethod;
    if ((getReadRowsMethod = ToastReadServiceGrpc.getReadRowsMethod) == null) {
      synchronized (ToastReadServiceGrpc.class) {
        if ((getReadRowsMethod = ToastReadServiceGrpc.getReadRowsMethod) == null) {
          ToastReadServiceGrpc.getReadRowsMethod = getReadRowsMethod =
              io.grpc.MethodDescriptor.<io.olake.iceberg.rpc.ToastRead.ReadRowsRequest, io.olake.iceberg.rpc.ToastRead.ReadRowsBatch>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadRows"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.iceberg.rpc.ToastRead.ReadRowsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.iceberg.rpc.ToastRead.ReadRowsBatch.getDefaultInstance()))
              .setSchemaDescriptor(new ToastReadServiceMethodDescriptorSupplier("ReadRows"))
              .build();
        }
      }
    }
    return getReadRowsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest,
      io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse> getFlushOpenFilesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FlushOpenFiles",
      requestType = io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest.class,
      responseType = io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest,
      io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse> getFlushOpenFilesMethod() {
    io.grpc.MethodDescriptor<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest, io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse> getFlushOpenFilesMethod;
    if ((getFlushOpenFilesMethod = ToastReadServiceGrpc.getFlushOpenFilesMethod) == null) {
      synchronized (ToastReadServiceGrpc.class) {
        if ((getFlushOpenFilesMethod = ToastReadServiceGrpc.getFlushOpenFilesMethod) == null) {
          ToastReadServiceGrpc.getFlushOpenFilesMethod = getFlushOpenFilesMethod =
              io.grpc.MethodDescriptor.<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest, io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FlushOpenFiles"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ToastReadServiceMethodDescriptorSupplier("FlushOpenFiles"))
              .build();
        }
      }
    }
    return getFlushOpenFilesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ToastReadServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ToastReadServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ToastReadServiceStub>() {
        @java.lang.Override
        public ToastReadServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ToastReadServiceStub(channel, callOptions);
        }
      };
    return ToastReadServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ToastReadServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ToastReadServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ToastReadServiceBlockingStub>() {
        @java.lang.Override
        public ToastReadServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ToastReadServiceBlockingStub(channel, callOptions);
        }
      };
    return ToastReadServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ToastReadServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ToastReadServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ToastReadServiceFutureStub>() {
        @java.lang.Override
        public ToastReadServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ToastReadServiceFutureStub(channel, callOptions);
        }
      };
    return ToastReadServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ToastReadService serves values a CDC change could not carry (Postgres omits unchanged
   * out-of-line columns from an UPDATE) by reading them out of the rows the destination
   * table already holds. Both calls reuse the session created by GET_OR_CREATE_TABLE.
   * </pre>
   */
  public static abstract class ToastReadServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * ReadRows streams the requested columns of the rows addressed by data file and position.
     * </pre>
     */
    public void readRows(io.olake.iceberg.rpc.ToastRead.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<io.olake.iceberg.rpc.ToastRead.ReadRowsBatch> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadRowsMethod(), responseObserver);
    }

    /**
     * <pre>
     * FlushOpenFiles closes the data files this thread's writer still holds open, without
     * committing them, so rows written earlier in the same sync become readable.
     * </pre>
     */
    public void flushOpenFiles(io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest request,
        io.grpc.stub.StreamObserver<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFlushOpenFilesMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getReadRowsMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                io.olake.iceberg.rpc.ToastRead.ReadRowsRequest,
                io.olake.iceberg.rpc.ToastRead.ReadRowsBatch>(
                  this, METHODID_READ_ROWS)))
          .addMethod(
            getFlushOpenFilesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest,
                io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse>(
                  this, METHODID_FLUSH_OPEN_FILES)))
          .build();
    }
  }

  /**
   * <pre>
   * ToastReadService serves values a CDC change could not carry (Postgres omits unchanged
   * out-of-line columns from an UPDATE) by reading them out of the rows the destination
   * table already holds. Both calls reuse the session created by GET_OR_CREATE_TABLE.
   * </pre>
   */
  public static final class ToastReadServiceStub extends io.grpc.stub.AbstractAsyncStub<ToastReadServiceStub> {
    private ToastReadServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ToastReadServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ToastReadServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * ReadRows streams the requested columns of the rows addressed by data file and position.
     * </pre>
     */
    public void readRows(io.olake.iceberg.rpc.ToastRead.ReadRowsRequest request,
        io.grpc.stub.StreamObserver<io.olake.iceberg.rpc.ToastRead.ReadRowsBatch> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getReadRowsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * FlushOpenFiles closes the data files this thread's writer still holds open, without
     * committing them, so rows written earlier in the same sync become readable.
     * </pre>
     */
    public void flushOpenFiles(io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest request,
        io.grpc.stub.StreamObserver<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFlushOpenFilesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * ToastReadService serves values a CDC change could not carry (Postgres omits unchanged
   * out-of-line columns from an UPDATE) by reading them out of the rows the destination
   * table already holds. Both calls reuse the session created by GET_OR_CREATE_TABLE.
   * </pre>
   */
  public static final class ToastReadServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<ToastReadServiceBlockingStub> {
    private ToastReadServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ToastReadServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ToastReadServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * ReadRows streams the requested columns of the rows addressed by data file and position.
     * </pre>
     */
    public java.util.Iterator<io.olake.iceberg.rpc.ToastRead.ReadRowsBatch> readRows(
        io.olake.iceberg.rpc.ToastRead.ReadRowsRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getReadRowsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * FlushOpenFiles closes the data files this thread's writer still holds open, without
     * committing them, so rows written earlier in the same sync become readable.
     * </pre>
     */
    public io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse flushOpenFiles(io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFlushOpenFilesMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * ToastReadService serves values a CDC change could not carry (Postgres omits unchanged
   * out-of-line columns from an UPDATE) by reading them out of the rows the destination
   * table already holds. Both calls reuse the session created by GET_OR_CREATE_TABLE.
   * </pre>
   */
  public static final class ToastReadServiceFutureStub extends io.grpc.stub.AbstractFutureStub<ToastReadServiceFutureStub> {
    private ToastReadServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ToastReadServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ToastReadServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * FlushOpenFiles closes the data files this thread's writer still holds open, without
     * committing them, so rows written earlier in the same sync become readable.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse> flushOpenFiles(
        io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFlushOpenFilesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_READ_ROWS = 0;
  private static final int METHODID_FLUSH_OPEN_FILES = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ToastReadServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ToastReadServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_READ_ROWS:
          serviceImpl.readRows((io.olake.iceberg.rpc.ToastRead.ReadRowsRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.iceberg.rpc.ToastRead.ReadRowsBatch>) responseObserver);
          break;
        case METHODID_FLUSH_OPEN_FILES:
          serviceImpl.flushOpenFiles((io.olake.iceberg.rpc.ToastRead.FlushOpenFilesRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.iceberg.rpc.ToastRead.FlushOpenFilesResponse>) responseObserver);
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

  private static abstract class ToastReadServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ToastReadServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.olake.iceberg.rpc.ToastRead.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ToastReadService");
    }
  }

  private static final class ToastReadServiceFileDescriptorSupplier
      extends ToastReadServiceBaseDescriptorSupplier {
    ToastReadServiceFileDescriptorSupplier() {}
  }

  private static final class ToastReadServiceMethodDescriptorSupplier
      extends ToastReadServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ToastReadServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (ToastReadServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ToastReadServiceFileDescriptorSupplier())
              .addMethod(getReadRowsMethod())
              .addMethod(getFlushOpenFilesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
