package io.olake.server.iceberg.rpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ============================================
 * Unified Writer Service
 * ============================================
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: record_ingest.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class WriterServiceGrpc {

  private WriterServiceGrpc() {}

  public static final String SERVICE_NAME = "io.olake.server.iceberg.rpc.WriterService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest,
      io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse> getInitSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InitSession",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest,
      io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse> getInitSessionMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest, io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse> getInitSessionMethod;
    if ((getInitSessionMethod = WriterServiceGrpc.getInitSessionMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getInitSessionMethod = WriterServiceGrpc.getInitSessionMethod) == null) {
          WriterServiceGrpc.getInitSessionMethod = getInitSessionMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest, io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InitSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("InitSession"))
              .build();
        }
      }
    }
    return getInitSessionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest,
      io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse> getSendRecordsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendRecords",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest,
      io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse> getSendRecordsMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest, io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse> getSendRecordsMethod;
    if ((getSendRecordsMethod = WriterServiceGrpc.getSendRecordsMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getSendRecordsMethod = WriterServiceGrpc.getSendRecordsMethod) == null) {
          WriterServiceGrpc.getSendRecordsMethod = getSendRecordsMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest, io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendRecords"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("SendRecords"))
              .build();
        }
      }
    }
    return getSendRecordsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest,
      io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse> getEvolveSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "EvolveSchema",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest,
      io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse> getEvolveSchemaMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest, io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse> getEvolveSchemaMethod;
    if ((getEvolveSchemaMethod = WriterServiceGrpc.getEvolveSchemaMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getEvolveSchemaMethod = WriterServiceGrpc.getEvolveSchemaMethod) == null) {
          WriterServiceGrpc.getEvolveSchemaMethod = getEvolveSchemaMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest, io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "EvolveSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("EvolveSchema"))
              .build();
        }
      }
    }
    return getEvolveSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest,
      io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse> getRefreshTableSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "RefreshTableSchema",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest,
      io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse> getRefreshTableSchemaMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest, io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse> getRefreshTableSchemaMethod;
    if ((getRefreshTableSchemaMethod = WriterServiceGrpc.getRefreshTableSchemaMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getRefreshTableSchemaMethod = WriterServiceGrpc.getRefreshTableSchemaMethod) == null) {
          WriterServiceGrpc.getRefreshTableSchemaMethod = getRefreshTableSchemaMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest, io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RefreshTableSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("RefreshTableSchema"))
              .build();
        }
      }
    }
    return getRefreshTableSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest,
      io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse> getGetFilePathMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFilePath",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest,
      io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse> getGetFilePathMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest, io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse> getGetFilePathMethod;
    if ((getGetFilePathMethod = WriterServiceGrpc.getGetFilePathMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getGetFilePathMethod = WriterServiceGrpc.getGetFilePathMethod) == null) {
          WriterServiceGrpc.getGetFilePathMethod = getGetFilePathMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest, io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFilePath"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("GetFilePath"))
              .build();
        }
      }
    }
    return getGetFilePathMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest,
      io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse> getUploadFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UploadFile",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest,
      io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse> getUploadFileMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest, io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse> getUploadFileMethod;
    if ((getUploadFileMethod = WriterServiceGrpc.getUploadFileMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getUploadFileMethod = WriterServiceGrpc.getUploadFileMethod) == null) {
          WriterServiceGrpc.getUploadFileMethod = getUploadFileMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest, io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UploadFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("UploadFile"))
              .build();
        }
      }
    }
    return getUploadFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CommitRequest,
      io.olake.server.iceberg.rpc.RecordIngest.CommitResponse> getCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Commit",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.CommitRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.CommitResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CommitRequest,
      io.olake.server.iceberg.rpc.RecordIngest.CommitResponse> getCommitMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CommitRequest, io.olake.server.iceberg.rpc.RecordIngest.CommitResponse> getCommitMethod;
    if ((getCommitMethod = WriterServiceGrpc.getCommitMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getCommitMethod = WriterServiceGrpc.getCommitMethod) == null) {
          WriterServiceGrpc.getCommitMethod = getCommitMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.CommitRequest, io.olake.server.iceberg.rpc.RecordIngest.CommitResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.CommitRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.CommitResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("Commit"))
              .build();
        }
      }
    }
    return getCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest,
      io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse> getCloseSessionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CloseSession",
      requestType = io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest.class,
      responseType = io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest,
      io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse> getCloseSessionMethod() {
    io.grpc.MethodDescriptor<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest, io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse> getCloseSessionMethod;
    if ((getCloseSessionMethod = WriterServiceGrpc.getCloseSessionMethod) == null) {
      synchronized (WriterServiceGrpc.class) {
        if ((getCloseSessionMethod = WriterServiceGrpc.getCloseSessionMethod) == null) {
          WriterServiceGrpc.getCloseSessionMethod = getCloseSessionMethod =
              io.grpc.MethodDescriptor.<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest, io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CloseSession"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new WriterServiceMethodDescriptorSupplier("CloseSession"))
              .build();
        }
      }
    }
    return getCloseSessionMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static WriterServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WriterServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WriterServiceStub>() {
        @java.lang.Override
        public WriterServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WriterServiceStub(channel, callOptions);
        }
      };
    return WriterServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static WriterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WriterServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WriterServiceBlockingStub>() {
        @java.lang.Override
        public WriterServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WriterServiceBlockingStub(channel, callOptions);
        }
      };
    return WriterServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static WriterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<WriterServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<WriterServiceFutureStub>() {
        @java.lang.Override
        public WriterServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new WriterServiceFutureStub(channel, callOptions);
        }
      };
    return WriterServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ============================================
   * Unified Writer Service
   * ============================================
   * </pre>
   */
  public static abstract class WriterServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void initSession(io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInitSessionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Row methods
     * </pre>
     */
    public void sendRecords(io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSendRecordsMethod(), responseObserver);
    }

    /**
     */
    public void evolveSchema(io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEvolveSchemaMethod(), responseObserver);
    }

    /**
     */
    public void refreshTableSchema(io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRefreshTableSchemaMethod(), responseObserver);
    }

    /**
     * <pre>
     * Arrow methods
     * </pre>
     */
    public void getFilePath(io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFilePathMethod(), responseObserver);
    }

    /**
     */
    public void uploadFile(io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUploadFileMethod(), responseObserver);
    }

    /**
     * <pre>
     * Common methods
     * </pre>
     */
    public void commit(io.olake.server.iceberg.rpc.RecordIngest.CommitRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CommitResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
    }

    /**
     */
    public void closeSession(io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloseSessionMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getInitSessionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest,
                io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse>(
                  this, METHODID_INIT_SESSION)))
          .addMethod(
            getSendRecordsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest,
                io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse>(
                  this, METHODID_SEND_RECORDS)))
          .addMethod(
            getEvolveSchemaMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest,
                io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse>(
                  this, METHODID_EVOLVE_SCHEMA)))
          .addMethod(
            getRefreshTableSchemaMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest,
                io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse>(
                  this, METHODID_REFRESH_TABLE_SCHEMA)))
          .addMethod(
            getGetFilePathMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest,
                io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse>(
                  this, METHODID_GET_FILE_PATH)))
          .addMethod(
            getUploadFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest,
                io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse>(
                  this, METHODID_UPLOAD_FILE)))
          .addMethod(
            getCommitMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.CommitRequest,
                io.olake.server.iceberg.rpc.RecordIngest.CommitResponse>(
                  this, METHODID_COMMIT)))
          .addMethod(
            getCloseSessionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest,
                io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse>(
                  this, METHODID_CLOSE_SESSION)))
          .build();
    }
  }

  /**
   * <pre>
   * ============================================
   * Unified Writer Service
   * ============================================
   * </pre>
   */
  public static final class WriterServiceStub extends io.grpc.stub.AbstractAsyncStub<WriterServiceStub> {
    private WriterServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WriterServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WriterServiceStub(channel, callOptions);
    }

    /**
     */
    public void initSession(io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInitSessionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Row methods
     * </pre>
     */
    public void sendRecords(io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSendRecordsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void evolveSchema(io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEvolveSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void refreshTableSchema(io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRefreshTableSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Arrow methods
     * </pre>
     */
    public void getFilePath(io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFilePathMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void uploadFile(io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUploadFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Common methods
     * </pre>
     */
    public void commit(io.olake.server.iceberg.rpc.RecordIngest.CommitRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CommitResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void closeSession(io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest request,
        io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCloseSessionMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * ============================================
   * Unified Writer Service
   * ============================================
   * </pre>
   */
  public static final class WriterServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<WriterServiceBlockingStub> {
    private WriterServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WriterServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WriterServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse initSession(io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInitSessionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Row methods
     * </pre>
     */
    public io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse sendRecords(io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSendRecordsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse evolveSchema(io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEvolveSchemaMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse refreshTableSchema(io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRefreshTableSchemaMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Arrow methods
     * </pre>
     */
    public io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse getFilePath(io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFilePathMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse uploadFile(io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUploadFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Common methods
     * </pre>
     */
    public io.olake.server.iceberg.rpc.RecordIngest.CommitResponse commit(io.olake.server.iceberg.rpc.RecordIngest.CommitRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse closeSession(io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCloseSessionMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * ============================================
   * Unified Writer Service
   * ============================================
   * </pre>
   */
  public static final class WriterServiceFutureStub extends io.grpc.stub.AbstractFutureStub<WriterServiceFutureStub> {
    private WriterServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected WriterServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new WriterServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse> initSession(
        io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInitSessionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Row methods
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse> sendRecords(
        io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSendRecordsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse> evolveSchema(
        io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEvolveSchemaMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse> refreshTableSchema(
        io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRefreshTableSchemaMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Arrow methods
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse> getFilePath(
        io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFilePathMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse> uploadFile(
        io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUploadFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Common methods
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.CommitResponse> commit(
        io.olake.server.iceberg.rpc.RecordIngest.CommitRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse> closeSession(
        io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCloseSessionMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_INIT_SESSION = 0;
  private static final int METHODID_SEND_RECORDS = 1;
  private static final int METHODID_EVOLVE_SCHEMA = 2;
  private static final int METHODID_REFRESH_TABLE_SCHEMA = 3;
  private static final int METHODID_GET_FILE_PATH = 4;
  private static final int METHODID_UPLOAD_FILE = 5;
  private static final int METHODID_COMMIT = 6;
  private static final int METHODID_CLOSE_SESSION = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final WriterServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(WriterServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_INIT_SESSION:
          serviceImpl.initSession((io.olake.server.iceberg.rpc.RecordIngest.InitSessionRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.InitSessionResponse>) responseObserver);
          break;
        case METHODID_SEND_RECORDS:
          serviceImpl.sendRecords((io.olake.server.iceberg.rpc.RecordIngest.SendRecordsRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.SendRecordsResponse>) responseObserver);
          break;
        case METHODID_EVOLVE_SCHEMA:
          serviceImpl.evolveSchema((io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.EvolveSchemaResponse>) responseObserver);
          break;
        case METHODID_REFRESH_TABLE_SCHEMA:
          serviceImpl.refreshTableSchema((io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.RefreshTableSchemaResponse>) responseObserver);
          break;
        case METHODID_GET_FILE_PATH:
          serviceImpl.getFilePath((io.olake.server.iceberg.rpc.RecordIngest.GetFilePathRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.GetFilePathResponse>) responseObserver);
          break;
        case METHODID_UPLOAD_FILE:
          serviceImpl.uploadFile((io.olake.server.iceberg.rpc.RecordIngest.UploadFileRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.UploadFileResponse>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((io.olake.server.iceberg.rpc.RecordIngest.CommitRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CommitResponse>) responseObserver);
          break;
        case METHODID_CLOSE_SESSION:
          serviceImpl.closeSession((io.olake.server.iceberg.rpc.RecordIngest.CloseSessionRequest) request,
              (io.grpc.stub.StreamObserver<io.olake.server.iceberg.rpc.RecordIngest.CloseSessionResponse>) responseObserver);
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

  private static abstract class WriterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    WriterServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.olake.server.iceberg.rpc.RecordIngest.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("WriterService");
    }
  }

  private static final class WriterServiceFileDescriptorSupplier
      extends WriterServiceBaseDescriptorSupplier {
    WriterServiceFileDescriptorSupplier() {}
  }

  private static final class WriterServiceMethodDescriptorSupplier
      extends WriterServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    WriterServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (WriterServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new WriterServiceFileDescriptorSupplier())
              .addMethod(getInitSessionMethod())
              .addMethod(getSendRecordsMethod())
              .addMethod(getEvolveSchemaMethod())
              .addMethod(getRefreshTableSchemaMethod())
              .addMethod(getGetFilePathMethod())
              .addMethod(getUploadFileMethod())
              .addMethod(getCommitMethod())
              .addMethod(getCloseSessionMethod())
              .build();
        }
      }
    }
    return result;
  }
}
