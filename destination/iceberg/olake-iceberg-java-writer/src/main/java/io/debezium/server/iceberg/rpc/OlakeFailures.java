package io.debezium.server.iceberg.rpc;

import java.sql.SQLException;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;

import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Builds the gRPC error the Go side reads.
 *
 * <p>The status code is INTERNAL for everything raised here, and the description is prose the Go
 * side deliberately does not parse — it changes between library versions and can contain table
 * names and column values. The facts travel as an {@link ErrorInfo} detail instead, a standard
 * google.rpc type, so neither side's generated protobuf changes.
 *
 * <p>Only what was caught is sent, never what it means: the taxonomy lives in Go alone, so it
 * cannot drift between two languages.
 */
public final class OlakeFailures {

    /** Identifies these details as ours, so the Go side never reads someone else's ErrorInfo. */
    public static final String DOMAIN = "olake.iceberg";

    /** The underlying system's own code, where the exception carried one. */
    public static final String METADATA_CODE = "code";

    /** The call being served when the failure happened, e.g. GET_OR_CREATE_TABLE. */
    public static final String METADATA_OPERATION = "operation";

    private OlakeFailures() {
    }

    /**
     * Wraps a caught exception as a gRPC exception carrying an ErrorInfo detail.
     *
     * @param cause     the exception that ended the call
     * @param operation the payload type being served, e.g. {@code GET_OR_CREATE_TABLE}
     * @param message   the description an operator reads; unchanged from what is logged
     */
    public static StatusRuntimeException toStatusException(Throwable cause, String operation, String message) {
        ErrorInfo.Builder info = ErrorInfo.newBuilder()
                .setDomain(DOMAIN)
                .setReason(rootCause(cause).getClass().getName());

        if (operation != null && !operation.isEmpty()) {
            info.putMetadata(METADATA_OPERATION, operation);
        }
        String code = vendorCode(cause);
        if (code != null && !code.isEmpty()) {
            info.putMetadata(METADATA_CODE, code);
        }

        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.INTERNAL.getNumber())
                .setMessage(message == null ? "" : message)
                .addDetails(Any.pack(info.build()))
                .build();

        return StatusProto.toStatusRuntimeException(status);
    }

    /**
     * Returns the innermost cause. Iceberg and Hadoop wrap heavily, and the outermost class is
     * usually a generic wrapper that identifies nothing.
     */
    private static Throwable rootCause(Throwable t) {
        Throwable root = t;
        // Bounded so a self-referencing cause cannot loop.
        for (int depth = 0; depth < 16; depth++) {
            Throwable next = root.getCause();
            if (next == null || next == root) {
                break;
            }
            root = next;
        }
        return root;
    }

    /**
     * Reports the underlying system's own error code where the exception carries one. Two systems
     * do: a JDBC catalog through SQLSTATE, and object storage through the S3 error code, which is
     * the only thing separating a wrong key from a missing bucket. Everything else is identified
     * by its exception class alone.
     */
    private static String vendorCode(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof SQLException) {
                return ((SQLException) t).getSQLState();
            }
            if (t instanceof AwsServiceException) {
                AwsErrorDetails details = ((AwsServiceException) t).awsErrorDetails();
                if (details != null) {
                    return details.errorCode();
                }
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return null;
    }
}
