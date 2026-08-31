package io.debezium.server.iceberg.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;

import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Pins the ErrorInfo payload Go parses. A change here is invisible to the Go compiler.
 */
class OlakeFailuresTest {

    /** Cause that unwraps to itself — both chain walks must terminate. */
    private static final class SelfReferencing extends RuntimeException {
        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }

    /** Unpacks the ErrorInfo Go will read, or fails if none was attached. */
    private static ErrorInfo errorInfoOf(StatusRuntimeException e) throws InvalidProtocolBufferException {
        Status status = StatusProto.fromThrowable(e);
        assertNotNull(status, "no google.rpc.Status attached");
        assertEquals(1, status.getDetailsCount(), "expected exactly one detail");
        return status.getDetails(0).unpack(ErrorInfo.class);
    }

    private static ErrorInfo errorInfoOf(Throwable cause) throws InvalidProtocolBufferException {
        return errorInfoOf(OlakeFailures.toStatusException(cause, "OP", "m"));
    }

    private static S3Exception s3Exception(String errorCode) {
        return s3Exception(errorCode, null);
    }

    private static S3Exception s3Exception(String errorCode, Throwable cause) {
        S3Exception.Builder builder = S3Exception.builder().message("denied");
        if (errorCode != null) {
            builder.awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build());
        }
        if (cause != null) {
            builder.cause(cause);
        }
        return (S3Exception) builder.build();
    }

    /** `depth` RuntimeException wrappers around an IllegalStateException. */
    private static Throwable chainOfDepth(int depth) {
        Throwable chain = new IllegalStateException("bottom");
        for (int i = 0; i < depth; i++) {
            chain = new RuntimeException("layer " + i, chain);
        }
        return chain;
    }

    // status is always INTERNAL; Go ignores the message and reads ErrorInfo instead
    @Test
    @DisplayName("status is INTERNAL and the message is unchanged")
    void statusAndMessage() {
        Status status = StatusProto.fromThrowable(
                OlakeFailures.toStatusException(new IllegalStateException("boom"), "GET_OR_CREATE_TABLE", "human text"));

        assertNotNull(status);
        assertEquals(Code.INTERNAL.getNumber(), status.getCode());
        assertEquals("human text", status.getMessage());
    }

    // a null message must not become the string "null", which Go would then treat as prose
    @Test
    @DisplayName("a null message becomes empty")
    void nullMessageIsEmpty() {
        assertEquals("", StatusProto.fromThrowable(
                OlakeFailures.toStatusException(new IllegalStateException("x"), "OP", null)).getMessage());
    }

    // Go matches on this literal, so the constant and the wire value must stay identical
    @Test
    @DisplayName("domain is olake.iceberg")
    void domainIsStamped() throws Exception {
        assertEquals("olake.iceberg", errorInfoOf(new IllegalStateException("x")).getDomain());
        assertEquals(OlakeFailures.DOMAIN, errorInfoOf(new IllegalStateException("x")).getDomain());
    }

    static Stream<Arguments> rootCauseCases() {
        return Stream.of(
                // Iceberg wraps heavily; the outermost class identifies nothing
                Arguments.of("nested wrappers report the innermost",
                        new RuntimeException("outer",
                                new IllegalStateException("middle", new NumberFormatException("innermost"))),
                        NumberFormatException.class.getName()),
                // no cause to walk
                Arguments.of("an exception with no cause is its own root",
                        new IllegalArgumentException("alone"), IllegalArgumentException.class.getName()),
                // a self-reference cannot hang the writer
                Arguments.of("a self-referencing cause terminates",
                        new SelfReferencing(), SelfReferencing.class.getName()),
                // 16 hops is the last layer that still reaches the leaf
                Arguments.of("a chain at the bound still reaches the leaf",
                        chainOfDepth(16), IllegalStateException.class.getName()),
                // past 16 hops it stops rather than walking forever
                Arguments.of("a chain deeper than the bound stops at the bound",
                        chainOfDepth(17), RuntimeException.class.getName()));
    }

    // reason is the innermost class name — that is what Go maps to a category
    @ParameterizedTest(name = "{0}")
    @MethodSource("rootCauseCases")
    @DisplayName("reason is the root cause's class")
    void reasonIsTheRootCause(String name, Throwable cause, String expectedReason) throws Exception {
        assertEquals(expectedReason, errorInfoOf(cause).getReason());
    }

    static Stream<Arguments> vendorCodeCases() {
        return Stream.of(
                // JDBC catalogs report SQLSTATE; 28000 is invalid_authorization_specification
                Arguments.of("sqlstate at the top", new SQLException("denied", "28000"), "28000"),
                // Iceberg wraps the catalog exception, so the walk must look through wrappers
                Arguments.of("sqlstate below a wrapper",
                        new RuntimeException("catalog failed", new SQLException("denied", "28000")), "28000"),
                // nested SQLSTATE: first match walking outward-in, so the outer code wins
                Arguments.of("outer sqlstate wins over inner sqlstate",
                        new SQLException("outer", "28000", new SQLException("inner", "42000")), "28000"),
                // the S3 code is the only thing separating a wrong key from a missing bucket
                Arguments.of("s3 error code", new RuntimeException("write failed", s3Exception("AccessDenied")),
                        "AccessDenied"),
                // mixed vendors: first match walking outward-in, not the innermost
                Arguments.of("outer sqlstate wins over inner s3",
                        new SQLException("denied", "28000", s3Exception("AccessDenied")), "28000"),
                Arguments.of("outer s3 wins over inner sqlstate",
                        s3Exception("AccessDenied", new SQLException("denied", "28000")), "AccessDenied"),
                // absent is omitted, not sent empty — Go treats a missing key as a real slice
                Arguments.of("no vendor cause at all", new IllegalStateException("x"), null),
                Arguments.of("sqlstate is null", new SQLException("no sqlstate"), null),
                Arguments.of("sqlstate is empty", new SQLException("no sqlstate", ""), null),
                Arguments.of("aws exception with no error details", s3Exception(null), null),
                Arguments.of("aws error code is empty", s3Exception(""), null),
                // vendorCode has its own cycle guard, separate from rootCause
                Arguments.of("self-referencing cause", new SelfReferencing(), null));
    }

    // SQLSTATE / S3 code travels when present; otherwise the key is omitted
    @ParameterizedTest(name = "{0}")
    @MethodSource("vendorCodeCases")
    @DisplayName("vendor code travels when there is one")
    void vendorCodeTravels(String name, Throwable cause, String expectedCode) throws Exception {
        Map<String, String> metadata = errorInfoOf(cause).getMetadataMap();

        if (expectedCode == null) {
            assertFalse(metadata.containsKey(OlakeFailures.METADATA_CODE),
                    "an absent code must be omitted, not sent empty");
        } else {
            assertEquals(expectedCode, metadata.get(OlakeFailures.METADATA_CODE));
        }
    }

    static Stream<Arguments> operationCases() {
        return Stream.of(
                // the call being served, e.g. GET_OR_CREATE_TABLE
                Arguments.of("supplied", "GET_OR_CREATE_TABLE", "GET_OR_CREATE_TABLE"),
                // empty/null must be omitted so Go does not read a blank operation
                Arguments.of("empty is omitted", "", null),
                Arguments.of("null is omitted", null, null));
    }

    // operation metadata is optional: sent when supplied, omitted when blank
    @ParameterizedTest(name = "{0}")
    @MethodSource("operationCases")
    @DisplayName("operation travels when supplied")
    void operationMetadata(String name, String operation, String expected) throws Exception {
        Map<String, String> metadata = errorInfoOf(
                OlakeFailures.toStatusException(new RuntimeException("x"), operation, "m")).getMetadataMap();

        if (expected == null) {
            assertFalse(metadata.containsKey(OlakeFailures.METADATA_OPERATION));
        } else {
            assertEquals(expected, metadata.get(OlakeFailures.METADATA_OPERATION));
        }
    }

    // Java sends facts only (class + vendor code). The category is decided in Go.
    @Test
    @DisplayName("no category is sent")
    void nothingElseTravels() throws Exception {
        ErrorInfo info = errorInfoOf(OlakeFailures.toStatusException(
                new SQLException("table users column ssn is invalid", "42000"), "GET_OR_CREATE_TABLE", "m"));

        assertEquals(Map.of(
                OlakeFailures.METADATA_OPERATION, "GET_OR_CREATE_TABLE",
                OlakeFailures.METADATA_CODE, "42000"), info.getMetadataMap());
        assertTrue(info.getReason().startsWith("java.sql."), "reason should be a class name, got " + info.getReason());
    }
}
