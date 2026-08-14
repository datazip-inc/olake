package iceberg

import (
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/utils/errs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Codes for conditions this writer detects itself, before or instead of talking to the JVM.
const (
	codeCatalogConfigInvalid   = "iceberg.catalog_config_invalid"
	codeUnsupportedCatalogType = "iceberg.unsupported_catalog_type"
	codeJarNotFound            = "iceberg.jar_not_found"
	codeJVMStartFailed         = "iceberg.jvm_start_failed"
)

// grpcCategories maps a gRPC status code to a failure category. Transport-level only.
//
// Iceberg runs in a JVM that OLake drives over gRPC, and that JVM collapses every failure it
// catches into Status.INTERNAL (rpc/OlakeRowsIngester.java). A missing table, a rejected
// credential and a schema conflict therefore arrive as the same code, carrying their meaning
// only in a description this design does not read.
//
// The codes below are the ones gRPC produces when the call never reached application code — the
// JVM is not up, the deadline passed, the channel was torn down. Those are the failures the Java
// side cannot report, because it never ran.
var grpcCategories = map[codes.Code]errs.Category{
	codes.Unavailable:       errs.NetworkUnreachable, // the JVM is not listening, or the channel dropped
	codes.DeadlineExceeded:  errs.Timeout,
	codes.Canceled:          errs.Canceled,
	codes.PermissionDenied:  errs.PermissionDenied,
	codes.Unauthenticated:   errs.AuthFailed,
	codes.NotFound:          errs.ObjectNotFound,
	codes.ResourceExhausted: errs.ResourceExhausted,
	codes.Unimplemented:     errs.UnsupportedFeature,
}

// Register so ReportFailure can classify without knowing which connector ran. Only gRPC
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads the gRPC status from the Iceberg JVM, then the writer marker.
//
// Order matters: a JVM that is not listening explains a write failure better than the write
// failure does.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	if f := fromGRPCStatus(err); f != nil {
		return f
	}
	if destination.IsWriteFailure(err) {
		return &errs.Failure{
			Category:     errs.DestinationWriteError,
			ClassifiedBy: errs.ClassifiedByPrecondition,
		}
	}
	return nil
}

// fromGRPCStatus classifies a failure the gRPC channel reported. status.FromError reaches the
// status through %w wrapping, which is why the leaf wraps on this path had to become %w.
func fromGRPCStatus(err error) *errs.Failure {
	if err == nil {
		return nil
	}
	grpcStatus, ok := status.FromError(err)
	if !ok || grpcStatus == nil || grpcStatus.Code() == codes.OK {
		return nil
	}

	// The code is prefixed because it is the *transport's* answer, not Iceberg's. An
	// unprefixed "NotFound" in telemetry would read as a missing table when it means the
	// gRPC method was not found — a completely different investigation.
	code := fmt.Sprintf("grpc.%s", grpcStatus.Code().String())

	if category, mapped := grpcCategories[grpcStatus.Code()]; mapped {
		return &errs.Failure{
			Category:     category,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         code,
		}
	}

	// Almost always Internal, which the JVM sends for everything it catches and which therefore
	// identifies nothing. Reported as unclassified so the share of failures arriving that way
	// stays visible; any category assigned to it would be wrong for most of them.
	return &errs.Failure{
		Category:     errs.Unclassified,
		ClassifiedBy: errs.ClassifiedByDefault,
		Code:         code,
	}
}
