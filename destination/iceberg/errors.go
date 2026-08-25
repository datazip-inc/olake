package iceberg

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils/errs"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stamped by the writer on every ErrorInfo it attaches, so details from other libraries
// passing through are ignored.
const (
	javaErrorDomain            = "olake.iceberg"
	javaMetadataCode           = "code"
	codeCatalogConfigInvalid   = "iceberg.catalog_config_invalid"
	codeUnsupportedCatalogType = "iceberg.unsupported_catalog_type"
	codeJarNotFound            = "iceberg.jar_not_found"
	codeJVMStartFailed         = "iceberg.jvm_start_failed"
	codePartitionRegexNoMatch  = "iceberg.partition_regex_no_match"
)

// errJVMStart marks the startup path so the classifier can find it with errors.Is, not message text.
var errJVMStart = errors.New("failed to start iceberg java writer and setup logger")

// startupCause pulls the root cause's class out of the line OlakeRpcServer prints on its way out.
var startupCause = regexp.MustCompile(`Iceberg writer failed to start \[([^\]]+)\]`)

// javaExceptions maps the exception the Iceberg JVM caught to a failure category.
var javaExceptions = map[string]errs.Category{
	// The catalog does not have it.
	"NoSuchTableException":     errs.ObjectNotFound,
	"NoSuchNamespaceException": errs.ObjectNotFound,
	"NoSuchViewException":      errs.ObjectNotFound,
	"EntityNotFoundException":  errs.ObjectNotFound,
	"NoSuchKeyException":       errs.ObjectNotFound,

	// At the catalog or at object storage.
	"NotAuthorizedException": errs.AuthFailed,
	"ForbiddenException":     errs.PermissionDenied,
	"AccessDeniedException":  errs.PermissionDenied,

	// Two writers reached the same table. The first clears on retry; the others leave a commit
	// only the catalog can settle.
	"CommitFailedException":       errs.ConcurrencyConflict,
	"AlreadyExistsException":      errs.CatalogError,
	"CommitStateUnknownException": errs.CatalogError,

	// The records do not fit the table.
	"ValidationException": errs.SchemaUnsupported,

	// Reachability, seen from the JVM rather than from Go.
	"UnknownHostException":       errs.DNSResolutionFailed,
	"ConnectException":           errs.NetworkUnreachable,
	"NoRouteToHostException":     errs.NetworkUnreachable,
	"TTransportException":        errs.NetworkUnreachable,
	"MetaException":              errs.NetworkUnreachable,
	"SocketTimeoutException":     errs.Timeout,
	"SSLHandshakeException":      errs.TLSFailed,
	"CertificateException":       errs.TLSFailed,
	"SSLPeerUnverifiedException": errs.TLSFailed,

	// The JVM ran out.
	"OutOfMemoryError": errs.ResourceExhausted,

	// It understood the request and will not serve it.
	"UnsupportedOperationException": errs.UnsupportedFeature,

	// Defects in the writer, not conditions a user can fix.
	"NoClassDefFoundError":     errs.InternalError,
	"ClassNotFoundException":   errs.InternalError,
	"NullPointerException":     errs.InternalError,
	"AssertionError":           errs.InternalError,
	"IllegalArgumentException": errs.InternalError,
	"IllegalStateException":    errs.InternalError,
}

// sqlStateClasses maps the two-character SQLSTATE class a JDBC catalog reports to a category.
// Only the class: it is standardized across vendors, while the last three characters are
// vendor-specific and a catalog can be any database.
var sqlStateClasses = map[string]errs.Category{
	"28": errs.AuthFailed,          // invalid authorization specification
	"08": errs.NetworkUnreachable,  // connection exception
	"40": errs.ConcurrencyConflict, // transaction rollback
	"53": errs.ResourceExhausted,   // insufficient resources
	"57": errs.ResourceExhausted,   // operator intervention
}

// sqlStateClass returns the class of a SQLSTATE, or "" for anything that is not one.
func sqlStateClass(code string) string {
	if len(code) != 5 {
		return ""
	}
	return code[:2]
}

// grpcCategories maps a gRPC status code to a failure category — transport level only. These
// are the codes gRPC produces when the call never reached application code: the JVM is not up,
// the deadline passed, the channel was torn down. Anything the JVM caught arrives as INTERNAL.
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

// Registered so ReportFailure can classify without knowing which connector ran. Only gRPC
// evidence lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register("iceberg", classify) }

// classify reads the gRPC status from the Iceberg JVM, then the writer marker. Order matters: a
// JVM that is not listening explains a write failure better than the write failure does.
func classify(err error) *errs.Failure {
	if f := fromGRPCStatus(err); f != nil {
		return f
	}
	if f := fromJVMStart(err); f != nil {
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

// fromJVMStart classifies a JVM that died before it could serve. Same evidence as fromErrorInfo —
// the root cause's class — arriving on stderr because no gRPC channel exists yet to carry it.
func fromJVMStart(err error) *errs.Failure {
	if !errors.Is(err, errJVMStart) {
		return nil
	}
	// Killed, or dead without naming a cause: the phase is the only thing left to report.
	f := errs.Failure{Category: errs.Unclassified, ClassifiedBy: errs.ClassifiedByDefault, Code: codeJVMStartFailed}

	match := startupCause.FindStringSubmatch(err.Error())
	if match == nil {
		return &f
	}
	// The line carries the fully-qualified class; the table is keyed on the simple name.
	f.Code = match[1]
	if i := strings.LastIndex(f.Code, "."); i >= 0 {
		f.Code = f.Code[i+1:]
	}
	if category, mapped := javaExceptions[f.Code]; mapped {
		f.Category, f.ClassifiedBy = category, errs.ClassifiedByVendor
	}
	return &f
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

	// Read first: the status is INTERNAL for everything the JVM handles, so the detail is the
	// only thing that identifies which failure it was.
	if f := fromErrorInfo(grpcStatus); f != nil {
		return f
	}

	// Prefixed because it is the transport's answer, not Iceberg's: a bare "NotFound" would
	// read as a missing table when it means the gRPC method was not found.
	code := fmt.Sprintf("grpc.%s", grpcStatus.Code().String())

	if category, mapped := grpcCategories[grpcStatus.Code()]; mapped {
		return &errs.Failure{
			Category:     category,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         code,
		}
	}

	// Almost always Internal, which the JVM sends for everything it catches and so identifies
	// nothing. Unclassified keeps that share visible; any category would be wrong for most.
	return &errs.Failure{
		Category:     errs.Unclassified,
		ClassifiedBy: errs.ClassifiedByDefault,
		Code:         code,
	}
}

// fromErrorInfo reads the structured detail the Iceberg writer attaches to a failed call. Only
// our own domain is read, so another library's ErrorInfo cannot be mistaken for one of ours.
func fromErrorInfo(grpcStatus *status.Status) *errs.Failure {
	for _, detail := range grpcStatus.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != javaErrorDomain {
			continue
		}

		// Reason is the fully-qualified class; the table is keyed on the simple name.
		exception := info.GetReason()
		if i := strings.LastIndex(exception, "."); i >= 0 {
			exception = exception[i+1:]
		}

		// A JDBC SQLSTATE is more precise and shares a code space with the Postgres and Db2
		// sources; otherwise the exception names the failure.
		code := info.GetMetadata()[javaMetadataCode]
		if code == "" {
			code = exception
		}

		if category, mapped := javaExceptions[exception]; mapped {
			return &errs.Failure{
				Category:     category,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         code,
			}
		}
		// The JVM writes straight to object storage through its own AWS SDK, and raises a bare
		// S3Exception whatever the reason — the meaning is in the service code, the same one
		// the S3 source and the parquet destination read.
		if category, mapped := objstorage.ServiceCodeCategories[code]; mapped {
			return &errs.Failure{
				Category:     category,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         code,
			}
		}
		// A JDBC catalog raises classes this table cannot enumerate, but supplies a SQLSTATE
		// whose class is standardized — the same code space the SQL sources report.
		if category, mapped := sqlStateClasses[sqlStateClass(info.GetMetadata()[javaMetadataCode])]; mapped {
			return &errs.Failure{
				Category:     category,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         code,
			}
		}
		// No rule covers it yet. The class name is what a rule would be written from.
		return &errs.Failure{
			Category:     errs.Unclassified,
			ClassifiedBy: errs.ClassifiedByDefault,
			Code:         code,
			ErrorType:    info.GetReason(),
		}
	}
	return nil
}
