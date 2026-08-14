package parquet

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/utils/errs"
)

// Codes for conditions this writer detects itself, where no vendor error exists to read.
const codeNoDestinationConfigured = "parquet.no_destination_configured"

// s3CodeCategories maps an S3 API error code to a failure category.
//
// The S3 source driver carries the same codes, in its own module and against SDK v2. This one
// reads SDK v1 (awserr.Error), whose surface includes the four entries at the bottom that v2 has
// no counterpart for.
var s3CodeCategories = map[string]errs.Category{
	"InvalidAccessKeyId":    errs.AuthFailed,
	"SignatureDoesNotMatch": errs.AuthFailed,
	"InvalidSecurity":       errs.AuthFailed,
	"ExpiredToken":          errs.AuthFailed,
	"InvalidToken":          errs.AuthFailed,
	"TokenRefreshRequired":  errs.AuthFailed,

	"AccessDenied":      errs.PermissionDenied,
	"AllAccessDisabled": errs.PermissionDenied,
	"Forbidden":         errs.PermissionDenied,

	"NoSuchBucket": errs.ObjectNotFound,
	"NoSuchKey":    errs.ObjectNotFound,
	"NotFound":     errs.ObjectNotFound,

	"PermanentRedirect":                  errs.ConfigInvalid,
	"AuthorizationHeaderMalformed":       errs.ConfigInvalid,
	"IllegalLocationConstraintException": errs.ConfigInvalid,
	"InvalidBucketName":                  errs.ConfigInvalid,

	"SlowDown":             errs.ResourceExhausted,
	"RequestLimitExceeded": errs.ResourceExhausted,
	"ThrottlingException":  errs.ResourceExhausted,

	"ServiceUnavailable": errs.NetworkUnreachable,
	"InternalError":      errs.NetworkUnreachable,

	"RequestTimeout": errs.Timeout,

	// v1-only. The SDK raises these before any request leaves the process, so they have no
	// HTTP status and no v2 counterpart.
	"NoCredentialProviders": errs.AuthFailed,    // the credential chain found nothing
	"MissingRegion":         errs.ConfigInvalid, // no region configured or inferable
	"MissingEndpoint":       errs.ConfigInvalid,
	"RequestCanceled":       errs.Canceled,
}

// Register so ReportFailure can classify without knowing which connector ran. Only S3 and filesystem
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads S3 (aws-sdk-go v1) and the local filesystem, then the writer marker.
//
// Order is the substance here: a parquet write can fail because the disk filled, because S3
// refused the credentials, or because the encoder could not represent a value. Only the last is
// a write error, so the specific causes are checked first.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	if f := fromAWSError(err, 0); f != nil {
		return f
	}
	if f := fromFilesystem(err); f != nil {
		return f
	}
	// Nothing more specific explained it, and the failure came from the writer itself.
	if destination.IsWriteFailure(err) {
		return &errs.Failure{
			Category:     errs.DestinationWriteError,
			ClassifiedBy: errs.ClassifiedByPrecondition,
		}
	}
	return nil
}

// maxOrigErrDepth bounds the walk through OrigErr. The SDK nests at most a couple of layers;
// the limit exists only so a self-referencing error cannot recurse without end.
const maxOrigErrDepth = 8

// fromAWSError classifies an aws-sdk-go v1 error.
//
// v1 predates error wrapping: awserr.Error exposes its cause through OrigErr() and implements no
// Unwrap, so errors.As cannot see past it. Every DNS, TLS and refused-connection failure on this
// path sits inside an error the shared rules cannot open.
//
// An unmapped code therefore has its cause pulled out by hand and put back through the public
// classifier, which is the only way those rules reach it.
func fromAWSError(err error, depth int) *errs.Failure {
	var awsErr awserr.Error
	if !errors.As(err, &awsErr) {
		return nil
	}

	if category, ok := s3CodeCategories[awsErr.Code()]; ok {
		return &errs.Failure{
			Category:     category,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         awsErr.Code(),
		}
	}

	// The status is coarser than a code but never absent on a response error, and it is what
	// lets S3-compatible endpoints classify at all.
	var requestFailure awserr.RequestFailure
	if errors.As(err, &requestFailure) {
		if category := categoryForStatus(requestFailure.StatusCode()); category != "" {
			code := awsErr.Code()
			if code == "" {
				// A bare status carries the http_ prefix so it cannot be read as a service
				// code; both share one telemetry field.
				code = fmt.Sprintf("http_%d", requestFailure.StatusCode())
			}
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	// Neither a known code nor a usable status. Open the error by hand and classify what is
	// underneath — a socket error, a certificate failure, a resolver answer.
	if cause := awsErr.OrigErr(); cause != nil && depth < maxOrigErrDepth {
		if f := fromAWSError(cause, depth+1); f != nil {
			return f
		}
		if underlying := errs.From(errs.Classify(cause)); underlying.Category != errs.Unclassified {
			return &underlying
		}
	}

	// A service code with nothing else to say about it. The code identifies the gap.
	return &errs.Failure{
		Category:     errs.Unclassified,
		ClassifiedBy: errs.ClassifiedByDefault,
		Code:         awsErr.Code(),
	}
}

// fromFilesystem classifies a failure writing to local disk, which is the other half of what
// this writer does: with no S3 config it writes parquet files to a path the user gave.
//
// These are all standard-library values, so they cannot be confused with a service error.
func fromFilesystem(err error) *errs.Failure {
	var category errs.Category
	var code string

	// The errno cases come first: os wraps an errno in a *PathError that can also satisfy
	// fs.ErrPermission, and the errno is the more specific answer.
	switch {
	case errors.Is(err, syscall.ENOSPC):
		category, code = errs.ResourceExhausted, "no_space_left"
	case errors.Is(err, syscall.EDQUOT):
		category, code = errs.ResourceExhausted, "quota_exceeded"
	case errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE):
		category, code = errs.ResourceExhausted, "too_many_open_files"
	case errors.Is(err, syscall.EROFS):
		category, code = errs.PermissionDenied, "read_only_filesystem"
	case errors.Is(err, fs.ErrPermission):
		category, code = errs.PermissionDenied, "permission_denied"
	case errors.Is(err, fs.ErrNotExist):
		category, code = errs.ConfigInvalid, "path_not_found"
	default:
		return nil
	}
	return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByStdlib, Code: code}
}

// categoryForStatus maps an HTTP response status to a category, or "" where the status says
// nothing useful on its own. Matches the S3 source, so a status means the same thing on both
// sides of a sync.
func categoryForStatus(status int) errs.Category {
	switch {
	case status == 401:
		return errs.AuthFailed
	case status == 403:
		return errs.PermissionDenied
	case status == 404:
		return errs.ObjectNotFound
	case status == 408:
		return errs.Timeout
	case status == 429:
		return errs.ResourceExhausted
	case status >= 500:
		return errs.NetworkUnreachable
	}
	return ""
}
