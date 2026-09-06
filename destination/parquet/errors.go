package parquet

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils/errs"
)

// Codes for conditions this writer detects itself, where no vendor error exists to read.
const codeNoDestinationConfigured = "parquet.no_destination_configured"

// Registered so ReportFailure can classify without knowing which connector ran. Only S3 and
// filesystem evidence lives here; DNS, TLS and socket failures belong to utils/errs.
func init() { errs.Register("parquet", classify) }

// classify reads S3 (aws-sdk-go v1) and the local filesystem, then the writer marker. Order is
// the substance: a write can fail on a full disk, refused credentials or an unencodable value,
// and only the last is a write error, so the specific causes are checked first.
func classify(err error) *errs.Failure {
	if f := fromAWSError(err, 0); f != nil {
		return f
	}
	if f := fromFilesystem(err); f != nil {
		return f
	}
	// Nothing more specific explained it, and the writer raised it.
	if destination.IsWriteFailure(err) {
		return &errs.Failure{
			Category:     errs.DestinationWriteError,
			ClassifiedBy: errs.ClassifiedByPrecondition,
		}
	}
	return nil
}

// maxOrigErrDepth bounds the OrigErr walk so a self-referencing error cannot recurse without end.
const maxOrigErrDepth = 8

// fromAWSError classifies an aws-sdk-go v1 error. v1 predates error wrapping: the cause sits
// behind OrigErr() with no Unwrap, so errors.As cannot reach the DNS, TLS and socket failures
// underneath. An unmapped code has its cause pulled out by hand and sent to the shared rules.
func fromAWSError(err error, depth int) *errs.Failure {
	var awsErr awserr.Error
	if !errors.As(err, &awsErr) {
		return nil
	}

	if category, ok := objstorage.ServiceCodeCategories[awsErr.Code()]; ok {
		return &errs.Failure{
			Category:     category,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         awsErr.Code(),
		}
	}

	// Coarser than a code but never absent on a response error, and what lets S3-compatible
	// endpoints classify at all.
	var requestFailure awserr.RequestFailure
	if errors.As(err, &requestFailure) {
		if category := categoryForStatus(requestFailure.StatusCode()); category != "" {
			code := awsErr.Code()
			if code == "" {
				// Prefixed so a bare status cannot be read as a service code; both share
				// one telemetry field.
				code = fmt.Sprintf("http_%d", requestFailure.StatusCode())
			}
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	// Neither a known code nor a usable status: open the error and classify what is under it.
	if cause := awsErr.OrigErr(); cause != nil && depth < maxOrigErrDepth {
		if f := fromAWSError(cause, depth+1); f != nil {
			return f
		}
		if underlying := errs.Standard(cause); underlying.Category != errs.Unclassified {
			return &underlying
		}
	}

	// A service code with nothing else to say about it.
	return &errs.Failure{
		Category:     errs.Unclassified,
		ClassifiedBy: errs.ClassifiedByDefault,
		Code:         awsErr.Code(),
	}
}

// fromFilesystem classifies a failure writing to local disk, the other half of what this writer
// does. All standard-library values, so they cannot be confused with a service error.
func fromFilesystem(err error) *errs.Failure {
	var category errs.Category
	var code string

	// Errno first: os wraps one in a *PathError that can also satisfy fs.ErrPermission, and the
	// errno is the more specific answer.
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

// categoryForStatus maps an HTTP response status to a category, or "" where it says nothing
// useful. Matches the S3 source, so a status means the same thing on both sides of a sync.
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
