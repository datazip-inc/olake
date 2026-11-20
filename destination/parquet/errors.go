package parquet

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils/errs"
)

// Codes for conditions this writer detects itself, where no vendor error exists to read.
const codeNoDestinationConfigured = "parquet.no_destination_configured"

// Registered so ReportFailure can classify without knowing which connector ran. Only S3 and
// filesystem evidence lives here; DNS, TLS and socket failures belong to utils/errs.
func init() { errs.Register("parquet", classify) }

// classify reads S3 (aws-sdk-go-v2) and the local filesystem, then the writer marker. Order is
// the substance: a write can fail on a full disk, refused credentials or an unencodable value,
// and only the last is a write error, so the specific causes are checked first.
func classify(err error) *errs.Failure {
	if f := fromS3Error(err); f != nil {
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

// fromS3Error classifies an aws-sdk-go-v2 error. Service codes and HTTP statuses come from the
// smithy chain, which unwraps, so a DNS or TLS failure under a request is left for the shared
// rules when this returns nil.
func fromS3Error(err error) *errs.Failure {
	var apiErr smithy.APIError
	hasAPI := errors.As(err, &apiErr)
	if hasAPI {
		if category, ok := objstorage.ServiceCodeCategories[apiErr.ErrorCode()]; ok {
			return &errs.Failure{
				Category:     category,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         apiErr.ErrorCode(),
			}
		}
	}

	// Coarser than a code but present on a response, and what lets S3-compatible
	// endpoints classify at all.
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		if category := categoryForStatus(respErr.HTTPStatusCode()); category != "" {
			code := ""
			if hasAPI {
				code = apiErr.ErrorCode()
			}
			if code == "" {
				// Prefixed so a bare status cannot be read as a service code; both share
				// one telemetry field.
				code = fmt.Sprintf("http_%d", respErr.HTTPStatusCode())
			}
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	if !hasAPI {
		return nil
	}

	// A service code the map does not name. The chain still unwraps, so a canceled or
	// unreachable cause under it keeps the more specific category.
	if underlying := errs.Standard(err); underlying.Category != errs.Unclassified {
		return &underlying
	}

	return &errs.Failure{
		Category:     errs.Unclassified,
		ClassifiedBy: errs.ClassifiedByDefault,
		Code:         apiErr.ErrorCode(),
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
