package driver

import (
	"archive/zip"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/smithy-go"
	"github.com/datazip-inc/olake/pkg/objstorage"
	"github.com/datazip-inc/olake/utils/errs"
)

// Codes for conditions this driver detects itself, where no API error exists to read.
const (
	codeCDCUnsupported        = "s3.cdc_unsupported"
	codeUnsupportedFileFormat = "s3.unsupported_file_format"
	codeNoFilesForStream      = "s3.no_files_for_stream"
)

// apiCodeCategories maps an S3 API error code to a failure category.
//
// The AWS SDK routes every service error through one interface, smithy.APIError, whose
// ErrorCode() returns the string S3 put in the response. That is the service's own identifier,
// so it is reported unchanged.
//
//	https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
var apiCodeCategories = map[string]errs.Category{
	// The request was not signed by anyone the service recognizes.
	"InvalidAccessKeyId":    errs.AuthFailed,
	"SignatureDoesNotMatch": errs.AuthFailed,
	"InvalidSecurity":       errs.AuthFailed,
	"ExpiredToken":          errs.AuthFailed,
	"InvalidToken":          errs.AuthFailed,
	"TokenRefreshRequired":  errs.AuthFailed,

	// The identity is known and the policy says no.
	"AccessDenied":      errs.PermissionDenied,
	"AllAccessDisabled": errs.PermissionDenied,
	"Forbidden":         errs.PermissionDenied, // HeadBucket answers 403 with no body

	// The object does not exist.
	"NoSuchBucket": errs.ObjectNotFound,
	"NoSuchKey":    errs.ObjectNotFound,
	"NotFound":     errs.ObjectNotFound, // HeadBucket answers 404 with no body

	// The request reached the wrong endpoint for this bucket, or named it illegally. Both are
	// fixed in the config, not on the service.
	"PermanentRedirect":                  errs.ConfigInvalid,
	"AuthorizationHeaderMalformed":       errs.ConfigInvalid,
	"IllegalLocationConstraintException": errs.ConfigInvalid,
	"InvalidBucketName":                  errs.ConfigInvalid,

	// Throttled. Nothing is wrong; the request rate was too high.
	"SlowDown":             errs.ResourceExhausted,
	"RequestLimitExceeded": errs.ResourceExhausted,
	"ThrottlingException":  errs.ResourceExhausted,

	// The service is not serving the request.
	"ServiceUnavailable": errs.NetworkUnreachable,
	"InternalError":      errs.NetworkUnreachable,

	"RequestTimeout": errs.Timeout,
}

// Register so ReportFailure can classify without knowing which connector ran. Only S3 evidence
// is handled here — DNS, TLS, refused connections and deadlines look the same for every
// connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads S3's API code, the response status, or a decoder failure. Returns nil for
// anything else.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// policy, a deleted key, throttling or a dropped connection.
func classify(err error) *errs.Failure {
	if f := fromAPIError(err); f != nil {
		return f
	}
	if f := fromDecoder(err); f != nil {
		return f
	}

	// pkg/objstorage joins its own sentinel alongside the service error rather than replacing
	// it, so the checks above answer first and keep the service code. This covers a sentinel
	// that arrives alone.
	if errors.Is(err, objstorage.ErrNotFound) {
		return &errs.Failure{
			Category:     errs.ObjectNotFound,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         "object_not_found",
		}
	}
	return nil
}

// fromAPIError classifies anything the service answered with.
//
// Two layers, in order. The API code is preferred because it is specific. Where there is none —
// S3 answers several requests with a bare status and no body, and S3-compatible services are
// looser still — the response status stands in.
func fromAPIError(err error) *errs.Failure {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return nil
	}

	code := apiErr.ErrorCode()
	if category, ok := apiCodeCategories[code]; ok {
		return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
	}

	// The status is read through an interface rather than a concrete type: the SDK has two
	// response-error types, in smithy and in the AWS transport layer, and both answer it.
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		if category := categoryForStatus(httpErr.HTTPStatusCode()); category != "" {
			if code == "" {
				// A bare status carries the http_ prefix so it cannot be read as a service
				// code; both share one telemetry field.
				code = fmt.Sprintf("http_%d", httpErr.HTTPStatusCode())
			}
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	// A service code with no mapping and no usable status. The code identifies the gap, so it
	// travels without a category being guessed at.
	return &errs.Failure{Category: errs.Unclassified, ClassifiedBy: errs.ClassifiedByDefault, Code: code}
}

// categoryForStatus maps an HTTP response status to a category, or "" where the status says
// nothing useful on its own.
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

// fromDecoder classifies a file that arrived intact and could not be read.
//
// Keyed on the decoder's own types, never on "raised inside pkg/parser": a connection dropped
// mid-object fails inside the parser too, and a call-site rule would report a healthy file as
// corrupt. These types are produced only by a decoder, so the two cannot be confused.
//
// No code accompanies them — nothing issued one.
func fromDecoder(err error) *errs.Failure {
	var (
		csvErr     *csv.ParseError
		jsonSyntax *json.SyntaxError
		jsonType   *json.UnmarshalTypeError
	)
	switch {
	case errors.As(err, &csvErr),
		errors.As(err, &jsonSyntax),
		errors.As(err, &jsonType),
		errors.Is(err, gzip.ErrHeader), errors.Is(err, gzip.ErrChecksum),
		errors.Is(err, zip.ErrFormat), errors.Is(err, zip.ErrChecksum):
		return &errs.Failure{Category: errs.SourceReadError, ClassifiedBy: errs.ClassifiedByVendor}
	}
	return nil
}
