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
	"github.com/datazip-inc/olake/pkg/parser"
	"github.com/datazip-inc/olake/utils/errs"
	pq "github.com/parquet-go/parquet-go"
)

// Codes for conditions this driver detects itself, where no API error exists to read.
const (
	codeCDCUnsupported        = "s3.cdc_unsupported"
	codeUnsupportedFileFormat = "s3.unsupported_file_format"
	codeNoFilesForStream      = "s3.no_files_for_stream"
)

// Registered so ReportFailure can classify without knowing which connector ran. Only S3 evidence
// lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register("s3", classify) }

// classify reads S3's API code, the response status, or a decoder failure, returning nil for
// anything else. The category comes from the error, never the call site.
func classify(err error) *errs.Failure {
	if f := fromAPIError(err); f != nil {
		return f
	}
	if f := fromDecoder(err); f != nil {
		return f
	}

	// Checked last: a dropped connection and a throttled request fail inside the parser too,
	// and both have better answers than "the file is unreadable".
	if parser.IsDecodeFailure(err) {
		return &errs.Failure{Category: errs.SourceReadError, ClassifiedBy: errs.ClassifiedByPrecondition}
	}

	// objstorage joins this sentinel alongside the service error rather than replacing it, so
	// the checks above keep the service code. This covers one arriving alone.
	if errors.Is(err, objstorage.ErrNotFound) {
		return &errs.Failure{
			Category:     errs.ObjectNotFound,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         "object_not_found",
		}
	}
	return nil
}

// fromAPIError classifies anything the service answered with. The API code is preferred; where
// there is none — S3 answers several requests with a bare status and no body — the status
// stands in, which is also what makes S3-compatible endpoints classify at all.
func fromAPIError(err error) *errs.Failure {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return nil
	}

	code := apiErr.ErrorCode()
	if category, ok := objstorage.ServiceCodeCategories[code]; ok {
		return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
	}

	// Read through an interface, not a concrete type: the SDK has two response-error types and
	// both answer it.
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		if category := categoryForStatus(httpErr.HTTPStatusCode()); category != "" {
			if code == "" {
				// Prefixed so a bare status cannot be read as a service code; both share
				// one telemetry field.
				code = fmt.Sprintf("http_%d", httpErr.HTTPStatusCode())
			}
			return &errs.Failure{Category: category, ClassifiedBy: errs.ClassifiedByVendor, Code: code}
		}
	}

	// No mapping and no usable status; the code alone makes the gap actionable.
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

// fromDecoder classifies a file that arrived intact and could not be read. Keyed on the decoder's
// own types, never on "raised inside pkg/parser": a connection dropped mid-object fails inside
// the parser too, and a call-site rule would report a healthy file as corrupt.
func fromDecoder(err error) *errs.Failure {
	var (
		csvErr       *csv.ParseError
		jsonSyntax   *json.SyntaxError
		jsonType     *json.UnmarshalTypeError
		parquetValue *pq.ConvertError
	)
	switch {
	case errors.As(err, &csvErr),
		errors.As(err, &jsonSyntax),
		errors.As(err, &jsonType),
		errors.Is(err, gzip.ErrHeader), errors.Is(err, gzip.ErrChecksum),
		errors.Is(err, zip.ErrFormat), errors.Is(err, zip.ErrChecksum),
		errors.Is(err, pq.ErrCorrupted), errors.Is(err, pq.ErrMissingPageHeader),
		errors.Is(err, pq.ErrMissingRootColumn), errors.Is(err, pq.ErrSeekOutOfRange):
		return &errs.Failure{Category: errs.SourceReadError, ClassifiedBy: errs.ClassifiedByVendor}

	case errors.As(err, &parquetValue):
		// A value the file holds does not fit the column type it declares.
		return &errs.Failure{Category: errs.SchemaUnsupported, ClassifiedBy: errs.ClassifiedByVendor}
	}
	return nil
}
