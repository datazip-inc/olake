package objstorage

import "github.com/datazip-inc/olake/utils/errs"

// ServiceCodeCategories maps the error code S3 returns to a failure category. The code is the
// service's own identifier, reported unchanged:
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
//
// Shared by all three components that reach S3 — the source (SDK v2), the parquet destination
// (SDK v1) and the Iceberg JVM — and by S3-compatible services, which answer with the same codes.
var ServiceCodeCategories = map[string]errs.Category{
	// Nobody the service recognizes signed the request.
	"InvalidAccessKeyId":    errs.AuthFailed,
	"SignatureDoesNotMatch": errs.AuthFailed,
	"InvalidSecurity":       errs.AuthFailed,
	"ExpiredToken":          errs.AuthFailed,
	"InvalidToken":          errs.AuthFailed,
	"TokenRefreshRequired":  errs.AuthFailed,

	// The identity is known; the policy says no.
	"AccessDenied":      errs.PermissionDenied,
	"AllAccessDisabled": errs.PermissionDenied,
	"Forbidden":         errs.PermissionDenied, // HeadBucket answers 403 with no body

	"NoSuchBucket": errs.ObjectNotFound,
	"NoSuchKey":    errs.ObjectNotFound,
	"NotFound":     errs.ObjectNotFound, // HeadBucket answers 404 with no body

	// Wrong endpoint for this bucket, or an illegal name. Both are fixed in the config.
	"PermanentRedirect":                  errs.ConfigInvalid,
	"AuthorizationHeaderMalformed":       errs.ConfigInvalid,
	"IllegalLocationConstraintException": errs.ConfigInvalid,
	"InvalidBucketName":                  errs.ConfigInvalid,

	// Throttled, not broken.
	"SlowDown":             errs.ResourceExhausted,
	"RequestLimitExceeded": errs.ResourceExhausted,
	"ThrottlingException":  errs.ResourceExhausted,

	// The service itself is down or erroring.
	"ServiceUnavailable": errs.NetworkUnreachable,
	"InternalError":      errs.NetworkUnreachable,

	"RequestTimeout": errs.Timeout,

	// SDK v1 only: raised before any request leaves the process, so no HTTP status, no v2 twin.
	"NoCredentialProviders": errs.AuthFailed,    // the credential chain found nothing
	"MissingRegion":         errs.ConfigInvalid, // no region configured or inferable
	"MissingEndpoint":       errs.ConfigInvalid,
	"RequestCanceled":       errs.Canceled,
}
