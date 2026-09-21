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

	// Account-wide codes, returned by any AWS service rather than by S3. A Glue or Lake Formation
	// catalog reports credential failures with these, under a base *Exception class that names
	// nothing on its own.
	"UnrecognizedClientException": errs.AuthFailed, // the security token is not valid
	"InvalidClientTokenId":        errs.AuthFailed,
	"ExpiredTokenException":       errs.AuthFailed,
	"IncompleteSignature":         errs.AuthFailed,
	"MissingAuthenticationToken":  errs.AuthFailed,
	"AuthFailure":                 errs.AuthFailed,
	"AccessDeniedException":       errs.PermissionDenied,
	"EntityNotFoundException":     errs.ObjectNotFound, // Glue: no such database or table

	// Glue catalog codes, returned under GlueException or as a dedicated SDK class.
	"ConcurrentModificationException":      errs.ConcurrencyConflict,
	"InvalidInputException":                errs.ConfigInvalid,
	"InternalServiceException":             errs.NetworkUnreachable,
	"InternalServerException":              errs.NetworkUnreachable,
	"OperationTimeoutException":            errs.Timeout,
	"ResourceNumberLimitExceededException": errs.ResourceExhausted,
	"VersionMismatchException":             errs.ConcurrencyConflict,
	"AlreadyExistsException":               errs.CatalogError,
	"GlueEncryptionException":              errs.PermissionDenied,
	"OperationNotSupportedException":       errs.UnsupportedFeature,
	"ResourceNotFoundException":            errs.ObjectNotFound,
	"ValidationException":                  errs.ConfigInvalid,

	// SDK v1 only: raised before any request leaves the process, so no HTTP status, no v2 twin.
	"NoCredentialProviders": errs.AuthFailed,    // the credential chain found nothing
	"MissingRegion":         errs.ConfigInvalid, // no region configured or inferable
	"MissingEndpoint":       errs.ConfigInvalid,
	"RequestCanceled":       errs.Canceled,
}
