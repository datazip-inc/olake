package driver

import (
	"errors"
	"strconv"

	"github.com/datazip-inc/olake/utils/errs"
	mssql "github.com/microsoft/go-mssqldb"
)

// Codes for conditions this driver detects itself. SQL Server exposes capture instances as
// catalog rows, so CDC prerequisites are validated up front and those failures have no server
// error to read.
const (
	codeCDCNotEnabledOnTable  = "mssql.cdc_not_enabled_on_table"
	codeLSNBeforeCaptureStart = "mssql.lsn_before_capture_instance"
	codeLSNUnavailable        = "mssql.lsn_unavailable"
	codeMetadataStateInvalid  = "mssql.metadata_state_invalid"
)

// errorNumberCategories maps a SQL Server error number to a failure category.
//
// Unlike the other drivers, no code table ships with go-mssqldb — it carries the number through
// and defines none of them. Every row here comes from Microsoft's published list, and every row
// can be re-checked against the server itself with the sys.messages query in
// the sys.messages catalog view.
//
// Client-side numbers cannot arrive here: go-mssqldb reports a refused connection or an
// expired deadline as a Go error, never as a SQL Server error, so -2 and 10061 cannot arrive.
var errorNumberCategories = map[int32]errs.Category{
	// Login. The server rejected the identity itself.
	18456: errs.AuthFailed, // Login failed for user '%s'
	18452: errs.AuthFailed, // Login failed; the login is from an untrusted domain
	18470: errs.AuthFailed, // Login failed; the account is disabled

	// Authorization. The login succeeded and lacks a right.
	229: errs.PermissionDenied, // The %s permission was denied on the object
	230: errs.PermissionDenied, // The %s permission was denied on the column
	262: errs.PermissionDenied, // %s permission denied in database '%s'
	297: errs.PermissionDenied, // The user does not have permission to perform this action
	300: errs.PermissionDenied, // VIEW SERVER STATE permission was denied
	916: errs.PermissionDenied, // The server principal is not able to access the database

	// The object does not exist, or cannot be opened by this login.
	4060: errs.ObjectNotFound, // Cannot open database "%s" requested by the login
	208:  errs.ObjectNotFound, // Invalid object name '%s'
	207:  errs.ObjectNotFound, // Invalid column name '%s' — a column vanished from under a sync
	2812: errs.ObjectNotFound, // Could not find stored procedure '%s'

	// The SQL *OLake generated* is wrong. The user never writes this SQL, so a failure here
	// is ours, not a misconfiguration — which is why they are source_read_error (owner:
	// olake) rather than config_invalid.
	102: errs.SourceReadError, // Incorrect syntax near '%s'
	156: errs.SourceReadError, // Incorrect syntax near the keyword '%s'

	// A value the server cannot represent in the type OLake asked for. The statement is
	// well-formed; the type is the problem.
	245:  errs.SchemaUnsupported, // Conversion failed when converting the %s value
	8114: errs.SchemaUnsupported, // Error converting data type %s to %s

	// Contention. Two transactions collided; retrying usually works.
	1205: errs.ConcurrencyConflict, // Transaction was deadlocked and chosen as the deadlock victim
	1222: errs.ConcurrencyConflict, // Lock request time out period exceeded

	// Capacity. Nothing is misconfigured; the server ran out of something.
	701:   errs.ResourceExhausted, // There is insufficient system memory to run this query
	1204:  errs.ResourceExhausted, // The instance cannot obtain a LOCK resource
	1105:  errs.ResourceExhausted, // Could not allocate space; the filegroup is full
	9002:  errs.ResourceExhausted, // The transaction log for database '%s' is full
	40501: errs.ResourceExhausted, // Azure SQL: the service is currently busy
	10928: errs.ResourceExhausted, // Azure SQL: resource limit for the database reached
	10929: errs.ResourceExhausted, // Azure SQL: minimum guarantee / limit exceeded

	// The server is going away, or the replica we connected to is not serving. From here
	// that is indistinguishable from unreachable, and the remedy is the same.
	6005:  errs.NetworkUnreachable, // SHUTDOWN is in progress
	40613: errs.NetworkUnreachable, // Azure SQL: database is currently unavailable
	976:   errs.NetworkUnreachable, // The target database is in an availability group and not available for connections
}

// Register so ReportFailure can classify without knowing which connector ran. Only SQL Server
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads SQL Server's error number. Returns nil for anything else, leaving it to the
// shared standard-library rules.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	// mssql.Error is a value type, so errors.As is given a value target. It is reachable
	// through the driver's own ServerError and RetryableError wrappers, both of which
	// implement Unwrap.
	var serverErr mssql.Error
	if !errors.As(err, &serverErr) {
		return nil
	}

	// The server answered and said what was wrong. Its number travels with the failure
	// whether or not it is mapped, so an unmapped number is still identifiable.
	f := errs.Failure{Code: strconv.FormatInt(int64(serverErr.Number), 10)}
	if category, ok := errorNumberCategories[serverErr.Number]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real error number with no mapping yet. The number identifies it precisely, so the gap
	// is actionable without guessing at a category.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
