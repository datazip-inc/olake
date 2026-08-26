package driver

import (
	"errors"
	"strconv"

	"github.com/datazip-inc/olake/utils/errs"
	mssql "github.com/microsoft/go-mssqldb"
)

// Codes for conditions this driver detects itself. Capture instances are catalog rows, so CDC
// prerequisites are validated up front and those failures carry no server error.
const (
	codeCDCNotEnabledOnTable  = "mssql.cdc_not_enabled_on_table"
	codeLSNBeforeCaptureStart = "mssql.lsn_before_capture_instance"
	codeLSNUnavailable        = "mssql.lsn_unavailable"
	codeMetadataStateInvalid  = "mssql.metadata_state_invalid"
)

// errorNumberCategories maps a SQL Server error number to a failure category. No code table
// ships with go-mssqldb, so every row comes from Microsoft's published list and is verifiable
// against the server's own sys.messages catalog view.
//
// Client-side numbers (-2, 10061) cannot arrive here: go-mssqldb reports a refused connection
// or an expired deadline as a Go error, never as a SQL Server error.
var errorNumberCategories = map[int32]errs.Category{
	// The server rejected the identity itself.
	18456: errs.AuthFailed, // Login failed for user '%s'
	18452: errs.AuthFailed, // Login failed; the login is from an untrusted domain
	18470: errs.AuthFailed, // Login failed; the account is disabled

	// The login succeeded and lacks a right.
	229: errs.PermissionDenied, // The %s permission was denied on the object
	230: errs.PermissionDenied, // The %s permission was denied on the column
	262: errs.PermissionDenied, // %s permission denied in database '%s'
	297: errs.PermissionDenied, // The user does not have permission to perform this action
	300: errs.PermissionDenied, // VIEW SERVER STATE permission was denied
	916: errs.PermissionDenied, // The server principal is not able to access the database

	// Missing, or not openable by this login.
	4060: errs.ObjectNotFound, // Cannot open database "%s" requested by the login
	208:  errs.ObjectNotFound, // Invalid object name '%s'
	207:  errs.ObjectNotFound, // Invalid column name '%s' — a column vanished from under a sync
	2812: errs.ObjectNotFound, // Could not find stored procedure '%s'

	// The SQL *OLake generated* is wrong. The user never writes it, so this is our defect.
	102: errs.SourceReadError, // Incorrect syntax near '%s'
	156: errs.SourceReadError, // Incorrect syntax near the keyword '%s'

	// Well-formed statement; the type is what the server cannot represent.
	245:  errs.SchemaUnsupported, // Conversion failed when converting the %s value
	8114: errs.SchemaUnsupported, // Error converting data type %s to %s

	// Two transactions collided; a retry usually clears it.
	1205: errs.ConcurrencyConflict, // Transaction was deadlocked and chosen as the deadlock victim
	1222: errs.ConcurrencyConflict, // Lock request time out period exceeded

	// Nothing is misconfigured; the server ran out.
	701:   errs.ResourceExhausted, // There is insufficient system memory to run this query
	1204:  errs.ResourceExhausted, // The instance cannot obtain a LOCK resource
	1105:  errs.ResourceExhausted, // Could not allocate space; the filegroup is full
	9002:  errs.ResourceExhausted, // The transaction log for database '%s' is full
	40501: errs.ResourceExhausted, // Azure SQL: the service is currently busy
	10928: errs.ResourceExhausted, // Azure SQL: resource limit for the database reached
	10929: errs.ResourceExhausted, // Azure SQL: minimum guarantee / limit exceeded

	// The server is going away, or the replica is not serving — same remedy as unreachable.
	6005:  errs.NetworkUnreachable, // SHUTDOWN is in progress
	40613: errs.NetworkUnreachable, // Azure SQL: database is currently unavailable
	976:   errs.NetworkUnreachable, // The target database is in an availability group and not available for connections
}

// Registered so ReportFailure can classify without knowing which connector ran. Only SQL Server
// evidence lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register("mssql", classify) }

// classify reads SQL Server's error number, returning nil for anything else so the shared rules
// get their chance. The category comes from the error, never the call site.
func classify(err error) *errs.Failure {
	// A value type, so errors.As gets a value target. Reachable through the driver's own
	// ServerError and RetryableError wrappers, both of which implement Unwrap.
	var serverErr mssql.Error
	if !errors.As(err, &serverErr) {
		return nil
	}

	// The number travels whether or not it is mapped.
	f := errs.Failure{Code: strconv.FormatInt(int64(serverErr.Number), 10)}
	if category, ok := errorNumberCategories[serverErr.Number]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real error number with no rule yet; the number alone makes the gap actionable.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
