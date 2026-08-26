package driver

import (
	"errors"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/jackc/pgx/v5/pgconn"
)

// Codes for conditions this driver detects itself, where the server never named the failure.
const (
	codeReplicationSlotMissing = "postgres.replication_slot_missing"
	codeCDCWaitTimeTooLow      = "postgres.cdc_initial_wait_time_too_low"
)

// sqlStateCategories maps a PostgreSQL SQLSTATE to a failure category. SQLSTATE rather than the
// message: stable across versions, locale-independent, and free of user data. Codes travel
// verbatim; trailing names are the condition names from postgres src/backend/utils/errcodes.txt.
var sqlStateCategories = map[string]errs.Category{
	"28P01": errs.AuthFailed, // invalid_password
	"28000": errs.AuthFailed, // invalid_authorization_specification

	// Missing object, or a role that cannot use it. Authentication already succeeded.
	"3D000": errs.ObjectNotFound,   // invalid_catalog_name, alias undefined_database
	"3F000": errs.ObjectNotFound,   // invalid_schema_name, alias undefined_schema
	"42P01": errs.ObjectNotFound,   // undefined_table
	"42703": errs.ObjectNotFound,   // undefined_column — a column vanished from under a running sync
	"42501": errs.PermissionDenied, // insufficient_privilege

	// Also class 42, but these mean the SQL *OLake generated* is wrong. The user never writes
	// it, so this is our defect rather than a misconfiguration.
	"42601": errs.SourceReadError, // syntax_error
	"42883": errs.SourceReadError, // undefined_function
	"42702": errs.SourceReadError, // ambiguous_column
	"42P10": errs.SourceReadError, // invalid_column_reference
	"42P08": errs.SourceReadError, // ambiguous_parameter
	"42809": errs.SourceReadError, // wrong_object_type

	// Well-formed statement; the type is what the server cannot handle.
	"42804": errs.SchemaUnsupported, // datatype_mismatch
	"42846": errs.SchemaUnsupported, // cannot_coerce
	"42P18": errs.SchemaUnsupported, // indeterminate_datatype

	"53300": errs.ResourceExhausted, // too_many_connections
	"53100": errs.ResourceExhausted, // disk_full
	"53200": errs.ResourceExhausted, // out_of_memory
	"54000": errs.ResourceExhausted, // program_limit_exceeded

	// 55000 is deliberately unmapped: it covers replication preconditions and unrelated features
	// alike, separable only by message text. It still reaches telemetry with its SQLSTATE.
	"55006": errs.ConcurrencyConflict, // object_in_use — another process holds the slot
	"55P03": errs.ConcurrencyConflict, // lock_not_available

	// Two transactions collided; a retry usually clears it.
	"40001": errs.ConcurrencyConflict, // serialization_failure
	"40P01": errs.ConcurrencyConflict, // deadlock_detected

	// The server is going away or not yet accepting work — same remedy as unreachable.
	"57P01": errs.NetworkUnreachable, // admin_shutdown
	"57P03": errs.NetworkUnreachable, // cannot_connect_now

	"08006": errs.NetworkUnreachable, // connection_failure
	"08001": errs.NetworkUnreachable, // sqlclient_unable_to_establish_sqlconnection
	"08004": errs.NetworkUnreachable, // sqlserver_rejected_establishment_of_sqlconnection
	"08003": errs.NetworkUnreachable, // connection_does_not_exist
	"08007": errs.NetworkUnreachable, // transaction_resolution_unknown

	"XX000": errs.InternalError, // internal_error — the server hit a bug
}

// Registered so ReportFailure can classify without knowing which connector ran. Only PostgreSQL
// evidence lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register("postgres", classify) }

// classify reads PostgreSQL's SQLSTATE, returning nil for anything else so the shared rules get
// their chance. The category comes from the error, never the call site: one call can fail on a
// revoked grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil
	}

	// The code travels whether or not it is mapped.
	f := errs.Failure{Code: pgErr.Code}
	if category, ok := sqlStateCategories[pgErr.Code]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real SQLSTATE with no rule yet; the code alone makes the gap actionable.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
