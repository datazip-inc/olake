package driver

import (
	"errors"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/jackc/pgx/v5/pgconn"
)

// Codes for conditions this driver detects itself. A code is only worth having where the server
// did not already name the failure; everywhere else the SQLSTATE is reported as-is.
const (
	codeReplicationSlotMissing = "postgres.replication_slot_missing"
	codeCDCWaitTimeTooLow      = "postgres.cdc_initial_wait_time_too_low"
)

// sqlStateCategories maps a PostgreSQL SQLSTATE to a failure category. Comments give the
// condition name exactly as errcodes.txt spells it — the file PostgreSQL generates both its C
// macros and its published error table from.
//
//	https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/utils/errcodes.txt
//
// SQLSTATE rather than the message: stable across versions, unaffected by locale, and carries
// nothing about the user. It is reported verbatim, so an unmapped code stays identifiable.
var sqlStateCategories = map[string]errs.Category{
	// Class 28 — Invalid Authorization Specification.
	"28P01": errs.AuthFailed, // invalid_password
	"28000": errs.AuthFailed, // invalid_authorization_specification

	// Class 3D / 3F / 42 — the object does not exist, or the role cannot use it.
	// Authentication has already succeeded by this point.
	"3D000": errs.ObjectNotFound,   // invalid_catalog_name, alias undefined_database
	"3F000": errs.ObjectNotFound,   // invalid_schema_name, alias undefined_schema
	"42P01": errs.ObjectNotFound,   // undefined_table
	"42703": errs.ObjectNotFound,   // undefined_column — a column vanished from under a running sync
	"42501": errs.PermissionDenied, // insufficient_privilege

	// Also Class 42, but these mean the SQL *OLake generated* is wrong. The user never
	// writes this SQL, so a failure here is ours, not a misconfiguration — which is why
	// they are source_read_error (owner: olake) rather than config_invalid.
	"42601": errs.SourceReadError, // syntax_error
	"42883": errs.SourceReadError, // undefined_function
	"42702": errs.SourceReadError, // ambiguous_column
	"42P10": errs.SourceReadError, // invalid_column_reference
	"42P08": errs.SourceReadError, // ambiguous_parameter
	"42809": errs.SourceReadError, // wrong_object_type

	// A type OLake asked the server to compare or convert in a way it cannot. Distinct from
	// the ones above: the statement is well-formed, the type is the problem.
	"42804": errs.SchemaUnsupported, // datatype_mismatch
	"42846": errs.SchemaUnsupported, // cannot_coerce
	"42P18": errs.SchemaUnsupported, // indeterminate_datatype

	// Class 53 — Insufficient Resources, Class 54 — Program Limit Exceeded.
	"53300": errs.ResourceExhausted, // too_many_connections
	"53100": errs.ResourceExhausted, // disk_full
	"53200": errs.ResourceExhausted, // out_of_memory
	"54000": errs.ResourceExhausted, // program_limit_exceeded

	// Class 55 — Object Not In Prerequisite State.
	//
	// 55000 is not mapped: it covers replication preconditions ("logical replication slot %s
	// exists, but wal_level < logical", slot.c) and unrelated features alike, and only the
	// message text separates them. It still reaches telemetry with its SQLSTATE.
	//
	// 55006 is "replication slot %s is active for PID %d" (slot.c): another process holds the
	// slot. Nothing ran out, so it is contention, and it clears when that run finishes.
	"55006": errs.ConcurrencyConflict, // object_in_use
	"55P03": errs.ConcurrencyConflict, // lock_not_available

	// Class 40 — Transaction Rollback. Two transactions collided; retrying usually works.
	"40001": errs.ConcurrencyConflict, // serialization_failure
	"40P01": errs.ConcurrencyConflict, // deadlock_detected

	// Class 57 — Operator Intervention. The server is going away or not yet accepting
	// work; from here that is indistinguishable from unreachable, same remedy.
	"57P01": errs.NetworkUnreachable, // admin_shutdown
	"57P03": errs.NetworkUnreachable, // cannot_connect_now

	// Class 08 — Connection Exception.
	"08006": errs.NetworkUnreachable, // connection_failure
	"08001": errs.NetworkUnreachable, // sqlclient_unable_to_establish_sqlconnection
	"08004": errs.NetworkUnreachable, // sqlserver_rejected_establishment_of_sqlconnection
	"08003": errs.NetworkUnreachable, // connection_does_not_exist
	"08007": errs.NetworkUnreachable, // transaction_resolution_unknown

	// Class XX — the server hit a bug. Not used to infer anything more specific, which
	// would require reading the message.
	"XX000": errs.InternalError, // internal_error
}

// Register so ReportFailure can classify without knowing which connector ran. Only PostgreSQL
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads PostgreSQL's SQLSTATE. Returns nil for anything else, leaving it to the
// shared standard-library rules.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil
	}

	// The server answered and said what was wrong. Its SQLSTATE travels with the failure
	// whether or not it is mapped, so an unmapped code is still identifiable.
	f := errs.Failure{Code: pgErr.Code}
	if category, ok := sqlStateCategories[pgErr.Code]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real SQLSTATE with no mapping yet. The code identifies it precisely, so the gap is
	// actionable without guessing at a category.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
