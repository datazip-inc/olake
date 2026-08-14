package driver

import (
	"errors"
	"strconv"

	"github.com/datazip-inc/olake/utils/errs"
	binlogmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
)

// Codes for conditions this driver detects itself. A code is only worth having where the server
// did not already name the failure; everywhere else the error number is reported as-is.
const (
	codeGlobalStateInvalid   = "mysql.global_state_invalid"
	codeServerIDMissing      = "mysql.server_id_missing"
	codeMetadataStateInvalid = "mysql.metadata_state_invalid"
	codePortInvalid          = "mysql.port_invalid"
)

// errnoCategories maps a MySQL server error number to a failure category. Comments give the
// ER_ macro as go-mysql's generated table spells it; that table ships with the dependency, so
// the mapping can be diffed offline.
//
//	github.com/go-mysql-org/go-mysql@v1.11.0/mysql/errcode.go
//	https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html  (the 3xxx codes)
//
// The number rather than the message: stable across versions and language settings. Reported
// verbatim, so an unmapped number stays identifiable.
//
// Client-side numbers (2002, 2003, 2006, 2013) cannot arrive here: they belong to the C
// client, and neither Go library can produce them. Those failures arrive as standard-library
// network errors.
var errnoCategories = map[uint16]errs.Category{
	// Authentication. The server refused the identity itself.
	1045: errs.AuthFailed, // ER_ACCESS_DENIED_ERROR
	1698: errs.AuthFailed, // ER_ACCESS_DENIED_NO_PASSWORD_ERROR
	1130: errs.AuthFailed, // ER_HOST_NOT_PRIVILEGED

	// Authorization. Authentication has already succeeded; the grant is missing.
	1044: errs.PermissionDenied, // ER_DBACCESS_DENIED_ERROR
	1142: errs.PermissionDenied, // ER_TABLEACCESS_DENIED_ERROR
	1143: errs.PermissionDenied, // ER_COLUMNACCESS_DENIED_ERROR
	1227: errs.PermissionDenied, // ER_SPECIFIC_ACCESS_DENIED_ERROR — REPLICATION CLIENT, SUPER

	// The object does not exist.
	1049: errs.ObjectNotFound, // ER_BAD_DB_ERROR
	1146: errs.ObjectNotFound, // ER_NO_SUCH_TABLE
	1109: errs.ObjectNotFound, // ER_UNKNOWN_TABLE
	1054: errs.ObjectNotFound, // ER_BAD_FIELD_ERROR — a column vanished from under a running sync

	// The SQL *OLake generated* is wrong. The user never writes this SQL, so a failure here
	// is ours, not a misconfiguration — which is why they are source_read_error (owner:
	// olake) rather than config_invalid.
	1064: errs.SourceReadError, // ER_PARSE_ERROR
	1052: errs.SourceReadError, // ER_NON_UNIQ_ERROR
	1247: errs.SourceReadError, // ER_ILLEGAL_REFERENCE
	1305: errs.SourceReadError, // ER_SP_DOES_NOT_EXIST

	// A type or collation OLake asked the server to compare in a way it cannot. Distinct
	// from the ones above: the statement is well-formed, the data is the problem.
	1267: errs.SchemaUnsupported, // ER_CANT_AGGREGATE_2COLLATIONS

	// Capacity. Nothing is misconfigured; the server ran out of something.
	1040: errs.ResourceExhausted, // ER_CON_COUNT_ERROR
	1203: errs.ResourceExhausted, // ER_TOO_MANY_USER_CONNECTIONS
	1226: errs.ResourceExhausted, // ER_USER_LIMIT_REACHED
	1021: errs.ResourceExhausted, // ER_DISK_FULL
	1041: errs.ResourceExhausted, // ER_OUT_OF_RESOURCES
	1037: errs.ResourceExhausted, // ER_OUTOFMEMORY
	1206: errs.ResourceExhausted, // ER_LOCK_TABLE_FULL

	// Contention. Two transactions collided; retrying usually works.
	1213: errs.ConcurrencyConflict, // ER_LOCK_DEADLOCK
	1205: errs.ConcurrencyConflict, // ER_LOCK_WAIT_TIMEOUT

	// The server is going away or the connection is being torn down; from here that is
	// indistinguishable from unreachable, same remedy.
	1053: errs.NetworkUnreachable, // ER_SERVER_SHUTDOWN
	1152: errs.NetworkUnreachable, // ER_ABORTING_CONNECTION
	1158: errs.NetworkUnreachable, // ER_NET_READ_ERROR
	1159: errs.NetworkUnreachable, // ER_NET_READ_INTERRUPTED
	1160: errs.NetworkUnreachable, // ER_NET_ERROR_ON_WRITE
	1161: errs.NetworkUnreachable, // ER_NET_WRITE_INTERRUPTED

	// Deadlines and interruption.
	3024: errs.Timeout,  // ER_QUERY_TIMEOUT — max_execution_time exceeded
	1317: errs.Canceled, // ER_QUERY_INTERRUPTED — KILL QUERY, or our own context cancellation

	// Binlog replication. The position we asked to resume from is gone, or binary logging
	// is off entirely.
	1236: errs.CDCPositionLost,       // ER_MASTER_FATAL_ERROR_READING_BINLOG
	1373: errs.CDCPositionLost,       // ER_UNKNOWN_TARGET_BINLOG
	1381: errs.CDCPreconditionFailed, // ER_NO_BINARY_LOGGING

	// The server rejected a setting the config asked for.
	1298: errs.ConfigInvalid, // ER_UNKNOWN_TIME_ZONE — jdbc_url_params.time_zone
	1231: errs.ConfigInvalid, // ER_WRONG_VALUE_FOR_VAR
	3159: errs.ConfigInvalid, // ER_SECURE_TRANSPORT_REQUIRED — ssl.mode disabled against a TLS-only server

	// The server understood the request and cannot serve it on this version or build.
	1251: errs.UnsupportedFeature, // ER_NOT_SUPPORTED_AUTH_MODE
	1235: errs.UnsupportedFeature, // ER_NOT_SUPPORTED_YET
}

// Register so ReportFailure can classify without knowing which connector ran. Only MySQL
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads MySQL's error number. Returns nil for anything else, leaving it to the
// shared standard-library rules.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	// MySQL is reached through two libraries and both surface the same numbering from the same
	// server: go-sql-driver carries queries, go-mysql carries the binlog stream. One table
	// serves both, and no other library produces either type, so no other driver's error can
	// enter it.
	var errno uint16
	var queryErr *mysql.MySQLError
	var binlogErr *binlogmysql.MyError
	switch {
	case errors.As(err, &queryErr):
		errno = queryErr.Number
	case errors.As(err, &binlogErr):
		errno = binlogErr.Code

	case errors.Is(err, mysql.ErrInvalidConn):
		// The connection died mid-request. go-sql-driver logs the underlying network error and
		// replaces it with this sentinel, so nothing survives for the standard-library rules to
		// read. driver.ErrBadConn is not checked alongside it: database/sql retries that one on
		// a fresh connection, and the retry fails with the real network error.
		return &errs.Failure{
			Category:     errs.NetworkUnreachable,
			ClassifiedBy: errs.ClassifiedByVendor,
			Code:         "invalid_connection",
		}

	default:
		return nil
	}

	// The server answered and said what was wrong. Its error number travels with the failure
	// whether or not it is mapped, so an unmapped number is still identifiable.
	f := errs.Failure{Code: strconv.FormatUint(uint64(errno), 10)}
	if category, ok := errnoCategories[errno]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
