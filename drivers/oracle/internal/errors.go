package driver

import (
	"errors"
	"strconv"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/sijms/go-ora/v2/network"
)

// oraCodeCategories maps an Oracle error number to a failure category. Entries marked "go-ora"
// come from the driver's own two tables — translate(), which spells out the codes it knows, and
// Bad(), its definition of an unusable connection. Both ship with the dependency.
//
//	github.com/sijms/go-ora/v2@v2.8.24/network/oracle_error.go
//	https://docs.oracle.com/en/error-help/db/
//
// ErrCode rather than the message: translate() derives the message *from* this field, so the
// number is the stable half and is identical before and after anything renders the error.
var oraCodeCategories = map[int]errs.Category{
	// Identity. The server rejected the credentials or the account itself.
	1017:  errs.AuthFailed, // invalid username/password; logon denied
	1005:  errs.AuthFailed, // null password given; logon denied
	28000: errs.AuthFailed, // the account is locked
	28001: errs.AuthFailed, // the password has expired

	// Rights. The logon succeeded and the grant is missing.
	1031: errs.PermissionDenied, // insufficient privileges
	1039: errs.PermissionDenied, // insufficient privileges on underlying objects of the view

	// The object does not exist. The last two are the listener's equivalent: the service or
	// SID named in the config is not one it serves.
	942:   errs.ObjectNotFound, // table or view does not exist
	904:   errs.ObjectNotFound, // invalid identifier (go-ora) — a column vanished from under a sync
	1918:  errs.ObjectNotFound, // user does not exist
	12514: errs.ObjectNotFound, // TNS:listener does not currently know of service requested (go-ora)
	12505: errs.ObjectNotFound, // TNS:listener does not currently know of SID given

	// The SQL *OLake generated* is wrong. The user never writes this SQL, so a failure here
	// is ours, not a misconfiguration — which is why they are source_read_error (owner:
	// olake) rather than config_invalid.
	900: errs.SourceReadError, // invalid SQL statement (go-ora)
	903: errs.SourceReadError, // invalid table name (go-ora)
	905: errs.SourceReadError, // missing keyword (go-ora)
	906: errs.SourceReadError, // missing left parenthesis (go-ora)
	907: errs.SourceReadError, // missing right parenthesis (go-ora)
	923: errs.SourceReadError, // FROM keyword not found where expected
	933: errs.SourceReadError, // SQL command not properly ended
	936: errs.SourceReadError, // missing expression

	// A type OLake asked the server to read or convert in a way it cannot. The statement is
	// well-formed; the data is the problem.
	902:  errs.SchemaUnsupported, // invalid data type (go-ora)
	932:  errs.SchemaUnsupported, // inconsistent datatypes
	1722: errs.SchemaUnsupported, // invalid number
	1858: errs.SchemaUnsupported, // a non-numeric character was found where a numeric was expected

	// Contention. Two sessions collided; retrying usually works.
	60: errs.ConcurrencyConflict, // deadlock detected while waiting for resource
	54: errs.ConcurrencyConflict, // resource busy and acquire with NOWAIT specified

	// Capacity. Nothing is misconfigured; the server ran out of something.
	257:   errs.ResourceExhausted, // archiver error; connect internal only, until freed
	4030:  errs.ResourceExhausted, // out of process memory
	4031:  errs.ResourceExhausted, // unable to allocate bytes of shared memory
	1652:  errs.ResourceExhausted, // unable to extend temp segment
	1555:  errs.ResourceExhausted, // snapshot too old — undo was recycled under a long read
	12516: errs.ResourceExhausted, // TNS:listener could not find available handler (go-ora)

	// The connection is dead or the instance is not serving. The first ten are go-ora's own
	// Bad() set — the codes the driver itself treats as an unusable connection.
	28:    errs.NetworkUnreachable, // your session has been killed (go-ora Bad)
	1012:  errs.NetworkUnreachable, // not logged on (go-ora Bad)
	1033:  errs.NetworkUnreachable, // ORACLE initialization or shutdown in progress (go-ora Bad)
	1034:  errs.NetworkUnreachable, // ORACLE not available (go-ora Bad)
	1089:  errs.NetworkUnreachable, // immediate shutdown in progress (go-ora Bad)
	3113:  errs.NetworkUnreachable, // end-of-file on communication channel (go-ora Bad)
	3114:  errs.NetworkUnreachable, // not connected to ORACLE (go-ora Bad)
	3135:  errs.NetworkUnreachable, // connection lost contact (go-ora Bad + translate)
	12528: errs.NetworkUnreachable, // TNS:listener: all appropriate instances are blocking (go-ora Bad)
	12537: errs.NetworkUnreachable, // TNS:connection closed (go-ora Bad)
	12541: errs.NetworkUnreachable, // TNS:no listener
	12545: errs.NetworkUnreachable, // connect failed because target host or object does not exist
	12560: errs.NetworkUnreachable, // TNS:protocol adapter error
	12564: errs.NetworkUnreachable, // TNS connection refused (go-ora)

	// Deadlines and interruption.
	12170: errs.Timeout,  // TNS:Connect timeout occurred
	1013:  errs.Canceled, // user requested cancel of current operation (go-ora)
}

// Register so ReportFailure can classify without knowing which connector ran. Only Oracle
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads Oracle's error number. Returns nil for anything else, leaving it to the
// shared standard-library rules.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	var oraErr *network.OracleError
	if !errors.As(err, &oraErr) {
		// go-ora replaces the error with this sentinel when a context deadline breaks the
		// connection mid-request, and the deadline itself does not always survive with it.
		// Without this rule those failures report as unclassified.
		if errors.Is(err, network.ErrConnReset) {
			return &errs.Failure{
				Category:     errs.Timeout,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         "connection_break_on_timeout",
			}
		}
		return nil
	}

	// The server answered and said what was wrong. Its number travels with the failure
	// whether or not it is mapped, so an unmapped number is still identifiable.
	//
	// ErrCode is read directly rather than through Error(): the message is translated lazily
	// and the translation is derived *from* this field, so the number is the stable half.
	f := errs.Failure{Code: strconv.Itoa(oraErr.ErrCode)}
	if category, ok := oraCodeCategories[oraErr.ErrCode]; ok {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real Oracle code with no mapping yet. The number identifies it precisely, so the gap
	// is actionable without guessing at a category.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}
