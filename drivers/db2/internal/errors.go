package driver

import (
	"errors"

	"github.com/datazip-inc/olake/utils/errs"
	go_ibm_db "github.com/ibmdb/go_ibm_db"
)

// The one condition this driver detects itself: Db2 has no change feed, so there is no
// replication state to validate.
const codeCDCUnsupported = "db2.cdc_unsupported"

// sqlStateCategories maps a Db2 SQLSTATE to a failure category. Two spaces arrive here: Db2's
// own, and the ODBC/CLI layer's (08S01, HYT00, IM002, 42S02), raised before Db2 is reached.
// SQLSTATE is what gets reported, never the SQLCODE, even where the SQLCODE decided the category.
var sqlStateCategories = map[string]errs.Category{
	// The CLI never reached a usable session.
	"08001": errs.NetworkUnreachable, // client unable to establish the connection
	"08003": errs.NetworkUnreachable, // connection does not exist
	"08006": errs.NetworkUnreachable, // connection failure
	"08S01": errs.NetworkUnreachable, // communication link failure (CLI)
	"08004": errs.AuthFailed,         // server rejected the connection — see sqlCodeOverrides

	"28000": errs.AuthFailed,

	// Missing object, or an authorization ID that cannot use it.
	"42501": errs.PermissionDenied, // insufficient authorization
	"42502": errs.PermissionDenied, // authorization violation
	"42704": errs.ObjectNotFound,   // undefined object name
	"42703": errs.ObjectNotFound,   // undefined column name
	"42S02": errs.ObjectNotFound,   // base table or view not found (CLI)
	"42S22": errs.ObjectNotFound,   // column not found (CLI)

	// Also class 42, but these mean the SQL *OLake generated* is wrong — our defect.
	"42601": errs.SourceReadError, // syntax error
	"42884": errs.SourceReadError, // no routine found with matching signature

	// Well-formed statement; the data cannot be represented as asked.
	"22007": errs.SchemaUnsupported, // invalid datetime format
	"22018": errs.SchemaUnsupported, // invalid character value for cast

	// Two units of work collided; a retry usually clears it.
	"40001": errs.ConcurrencyConflict, // rollback caused by deadlock or timeout
	"57033": errs.ConcurrencyConflict, // deadlock or timeout with no automatic rollback

	// Nothing is misconfigured; the server ran out.
	"57011": errs.ResourceExhausted, // resources not available — includes a full transaction log
	"57019": errs.ResourceExhausted, // resource unavailable
	"54001": errs.ResourceExhausted, // statement too complex

	"HYT00": errs.Timeout,  // timeout expired (CLI)
	"HYT01": errs.Timeout,  // connection timeout expired (CLI)
	"57014": errs.Canceled, // processing was canceled by an interrupt

	// IM002 is a data source the user named and the driver cannot find; IM003 is our own
	// packaging, since OLake ships the CLI.
	"IM002": errs.ConfigInvalid,
	"IM003": errs.InternalError,
}

// sqlCodeOverrides resolves the two SQLSTATEs Db2 reuses across conditions needing different
// fixes. The SQLCODE is consulted only where it changes the answer; the reported code stays
// the SQLSTATE, so telemetry keeps one code space.
var sqlCodeOverrides = map[int]errs.Category{
	-30082: errs.AuthFailed,         // SQL30082N security processing failed — SQLSTATE 08001
	-30081: errs.NetworkUnreachable, // SQL30081N communication error — SQLSTATE 08001
	-30061: errs.ObjectNotFound,     // SQL30061N database alias or name not found — SQLSTATE 08004
	-1403:  errs.AuthFailed,         // SQL1403N username or password incorrect — SQLSTATE 08004
}

// Registered so ReportFailure can classify without knowing which connector ran. Only Db2
// evidence lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register("db2", classify) }

// classify reads Db2's SQLSTATE, and the SQLCODE where the SQLSTATE is ambiguous, returning nil
// for anything else. The category comes from the error, never the call site.
func classify(err error) *errs.Failure {
	var db2Err *go_ibm_db.Error
	if !errors.As(err, &db2Err) {
		return nil
	}

	// Diagnostics arrive as a slice whose length and contents depend on the CLI build, and the
	// CLI puts a general record ahead of the real cause — so walk all of them, not just the first.
	var firstState string
	for i := range db2Err.Diag {
		record := db2Err.Diag[i]
		if record.State == "" {
			continue
		}
		if firstState == "" {
			firstState = record.State
		}

		// Consulted only where the SQLSTATE covers unrelated conditions.
		if category, ok := sqlCodeOverrides[record.NativeError]; ok {
			return &errs.Failure{
				Category:     category,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         record.State,
			}
		}
		if category, ok := sqlStateCategories[record.State]; ok {
			return &errs.Failure{
				Category:     category,
				ClassifiedBy: errs.ClassifiedByVendor,
				Code:         record.State,
			}
		}
	}

	// Diagnostics present, nothing mapped; the SQLSTATE alone makes the gap actionable.
	if firstState != "" {
		return &errs.Failure{
			Category:     errs.Unclassified,
			ClassifiedBy: errs.ClassifiedByDefault,
			Code:         firstState,
		}
	}

	// No diagnostics at all, which some CLI builds do normally. Handed back to the shared rules,
	// which may still recognize the transport failure underneath.
	return nil
}
