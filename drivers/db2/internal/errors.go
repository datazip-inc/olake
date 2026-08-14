package driver

import (
	"errors"

	"github.com/datazip-inc/olake/utils/errs"
	go_ibm_db "github.com/ibmdb/go_ibm_db"
)

// Code for the one condition this driver detects itself. Db2 is a backfill-and-incremental
// source with no change feed, so there is no replication state to validate.
const codeCDCUnsupported = "db2.cdc_unsupported"

// sqlStateCategories maps a Db2 SQLSTATE to a failure category. Two spaces arrive here and both
// are mapped: Db2's own, and the ODBC/CLI layer's (08S01, HYT00, IM002, 42S02), which the CLI
// driver generates before Db2 is reached.
//
//	https://www.ibm.com/docs/en/db2/11.5?topic=messages-sqlstate
//
// SQLSTATE is what gets reported — never the SQLCODE, even when the SQLCODE decided the
// category. Db2 is the only source with two numbering schemes in play, and one field can hold
// only one of them.
var sqlStateCategories = map[string]errs.Category{
	// Class 08 — Connection Exception. The CLI never reached a usable session.
	"08001": errs.NetworkUnreachable, // client unable to establish the connection
	"08003": errs.NetworkUnreachable, // connection does not exist
	"08006": errs.NetworkUnreachable, // connection failure
	"08S01": errs.NetworkUnreachable, // communication link failure (CLI)
	"08004": errs.AuthFailed,         // server rejected the connection — see sqlCodeOverrides

	// Class 28 — Invalid Authorization Specification.
	"28000": errs.AuthFailed,

	// Class 42 — the object does not exist, or the authorization ID cannot use it.
	"42501": errs.PermissionDenied, // insufficient authorization
	"42502": errs.PermissionDenied, // authorization violation
	"42704": errs.ObjectNotFound,   // undefined object name
	"42703": errs.ObjectNotFound,   // undefined column name
	"42S02": errs.ObjectNotFound,   // base table or view not found (CLI)
	"42S22": errs.ObjectNotFound,   // column not found (CLI)

	// Also Class 42, but these mean the SQL *OLake generated* is wrong. The user never writes
	// this SQL, so a failure here is ours, not a misconfiguration.
	"42601": errs.SourceReadError, // syntax error
	"42884": errs.SourceReadError, // no routine found with matching signature

	// The statement is well formed and the data cannot be represented as asked.
	"22007": errs.SchemaUnsupported, // invalid datetime format
	"22018": errs.SchemaUnsupported, // invalid character value for cast

	// Contention. Two units of work collided; retrying usually works.
	"40001": errs.ConcurrencyConflict, // rollback caused by deadlock or timeout
	"57033": errs.ConcurrencyConflict, // deadlock or timeout with no automatic rollback

	// Capacity. Nothing is misconfigured; the server ran out of something.
	"57011": errs.ResourceExhausted, // resources not available — includes a full transaction log
	"57019": errs.ResourceExhausted, // resource unavailable
	"54001": errs.ResourceExhausted, // statement too complex

	// Deadlines and interruption.
	"HYT00": errs.Timeout,  // timeout expired (CLI)
	"HYT01": errs.Timeout,  // connection timeout expired (CLI)
	"57014": errs.Canceled, // processing was canceled by an interrupt

	// The CLI could not be configured or loaded at all. IM002 is a data source the user named
	// and the driver cannot find; IM003 is our own packaging, since OLake ships the CLI.
	"IM002": errs.ConfigInvalid,
	"IM003": errs.InternalError,
}

// sqlCodeOverrides resolves the two SQLSTATEs that cover unrelated conditions.
//
// Db2 reuses a SQLSTATE across messages that need different fixes, and the native SQLCODE is
// the only thing that separates them. Rather than introduce a second code space into
// telemetry, the SQLCODE is consulted only where it changes the answer; the reported code
// stays the SQLSTATE in every case.
var sqlCodeOverrides = map[int]errs.Category{
	-30082: errs.AuthFailed,         // SQL30082N security processing failed — SQLSTATE 08001
	-30081: errs.NetworkUnreachable, // SQL30081N communication error — SQLSTATE 08001
	-30061: errs.ObjectNotFound,     // SQL30061N database alias or name not found — SQLSTATE 08004
	-1403:  errs.AuthFailed,         // SQL1403N username or password incorrect — SQLSTATE 08004
}

// Register so ReportFailure can classify without knowing which connector ran. Only Db2
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads Db2's SQLSTATE, and the SQLCODE where the SQLSTATE is ambiguous. Returns nil
// for anything else.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	var db2Err *go_ibm_db.Error
	if !errors.As(err, &db2Err) {
		return nil
	}

	// The diagnostics arrive as a *slice* of records, not as one error. How many there are,
	// and whether they carry a SQLSTATE at all, depends on the CLI driver build — so this
	// walks every record and takes the first one it can classify, rather than assuming the
	// first record is the interesting one.
	var firstState string
	for i := range db2Err.Diag {
		record := db2Err.Diag[i]
		if record.State == "" {
			continue
		}
		if firstState == "" {
			firstState = record.State
		}

		// The SQLCODE is consulted only where the SQLSTATE covers unrelated conditions.
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

	// Diagnostics were present but nothing in them is mapped. The SQLSTATE identifies the gap
	// precisely, so it is worth reporting without guessing at a category.
	if firstState != "" {
		return &errs.Failure{
			Category:     errs.Unclassified,
			ClassifiedBy: errs.ClassifiedByDefault,
			Code:         firstState,
		}
	}

	// No diagnostics at all. This is normal for this driver rather than a defect — Diag is
	// populated from CLI diagnostics and some builds leave it empty — so the error is handed
	// back to the shared rules, which may still recognize the transport failure underneath.
	return nil
}
