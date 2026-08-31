package driver

import (
	"context"
	"errors"
	"strconv"

	"github.com/datazip-inc/olake/utils/errs"
	"go.mongodb.org/mongo-driver/mongo"
	mongodriver "go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

// Codes for conditions this driver detects itself; the resume token is the only state it
// validates before asking the server.
const (
	codeResumeTokenMissing = "mongodb.resume_token_missing" // #nosec G101 -- a failure code, not a credential
	codeResumeTokenInvalid = "mongodb.resume_token_invalid" // #nosec G101 -- a failure code, not a credential
)

// commandCodeCategories maps a MongoDB server error code to a failure category. Codes marked
// "driver" are taken from mongo-driver's own retryableCodes and resumableChangeStreamErrors, so
// telemetry agrees with what the library actually does with the connection.
var commandCodeCategories = map[int32]errs.Category{
	18: errs.AuthFailed,       // AuthenticationFailed
	11: errs.AuthFailed,       // UserNotFound
	13: errs.PermissionDenied, // Unauthorized

	26: errs.ObjectNotFound, // NamespaceNotFound — database or collection

	// Change streams. CDC preconditions and position losses have their own codes, so none of
	// this is inferred from a message.
	286:   errs.CDCPositionLost,       // ChangeStreamHistoryLost — resume token older than the oplog
	136:   errs.CDCPositionLost,       // CappedPositionLost — the oplog rolled over while tailing
	280:   errs.CDCPreconditionFailed, // ChangeStreamFatalError
	40573: errs.UnsupportedFeature,    // Location40573 — $changeStream needs a replica set

	// Our cursor expired server-side because we did not read from it in time — our defect.
	43: errs.SourceReadError, // CursorNotFound (driver)

	// The server enforced a limit; nothing is unreachable.
	50:  errs.Timeout, // MaxTimeMSExpired (driver, CommandError.IsMaxTimeMSExpiredError)
	89:  errs.Timeout, // NetworkTimeout (driver)
	262: errs.Timeout, // ExceededTimeLimit (driver)

	// The member is gone, going away, or no longer one we may read from — same remedy as
	// unreachable: wait and retry.
	6:     errs.NetworkUnreachable, // HostUnreachable (driver)
	7:     errs.NetworkUnreachable, // HostNotFound (driver)
	9001:  errs.NetworkUnreachable, // SocketException (driver)
	91:    errs.NetworkUnreachable, // ShutdownInProgress (driver)
	189:   errs.NetworkUnreachable, // PrimarySteppedDown (driver)
	10107: errs.NetworkUnreachable, // NotPrimary (driver)
	10058: errs.NetworkUnreachable, // legacy not-primary (driver, notPrimaryCodes)
	13435: errs.NetworkUnreachable, // NotPrimaryNoSecondaryOK (driver)
	13436: errs.NetworkUnreachable, // NotPrimaryOrSecondary (driver)
	11600: errs.NetworkUnreachable, // InterruptedAtShutdown (driver)
	11602: errs.NetworkUnreachable, // InterruptedDueToReplStateChange (driver)
	133:   errs.NetworkUnreachable, // FailedToSatisfyReadPreference (driver)

	292:   errs.ResourceExhausted, // QueryExceededMemoryLimitNoDiskUseAllowed — reachable from $bucketAuto
	11601: errs.Canceled,          // Interrupted — killOp, or our own disconnect mid-command
}

// Registered so ReportFailure can classify without knowing which connector ran. Only MongoDB
// evidence lives here; DNS, TLS and socket failures are shared and belong to utils/errs.
func init() { errs.Register("mongodb", classify) }

// classify reads MongoDB's server code, or the reason recorded against a candidate member, and
// returns nil for anything else. The category comes from the error, never the call site.
func classify(err error) *errs.Failure {
	if f := fromServerCode(err); f != nil {
		return f
	}
	return fromServerSelection(err)
}

// fromServerCode classifies anything carrying a MongoDB error code. Two types carry one:
// mongo.CommandError from the public API, and driver.Error, the pre-conversion form kept inside
// a topology description. Both are read so either path classifies identically.
func fromServerCode(err error) *errs.Failure {
	code, ok := serverCode(err)
	if !ok {
		return nil
	}

	// The code travels whether or not it is mapped.
	f := errs.Failure{Code: strconv.FormatInt(int64(code), 10)}
	if category, found := commandCodeCategories[code]; found {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real server code with no rule yet; the code alone makes the gap actionable.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}

// fromServerSelection classifies the failure MongoDB reports when it could not pick a server —
// the shape most outages take. The reason is not in the error chain: it sits in the topology
// snapshot's per-server LastError, which errors.As cannot reach, so it is walked by hand.
func fromServerSelection(err error) *errs.Failure {
	var selectionErr topology.ServerSelectionError
	if !errors.As(err, &selectionErr) {
		return nil
	}

	// Our own context ended the wait; the shared rules already tell cancellation and deadline apart.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	// Read the structure, never the rendered topology string; the first server with a reason
	// wins, since one cause takes down the whole selection. A recorded reason is often a
	// resolver or socket error rather than a MongoDB code, so unmapped ones go to the shared rules.
	for _, server := range selectionErr.Desc.Servers {
		if server.LastError == nil {
			continue
		}
		if f := fromServerCode(server.LastError); f != nil {
			return f
		}
		if underlying := errs.Standard(server.LastError); underlying.Category != errs.Unclassified {
			return &underlying
		}
	}

	// No member gave a reason: nothing answered.
	return &errs.Failure{
		Category:     errs.NetworkUnreachable,
		ClassifiedBy: errs.ClassifiedByVendor,
		Code:         "server_selection_failed",
	}
}

// serverCode reads MongoDB's error code out of whichever type carries it. Both are value types,
// so errors.As gets a value target rather than a pointer.
func serverCode(err error) (int32, bool) {
	var commandErr mongo.CommandError
	if errors.As(err, &commandErr) {
		return commandErr.Code, true
	}
	var wireErr mongodriver.Error
	if errors.As(err, &wireErr) {
		return wireErr.Code, true
	}
	return 0, false
}
