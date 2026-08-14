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

// Codes for conditions this driver detects itself. Both are about the resume token, because
// that is the only piece of state this driver validates before the server is asked.
const (
	codeResumeTokenMissing = "mongodb.resume_token_missing" // #nosec G101 -- a failure code, not a credential
	codeResumeTokenInvalid = "mongodb.resume_token_invalid" // #nosec G101 -- a failure code, not a credential
)

// commandCodeCategories maps a MongoDB server error code to a failure category. Codes marked
// "driver" come from mongo-driver's own retry and change-stream allowlists, which ship with the
// dependency and carry the code names in comments.
//
//	go.mongodb.org/mongo-driver@v1.17.3/x/mongo/driver/errors.go       retryableCodes
//	go.mongodb.org/mongo-driver@v1.17.3/mongo/change_stream.go         resumableChangeStreamErrors
//	https://www.mongodb.com/docs/manual/reference/error-codes/
var commandCodeCategories = map[int32]errs.Category{
	// Identity and rights.
	18: errs.AuthFailed,       // AuthenticationFailed
	11: errs.AuthFailed,       // UserNotFound
	13: errs.PermissionDenied, // Unauthorized

	// Existence.
	26: errs.ObjectNotFound, // NamespaceNotFound — database or collection

	// Change streams. MongoDB's CDC preconditions and position losses have their own codes,
	// which is why none of this has to be inferred from a message.
	286:   errs.CDCPositionLost,       // ChangeStreamHistoryLost — resume token older than the oplog
	136:   errs.CDCPositionLost,       // CappedPositionLost — the oplog rolled over while tailing
	280:   errs.CDCPreconditionFailed, // ChangeStreamFatalError
	40573: errs.UnsupportedFeature,    // Location40573 — $changeStream needs a replica set

	// A cursor OLake was holding no longer exists on the server. Ours to fix: the cursor
	// timed out because we did not read from it in time.
	43: errs.SourceReadError, // CursorNotFound (driver)

	// Deadlines. The server enforced a limit; nothing is unreachable.
	50:  errs.Timeout, // MaxTimeMSExpired (driver, CommandError.IsMaxTimeMSExpiredError)
	89:  errs.Timeout, // NetworkTimeout (driver)
	262: errs.Timeout, // ExceededTimeLimit (driver)

	// The member is gone, going away, or no longer the one we may read from. From here that
	// is indistinguishable from unreachable, and the remedy is the same: wait and retry.
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

	// Capacity and interruption.
	292:   errs.ResourceExhausted, // QueryExceededMemoryLimitNoDiskUseAllowed — reachable from $bucketAuto
	11601: errs.Canceled,          // Interrupted — killOp, or our own disconnect mid-command
}

// Register so ReportFailure can classify without knowing which connector ran. Only MongoDB
// evidence is handled here — DNS, TLS, refused connections and deadlines look the same for
// every connector and belong to utils/errs.
func init() { errs.Register(classify) }

// classify reads MongoDB's server code, or the reason recorded against a candidate member.
// Returns nil for anything else.
//
// The category comes from the error, never from the call site: one call can fail on a revoked
// grant, a missing object, contention or a dropped connection.
func classify(err error) *errs.Failure {
	if f := fromServerCode(err); f != nil {
		return f
	}
	return fromServerSelection(err)
}

// fromServerCode classifies anything carrying a MongoDB error code.
//
// Two types carry one: mongo.CommandError, which the public API returns, and driver.Error,
// which is what the same failure looks like before the public API converts it — the form it
// keeps inside a topology description. Both are read so that a code found through either
// path is classified identically.
func fromServerCode(err error) *errs.Failure {
	code, ok := serverCode(err)
	if !ok {
		return nil
	}

	// The server answered and said what was wrong. Its code travels with the failure whether
	// or not we have mapped it, so an unmapped code is still identifiable.
	f := errs.Failure{Code: strconv.FormatInt(int64(code), 10)}
	if category, found := commandCodeCategories[code]; found {
		f.Category = category
		f.ClassifiedBy = errs.ClassifiedByVendor
		return &f
	}
	// A real server code with no mapping yet. The code identifies it precisely, so the gap is
	// actionable without guessing at a category.
	f.Category = errs.Unclassified
	f.ClassifiedBy = errs.ClassifiedByDefault
	return &f
}

// fromServerSelection classifies the failure MongoDB reports when it could not pick a server.
//
// The shape most MongoDB outages take, and it needs handling here because the reason is not in
// the error chain: selection fails against a topology snapshot, and why each candidate was
// rejected is recorded in that snapshot's per-server LastError, which errors.As cannot reach.
// Without this walk a wrong password reports as an unreachable network.
func fromServerSelection(err error) *errs.Failure {
	var selectionErr topology.ServerSelectionError
	if !errors.As(err, &selectionErr) {
		return nil
	}

	// Our own context ended the wait. That is not MongoDB's failure to describe, and the
	// shared rules already tell cancellation and deadline apart.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	// Read the structure, never the rendered topology string. The first server that recorded
	// a reason wins; they are near-identical in practice, since one cause takes down the
	// whole selection.
	for _, server := range selectionErr.Desc.Servers {
		if server.LastError == nil {
			continue
		}
		if f := fromServerCode(server.LastError); f != nil {
			return f
		}
	}

	// No member gave a reason: nothing answered. The category stands on its own — there is no
	// code, because no server ever produced one.
	return &errs.Failure{
		Category:     errs.NetworkUnreachable,
		ClassifiedBy: errs.ClassifiedByVendor,
		Code:         "server_selection_failed",
	}
}

// serverCode reads MongoDB's own error code out of whichever type is carrying it.
//
// Both types are value types, so errors.As is given a value target rather than a pointer.
// No other library produces either, so no other driver's error can enter this table.
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
