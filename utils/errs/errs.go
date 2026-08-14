// Package errs carries a failure classification alongside an error.
//
// The classification exists so telemetry can report why a run failed without shipping
// any customer data: a category and a code are constants written in this repository, so a
// failure event contains nothing read from a config, a server message, or a user's input.
//
// The category is a property of the error, never of the line that raised it. One
// statement fails for unrelated reasons — a query can fail on a syntax error, a revoked
// grant, a dropped connection or an expired deadline — so a call site cannot decide the
// category. Classify walks the error chain and lets the innermost evidence win.
//
// Nothing here performs I/O, starts a goroutine, or returns an error of its own. It runs
// on a path that is already failing and must not add a second failure to it.
package errs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"
)

// Category is the closed set of failure buckets. Values are contract: they appear in
// telemetry and in dashboard queries, so adding, removing or renaming one is a breaking
// change.
type Category string

const (
	// User configuration and state.
	ConfigInvalid       Category = "config_invalid"
	ConfigDecryptFailed Category = "config_decrypt_failed"
	StateInvalid        Category = "state_invalid"

	// Reachability.
	DNSResolutionFailed Category = "dns_resolution_failed"
	NetworkUnreachable  Category = "network_unreachable"
	TLSFailed           Category = "tls_failed"
	SSHTunnelFailed     Category = "ssh_tunnel_failed"
	Timeout             Category = "timeout"

	// Identity and rights.
	AuthFailed       Category = "auth_failed"
	PermissionDenied Category = "permission_denied"

	// Existence and capability.
	ObjectNotFound     Category = "object_not_found"
	UnsupportedFeature Category = "unsupported_feature"

	// Change data capture.
	CDCPreconditionFailed Category = "cdc_precondition_failed"
	CDCPositionLost       Category = "cdc_position_lost"

	// Capacity and contention.
	ResourceExhausted   Category = "resource_exhausted"
	ConcurrencyConflict Category = "concurrency_conflict"

	// Data movement. A rise in these is usually a regression in OLake.
	SourceReadError       Category = "source_read_error"
	SchemaUnsupported     Category = "schema_unsupported"
	DestinationWriteError Category = "destination_write_error"
	CatalogError          Category = "catalog_error"

	// Lifecycle, gaps and bugs.
	Canceled      Category = "canceled"
	Unclassified  Category = "unclassified"
	InternalError Category = "internal_error"
)

// How a category was decided. The share arriving as ClassifiedByDefault is the coverage
// metric: a rise means rules are missing, not that failures changed.
const (
	ClassifiedByPrecondition = "precondition" // a condition OLake detected itself
	ClassifiedByVendor       = "vendor"       // from the vendor's own error code
	ClassifiedByStdlib       = "stdlib"       // from a Go standard library error type
	ClassifiedByDefault      = "default"      // nothing matched
)

// Failure is what telemetry reports.
//
// Category and ClassifiedBy are always set and answer different questions: Category is what
// went wrong, ClassifiedBy is how that was decided — the second is how coverage is measured,
// since a rise in "default" means rules are missing rather than that failures changed.
//
// Code and ErrorType are optional
type Failure struct {
	Category     Category `json:"category"`
	ClassifiedBy string   `json:"classified_by"`

	// Code identifies the specific failure. It is the vendor's own code where the server
	// gave one ("28P01", "1045", "AccessDenied"), otherwise a constant naming a condition
	// the driver detected itself ("postgres.replication_slot_missing"). One field rather
	// than two because the two are mutually exclusive, and ClassifiedBy already says which
	// kind it is.
	Code string `json:"code,omitempty"`

	// ErrorType is the root cause's concrete type, recorded only when nothing classified
	// the error — it is the sole remaining clue about what rule to write next.
	ErrorType string `json:"error_type,omitempty"`
}

// Error carries a Failure alongside the error it describes. The Failure is embedded so a
// classification reads as e.Category rather than e.Failure.Category.
type Error struct {
	Failure
	err error
}

// Error returns the wrapped error's message, unchanged. Classification must not alter what an
// operator reads in the console or in olake.log, or what any caller matching on message text
// sees.
func (e *Error) Error() string { return e.err.Error() }

// Unwrap keeps errors.Is and errors.As reaching the cause through the classification.
func (e *Error) Unwrap() error { return e.err }

// Attach records a classification for an error that already exists, keeping the original as
// the cause so its message and type stay intact for anything that inspects them.
//
// Returns error rather than *Error so a nil error cannot become a non-nil interface holding a
// nil pointer.
func Attach(err error, f Failure) error {
	if err == nil {
		return nil
	}
	return &Error{Failure: f, err: err}
}

// Precondition classifies a condition a connector detected itself, where no vendor error exists
// to read — a missing replication slot, an unparseable resume token, a jar that is not on disk.
//
// These are classified at the raise site, the one place that knows what the check was for, so
// they win over anything inferred later: From takes the innermost answer.
func Precondition(category Category, code string, err error) error {
	return Attach(err, Failure{
		Category:     category,
		ClassifiedBy: ClassifiedByPrecondition,
		Code:         code,
	})
}

// From reports the classification of err.
//
// It returns the innermost classification in the chain: the root cause outranks anything
// a caller added on the way up. An unclassified error is reported as Unclassified rather
// than InternalError, so a missing rule stays distinguishable from a genuine bug.
//
// It never panics. A failure here would otherwise replace the error being reported.
func From(err error) (f Failure) {
	defer func() {
		if r := recover(); r != nil {
			f = Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
		}
	}()

	if err == nil {
		return Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
	}

	if found, ok := classificationOf(err, 0); ok {
		return found
	}
	return Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
}

// maxUnwrapDepth bounds the walk. Error chains are a handful of links deep in practice;
// the limit exists only so a cyclic or pathological chain cannot hang a failure report.
const maxUnwrapDepth = 100

// classificationOf finds the classification closest to the root cause.
//
// Two shapes have to be handled. A chain built with %w unwraps one error at a time, and
// the walk keeps descending, so the deepest classification wins — the root cause outranks
// any context a caller added above it. A chain built with errors.Join or go-multierror
// unwraps to a slice, which errors.Unwrap does not follow at all; without the branch
// below, a classified error joined with anything else would be invisible and every such
// failure would report as unclassified.
func classificationOf(err error, depth int) (Failure, bool) {
	var found Failure
	var ok bool

	for e := err; e != nil && depth < maxUnwrapDepth; e, depth = errors.Unwrap(e), depth+1 {
		if classified, isOurs := e.(*Error); isOurs && classified.Category != "" {
			found, ok = classified.Failure, true
		}
		if joined, isJoined := e.(interface{ Unwrap() []error }); isJoined {
			// Branches are searched in order and the first classified one wins, so the
			// result does not depend on map iteration or goroutine scheduling.
			for _, branch := range joined.Unwrap() {
				if f, branchOK := classificationOf(branch, depth+1); branchOK {
					return f, true
				}
			}
			break
		}
	}
	return found, ok
}

// VendorClassifier recognizes errors from one library. It returns nil for anything it does
// not recognize, leaving the error for another classifier or for the standard-library
// rules below.
type VendorClassifier func(err error) *Failure

var (
	classifiersMu sync.RWMutex
	classifiers   []VendorClassifier
)

// Register adds a classifier for a source or destination's own error types.
//
// Each connector binary contains exactly one source driver and the destinations it
// supports, and registration happens from their init functions, so the set is fixed before
// any command runs. A classifier that does not recognize an error returns nil, so
// registering several is safe: an Iceberg error simply falls past the Postgres classifier.
func Register(c VendorClassifier) {
	if c == nil {
		return
	}
	classifiersMu.Lock()
	defer classifiersMu.Unlock()
	classifiers = append(classifiers, c)
}

// Classify attaches a failure classification to err.
//
// Commands call this once, where the error is finally handled. Classification reads the
// error's own structure rather than the call site, so a single call at the top covers every
// failure underneath it, whatever path produced it — a rejected password from Setup, a
// revoked grant from a chunk read, an unreachable catalog from a destination.
//
// The error itself is untouched: its message and its chain are preserved exactly, and only
// a classification travels alongside it.
//
// It never panics. A failure here would otherwise replace the error being reported.
func Classify(err error) (out error) {
	defer func() {
		if r := recover(); r != nil {
			out = err
		}
	}()

	if err == nil {
		return nil
	}
	// Already classified deeper in the chain — a precondition a driver detected, or a
	// classification attached by a lower layer. The root cause outranks anything here.
	if From(err).Category != Unclassified {
		return err
	}

	classifiersMu.RLock()
	registered := classifiers
	classifiersMu.RUnlock()

	for _, classify := range registered {
		if f := classify(err); f != nil {
			return Attach(err, *f)
		}
	}
	return Attach(err, standard(err))
}

// standard classifies failures that never reached a server, so carry no vendor code. These
// rules are identical for every connector — a refused connection is a refused connection
// whether it was Postgres or an Iceberg catalog on the other end — so they live here rather
// than being repeated in each driver.
func standard(err error) Failure {
	stdlib := func(category Category) Failure {
		return Failure{Category: category, ClassifiedBy: ClassifiedByStdlib}
	}

	// Cancellation is not a failure. Checked before deadlines, which are timeouts.
	if errors.Is(err, context.Canceled) {
		return stdlib(Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return stdlib(Timeout)
	}

	// Certificate and handshake failures, before the generic network case: a TLS failure
	// arrives wrapped in a network error and would otherwise be misread as unreachable.
	var certErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var recordErr *tls.RecordHeaderError
	switch {
	case errors.As(err, &certErr), errors.As(err, &unknownAuthority),
		errors.As(err, &hostnameErr), errors.As(err, &invalidCert),
		errors.As(err, &recordErr):
		return stdlib(TLSFailed)
	}

	// Resolution failures split into two different fixes: a name that does not exist is
	// usually a typo in the config, while a resolver that timed out is an infrastructure
	// problem. The category alone cannot say which.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		f := stdlib(DNSResolutionFailed)
		switch {
		case dnsErr.IsNotFound:
			f.Code = "host_not_found"
		case dnsErr.IsTimeout:
			f.Code = "resolver_timeout"
		}
		return f
	}

	// A network timeout reports itself as one; check it before treating the error as a
	// refused connection.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return stdlib(Timeout)
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		f := stdlib(NetworkUnreachable)
		f.Code = syscallCode(err)
		return f
	}

	// Nothing matched. Unclassified rather than InternalError: a missing rule is not a bug,
	// and keeping them apart is what makes either number meaningful. The concrete type is
	// the only clue left, so it is worth reporting here — everywhere above, the category
	// already says what the type would.
	return Failure{
		Category:     Unclassified,
		ClassifiedBy: ClassifiedByDefault,
		ErrorType:    rootType(err),
	}
}

// syscallCode names the operating system error behind a network failure, so
// network_unreachable can be split by what actually happened: a refused connection is a
// wrong port or a stopped server, an unreachable host is a routing or firewall problem, and
// a reset is a peer that hung up mid-conversation. Different fixes, same category.
//
// The names are ours rather than the raw numbers, which differ between Linux and macOS, and
// rather than the messages, which are prose. An errno with no entry here reports nothing —
// the category still stands on its own.
func syscallCode(err error) string {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ""
	}
	switch errno {
	case syscall.ECONNREFUSED:
		return "connection_refused"
	case syscall.ECONNRESET:
		return "connection_reset"
	case syscall.ECONNABORTED:
		return "connection_aborted"
	case syscall.EHOSTUNREACH:
		return "host_unreachable"
	case syscall.ENETUNREACH:
		return "network_unreachable"
	case syscall.ENETDOWN:
		return "network_down"
	case syscall.EPIPE:
		return "broken_pipe"
	case syscall.ETIMEDOUT:
		return "timed_out"
	}
	return ""
}

// rootType names the concrete type at the bottom of the chain.
//
// The outermost type is almost always *fmt.wrapError, which says nothing about what went
// wrong. The root is what identifies the failure — *status.Error for something the Iceberg
// JVM returned, *net.OpError for a socket — and it is the field someone reads when deciding
// which rule to add next.
func rootType(err error) string {
	root := err
	for range maxUnwrapDepth {
		next := errors.Unwrap(root)
		if next == nil {
			break
		}
		root = next
	}
	return fmt.Sprintf("%T", root)
}
