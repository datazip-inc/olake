// Package errs carries a failure classification alongside an error, so telemetry can report why
// a run failed without shipping customer data: categories and codes are constants in this repo.
//
// The category belongs to the error, never to the line that raised it, so Classify walks the
// chain and lets the innermost evidence win. Nothing here does I/O or returns an error of its
// own: it runs on a path that is already failing.
package errs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
)

// Category is the closed set of failure buckets. Values are contract — they appear in telemetry
// and dashboard queries — so adding, removing or renaming one is a breaking change.
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

// How a category was decided. The share arriving as ClassifiedByDefault is the coverage metric:
// a rise means rules are missing, not that failures changed.
const (
	ClassifiedByPrecondition = "precondition" // a condition OLake detected itself
	ClassifiedByVendor       = "vendor"       // from the vendor's own error code
	ClassifiedByStdlib       = "stdlib"       // from a Go standard library error type
	ClassifiedByDefault      = "unclassified" // nothing matched
)

// Failure is what telemetry reports. Category and ClassifiedBy are always set and answer
// different questions — what went wrong, and on what evidence. The rest are optional.
type Failure struct {
	Category     Category `json:"category"`
	ClassifiedBy string   `json:"classified_by"`

	// Code is the vendor's own code where the server gave one ("28P01", "AccessDenied"),
	// otherwise a constant for a condition the driver detected itself
	// ("postgres.replication_slot_missing"). One field: the two are mutually exclusive.
	Code string `json:"code,omitempty"`

	// ErrorType is the root cause's concrete type, recorded only when nothing classified the
	// error — the sole remaining clue about which rule to write next.
	ErrorType string `json:"error_type,omitempty"`

	// Component names the connector that classified the failure — "postgres", "iceberg". A run
	// has a source and a destination and which one failed is not otherwise recoverable. Empty
	// where the shared rules matched, since they cannot know whose endpoint was on the far end.
	Component string `json:"component,omitempty"`
}

// Error carries a Failure alongside the error it describes.
type Error struct {
	Failure
	err error
}

// Error returns the wrapped error's message unchanged: classification must not alter what an
// operator reads, or what a caller matching on message text sees.
func (e *Error) Error() string { return e.err.Error() }

// Unwrap keeps errors.Is and errors.As reaching the cause through the classification.
func (e *Error) Unwrap() error { return e.err }

// Attach records a classification for an existing error, keeping the original as the cause. It
// returns error rather than *Error so nil cannot become a non-nil interface holding a nil pointer.
func Attach(err error, f Failure) error {
	if err == nil {
		return nil
	}
	return &Error{Failure: f, err: err}
}

// Precondition classifies a condition a connector detected itself, where no vendor error exists
// to read — a missing replication slot, an unparseable resume token, a jar that is not on disk.
// Classified at the raise site, the only place that knows what the check was for.
func Precondition(category Category, code string, err error) error {
	// Codes are written "<component>.<condition>", so no second argument can disagree with them.
	component, _, _ := strings.Cut(code, ".")
	return Attach(err, Failure{
		Category:     category,
		ClassifiedBy: ClassifiedByPrecondition,
		Code:         code,
		Component:    component,
	})
}

// From reports the innermost classification in err's chain: the root cause outranks anything a
// caller added above it. Unmatched errors report Unclassified rather than InternalError, keeping
// a missing rule distinguishable from a bug. It never panics — that would replace the real error.
func From(err error) (f Failure) {
	defer func() {
		if r := recover(); r != nil {
			f = Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
		}
	}()

	if err == nil {
		return Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
	}

	if found, ok := classificationOf(err); ok {
		return found
	}
	return Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
}

// maxUnwrapDepth bounds the walk so a cyclic chain cannot hang a failure report.
const maxUnwrapDepth = 100

// walk searches err for the answer accept recognizes. Two chain shapes exist: %w unwraps one
// error at a time, so the walk keeps descending and a deeper answer replaces a shallower one;
// errors.Join unwraps to a slice, which errors.Unwrap does not follow, so branches are searched
// by hand and the first that answers wins. depth bounds it so a cyclic chain cannot hang.
func walk[T any](err error, depth int, accept func(error) (T, bool)) (T, bool) {
	var found T
	var ok bool

	for e := err; e != nil && depth < maxUnwrapDepth; e, depth = errors.Unwrap(e), depth+1 {
		if answer, matched := accept(e); matched {
			found, ok = answer, true
		}
		if joined, isJoined := e.(interface{ Unwrap() []error }); isJoined {
			// First answering branch wins, so the result is deterministic.
			for _, branch := range joined.Unwrap() {
				if answer, branchOK := walk(branch, depth+1, accept); branchOK {
					return answer, true
				}
			}
			break
		}
	}
	return found, ok
}

// classificationOf finds the classification closest to the root cause.
func classificationOf(err error) (Failure, bool) {
	return walk(err, 0, func(e error) (Failure, bool) {
		classified, isOurs := e.(*Error)
		if !isOurs || classified.Category == "" {
			return Failure{}, false
		}
		return classified.Failure, true
	})
}

// VendorClassifier recognizes errors from one library, returning nil for anything else so the
// error falls through to another classifier or to the shared rules.
type VendorClassifier func(err error) *Failure

type registeredClassifier struct {
	component string
	classify  VendorClassifier
}

var (
	classifiersMu sync.RWMutex
	classifiers   []registeredClassifier
)

// Register adds a classifier for a source or destination's own error types. Each binary holds
// one source plus its destinations and registers from init, so the set is fixed before any
// command runs; classifiers return nil for errors they do not recognize, so several are safe.
func Register(component string, c VendorClassifier) {
	if c == nil {
		return
	}
	classifiersMu.Lock()
	defer classifiersMu.Unlock()
	classifiers = append(classifiers, registeredClassifier{component: component, classify: c})
}

// Classify attaches a failure classification to err. Called once, where the error is finally
// handled: it reads the error's structure rather than the call site, so one call at the top
// covers everything underneath. The message and chain are untouched, and it never panics.
func Classify(err error) (out error) {
	defer func() {
		if r := recover(); r != nil {
			out = err
		}
	}()

	if err == nil {
		return nil
	}
	// Already classified deeper in the chain; the root cause outranks anything here.
	if From(err).Category != Unclassified {
		return err
	}

	classifiersMu.RLock()
	registered := classifiers
	classifiersMu.RUnlock()

	for _, rc := range registered {
		if f := rc.classify(err); f != nil {
			f.Component = rc.component
			return Attach(err, *f)
		}
	}
	return Attach(err, standard(err))
}

// Standard applies only the shared rules, skipping every registered classifier. It exists for
// libraries that hold their cause in a field rather than the chain — a topology description, an
// SDK's OrigErr — which classifiers pull out by hand and pass here.
func Standard(err error) Failure {
	if err == nil {
		return Failure{Category: Unclassified, ClassifiedBy: ClassifiedByDefault}
	}
	return standard(err)
}

// standard classifies failures that never reached a server, so carry no vendor code. Identical
// for every connector, which is why they live here rather than in each driver.
func standard(err error) Failure {
	stdlib := func(category Category) Failure {
		return Failure{Category: category, ClassifiedBy: ClassifiedByStdlib}
	}

	// Checked before deadlines, which are timeouts.
	if errors.Is(err, context.Canceled) {
		return stdlib(Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return stdlib(Timeout)
	}

	// Before the generic network case: a TLS failure arrives wrapped in a network error and
	// would otherwise be misread as unreachable.
	var certErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var recordErr tls.RecordHeaderError
	switch {
	case errors.As(err, &certErr), errors.As(err, &unknownAuthority),
		errors.As(err, &hostnameErr), errors.As(err, &invalidCert),
		errors.As(err, &recordErr):
		return stdlib(TLSFailed)
	}

	// Two different fixes: a name that does not exist is usually a typo, a resolver that timed
	// out is infrastructure. The category alone cannot say which.
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

	// A network timeout reports itself as one; checked before the refused-connection case.
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

	// Unclassified rather than InternalError: a missing rule is not a bug, and keeping them
	// apart is what makes either number meaningful. The concrete type is the only clue left.
	return Failure{
		Category:     Unclassified,
		ClassifiedBy: ClassifiedByDefault,
		ErrorType:    rootType(err),
	}
}

// syscallCode splits network_unreachable by what actually happened: a wrong port, a firewall, a
// peer that hung up — different fixes, same category. Names rather than raw errno numbers, which
// differ between Linux and macOS; an errno with no entry here simply reports nothing.
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

// genericErrorType is what errors.New and a %w-less fmt.Errorf produce. Every such error has it,
// so it identifies nothing and is only reported when no branch offers anything better.
const genericErrorType = "*errors.errorString"

// namedType accepts a leaf whose concrete type identifies the failure.
func namedType(e error) (string, bool) {
	if _, joined := e.(interface{ Unwrap() []error }); joined || errors.Unwrap(e) != nil {
		return "", false
	}
	name := fmt.Sprintf("%T", e)
	return name, name != genericErrorType
}

// rootType names the concrete type at the bottom of the chain. The outermost is almost always
// *fmt.wrapError, which says nothing; the root identifies the failure and is what someone reads
// when deciding which rule to add next.
func rootType(err error) string {
	if name, ok := walk(err, 0, namedType); ok {
		return name
	}
	// Nothing anywhere names the failure, so report the type they all share.
	return genericErrorType
}
