package errs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leaf is a distinguishable concrete type, so rootType assertions cannot pass by accident.
type leaf struct{ msg string }

func (l *leaf) Error() string { return l.msg }

// selfWrapper unwraps to itself, the shape maxUnwrapDepth exists to bound.
type selfWrapper struct{ next error }

func (s *selfWrapper) Error() string { return "cycle" }
func (s *selfWrapper) Unwrap() error { return s.next }

func classified(category Category, code string) error {
	return Precondition(category, code, errors.New("cause"))
}

// TestFrom covers how a classification is found in every chain shape the codebase produces:
// a linear %w chain, an errors.Join tree, a two-%w-verb tree, and the degenerate cases.
func TestFrom(t *testing.T) {
	testCases := []struct {
		name             string
		err              error
		expectedCategory Category
		expectedCode     string
		expectedBy       string
	}{
		// nothing to classify
		{
			name:             "nil error",
			err:              nil,
			expectedCategory: Unclassified,
			expectedBy:       ClassifiedByDefault,
		},
		// no rule matches, which is a missing rule rather than a bug
		{
			name:             "unmatched error",
			err:              fmt.Errorf("wrapped: %w", &leaf{"nothing recognizes this"}),
			expectedCategory: Unclassified,
			expectedBy:       ClassifiedByDefault,
		},
		// %w descends one at a time and the root cause outranks anything above it
		{
			name: "linear chain, innermost wins",
			err: Attach(
				fmt.Errorf("while connecting: %w", classified(AuthFailed, "postgres.auth")),
				Failure{Category: ConfigInvalid, ClassifiedBy: ClassifiedByPrecondition, Code: "outer.code"}),
			expectedCategory: AuthFailed,
			expectedCode:     "postgres.auth",
			expectedBy:       ClassifiedByPrecondition,
		},
		// three levels deep, to prove the walk does not stop after one hop
		{
			name: "linear chain, three levels",
			err: fmt.Errorf("a: %w", fmt.Errorf("b: %w",
				classified(CDCPositionLost, "mssql.lsn_lost"))),
			expectedCategory: CDCPositionLost,
			expectedCode:     "mssql.lsn_lost",
			expectedBy:       ClassifiedByPrecondition,
		},
		// errors.Join drops the nil but still returns a join node, and errors.Unwrap does not
		// follow it: without the by-hand branch walk this classification would be invisible
		{
			name:             "join with a single branch",
			err:              errors.Join(nil, classified(CDCPositionLost, "mssql.lsn_lost")),
			expectedCategory: CDCPositionLost,
			expectedCode:     "mssql.lsn_lost",
			expectedBy:       ClassifiedByPrecondition,
		},
		// two %w verbs produce *fmt.wrapErrors, a tree rather than a linear chain
		{
			name: "two %w verbs is a tree",
			err: fmt.Errorf("%w: %w", errors.New("non retryable"),
				classified(SchemaUnsupported, "kafka.bad_schema")),
			expectedCategory: SchemaUnsupported,
			expectedCode:     "kafka.bad_schema",
			expectedBy:       ClassifiedByPrecondition,
		},
		// the sentinel-first pattern used across the drivers must not shadow the real error
		{
			name: "sentinel first, real error second",
			err: fmt.Errorf("%w: failed to read: %w", errors.New("sentinel"),
				classified(SourceReadError, "s3.read_failed")),
			expectedCategory: SourceReadError,
			expectedCode:     "s3.read_failed",
			expectedBy:       ClassifiedByPrecondition,
		},
		// branches are searched in order, so the first classified one answers
		{
			name:             "join, first classified branch wins",
			err:              errors.Join(classified(ConfigInvalid, "a.first"), classified(AuthFailed, "b.second")),
			expectedCategory: ConfigInvalid,
			expectedCode:     "a.first",
			expectedBy:       ClassifiedByPrecondition,
		},
		// the same two branches the other way round, pinning that order decides it
		{
			name:             "join, order decides the answer",
			err:              errors.Join(classified(AuthFailed, "b.second"), classified(ConfigInvalid, "a.first")),
			expectedCategory: AuthFailed,
			expectedCode:     "b.second",
			expectedBy:       ClassifiedByPrecondition,
		},
		// an unclassified branch is skipped rather than ending the walk
		{
			name: "join skips unclassified branches",
			err: errors.Join(errors.New("nothing here"), errors.New("nor here"),
				classified(ResourceExhausted, "z.last")),
			expectedCategory: ResourceExhausted,
			expectedCode:     "z.last",
			expectedBy:       ClassifiedByPrecondition,
		},
		// nested joins: the walk has to recurse, not just look one level down
		{
			name: "nested joins",
			err: errors.Join(
				errors.Join(errors.New("a"), errors.New("b")),
				errors.Join(errors.New("c"), classified(PermissionDenied, "iceberg.denied"))),
			expectedCategory: PermissionDenied,
			expectedCode:     "iceberg.denied",
			expectedBy:       ClassifiedByPrecondition,
		},
		// a branch deeper than the outer classification still outranks it
		{
			name: "join beneath an outer classification",
			err: Attach(fmt.Errorf("wrapped: %w", errors.Join(errors.New("a"), classified(TLSFailed, "deep.tls"))),
				Failure{Category: ConfigInvalid, ClassifiedBy: ClassifiedByPrecondition, Code: "outer"}),
			expectedCategory: TLSFailed,
			expectedCode:     "deep.tls",
			expectedBy:       ClassifiedByPrecondition,
		},
		// where no branch is classified the outer classification survives
		{
			name: "outer classification survives a fruitless join",
			err: Attach(fmt.Errorf("wrapped: %w", errors.Join(errors.New("a"), errors.New("b"))),
				Failure{Category: ConfigInvalid, ClassifiedBy: ClassifiedByPrecondition, Code: "outer.only"}),
			expectedCategory: ConfigInvalid,
			expectedCode:     "outer.only",
			expectedBy:       ClassifiedByPrecondition,
		},
		// a zero-valued Failure is not a classification, so the walk keeps descending
		{
			name:             "empty category is not a classification",
			err:              Attach(classified(AuthFailed, "inner.real"), Failure{}),
			expectedCategory: AuthFailed,
			expectedCode:     "inner.real",
			expectedBy:       ClassifiedByPrecondition,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := From(tc.err)
			assert.Equal(t, tc.expectedCategory, got.Category, "category")
			assert.Equal(t, tc.expectedCode, got.Code, "code")
			assert.Equal(t, tc.expectedBy, got.ClassifiedBy, "classified_by")
		})
	}
}

// TestFromTerminatesAndNeverPanics covers the two guarantees From documents: a cyclic chain
// cannot hang a failure report, and classification never replaces the real error with a panic.
func TestFromTerminatesAndNeverPanics(t *testing.T) {
	t.Run("cyclic chain terminates", func(t *testing.T) {
		cyclic := &selfWrapper{}
		cyclic.next = cyclic

		done := make(chan Failure, 1)
		go func() { done <- From(cyclic) }()

		select {
		case got := <-done:
			assert.Equal(t, Unclassified, got.Category)
		case <-time.After(5 * time.Second):
			require.Fail(t, "From did not terminate on a cyclic chain")
		}
	})

	t.Run("chain longer than maxUnwrapDepth terminates", func(t *testing.T) {
		deep := error(&leaf{"bottom"})
		for i := range maxUnwrapDepth * 2 {
			deep = fmt.Errorf("layer %d: %w", i, deep)
		}
		assert.NotPanics(t, func() { From(deep) })
	})

	t.Run("nil-typed pointer in the chain", func(t *testing.T) {
		// A non-nil interface holding a nil *Error would panic on field access without recover.
		var nilTyped *Error
		assert.Equal(t, Unclassified, From(fmt.Errorf("wrapped: %w", error(nilTyped))).Category)
	})
}

// TestAttachAndPrecondition covers the two constructors: nil handling, message preservation,
// chain preservation, and the component the code prefix implies.
func TestAttachAndPrecondition(t *testing.T) {
	t.Run("attach nil stays a nil interface", func(t *testing.T) {
		assert.Nil(t, Attach(nil, Failure{Category: AuthFailed}))
	})

	t.Run("message is unchanged", func(t *testing.T) {
		cause := &leaf{"the original text"}
		assert.Equal(t, cause.Error(), Precondition(AuthFailed, "x.y", cause).Error())
	})

	t.Run("errors.As still reaches the cause", func(t *testing.T) {
		var target *leaf
		require.True(t, errors.As(Precondition(AuthFailed, "x.y", &leaf{"c"}), &target))
		assert.Equal(t, "c", target.msg)
	})

	testCases := []struct {
		name              string
		code              string
		expectedComponent string
	}{
		// codes are written "<component>.<condition>", so the prefix names the connector
		{name: "component from prefix", code: "mongodb.resume_token_invalid", expectedComponent: "mongodb"},
		{name: "only the first segment", code: "s3.parser.decode", expectedComponent: "s3"},
		// a code without the separator is a caller mistake; the whole string is at least never
		// silently wrong about which connector it names
		{name: "no separator", code: "nodot", expectedComponent: "nodot"},
		{name: "empty code", code: "", expectedComponent: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := From(classified(StateInvalid, tc.code))
			assert.Equal(t, tc.expectedComponent, got.Component)
			assert.Equal(t, tc.code, got.Code)
		})
	}
}

// TestStandard covers every shared stdlib rule and the ordering between them. The order is the
// substance: a TLS failure arrives wrapped in a network error and must not read as unreachable.
func TestStandard(t *testing.T) {
	testCases := []struct {
		name             string
		err              error
		expectedCategory Category
		expectedCode     string
	}{
		// lifecycle, checked before deadlines
		{name: "context canceled", err: context.Canceled, expectedCategory: Canceled},
		{name: "canceled through a wrap", err: fmt.Errorf("stopped: %w", context.Canceled), expectedCategory: Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded, expectedCategory: Timeout},

		// TLS, checked before the generic network case
		{name: "tls cert verification", err: &tls.CertificateVerificationError{Err: errors.New("bad")}, expectedCategory: TLSFailed},
		{name: "x509 unknown authority", err: x509.UnknownAuthorityError{}, expectedCategory: TLSFailed},
		{name: "x509 hostname", err: x509.HostnameError{Host: "h"}, expectedCategory: TLSFailed},
		{name: "x509 cert invalid", err: x509.CertificateInvalidError{}, expectedCategory: TLSFailed},
		// crypto/tls returns this by value, so a pointer target would never match a real one
		{name: "tls record header", err: tls.RecordHeaderError{Msg: "bad"}, expectedCategory: TLSFailed},
		// a TLS failure inside a net.OpError must not be read as unreachable
		{
			name:             "tls beneath a net error",
			err:              &net.OpError{Op: "read", Err: &tls.CertificateVerificationError{Err: errors.New("x")}},
			expectedCategory: TLSFailed,
		},

		// DNS, split by what a reader would do about it
		{name: "dns not found", err: &net.DNSError{IsNotFound: true}, expectedCategory: DNSResolutionFailed, expectedCode: "host_not_found"},
		{name: "dns resolver timeout", err: &net.DNSError{IsTimeout: true}, expectedCategory: DNSResolutionFailed, expectedCode: "resolver_timeout"},
		{name: "dns, neither flag", err: &net.DNSError{}, expectedCategory: DNSResolutionFailed},

		// network, with the errno naming what actually happened
		{name: "connection refused", err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, expectedCategory: NetworkUnreachable, expectedCode: "connection_refused"},
		{name: "connection reset", err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}, expectedCategory: NetworkUnreachable, expectedCode: "connection_reset"},
		{name: "connection aborted", err: &net.OpError{Op: "read", Err: syscall.ECONNABORTED}, expectedCategory: NetworkUnreachable, expectedCode: "connection_aborted"},
		{name: "host unreachable", err: &net.OpError{Op: "dial", Err: syscall.EHOSTUNREACH}, expectedCategory: NetworkUnreachable, expectedCode: "host_unreachable"},
		{name: "network unreachable", err: &net.OpError{Op: "dial", Err: syscall.ENETUNREACH}, expectedCategory: NetworkUnreachable, expectedCode: "network_unreachable"},
		{name: "network down", err: &net.OpError{Op: "dial", Err: syscall.ENETDOWN}, expectedCategory: NetworkUnreachable, expectedCode: "network_down"},
		{name: "broken pipe", err: &net.OpError{Op: "write", Err: syscall.EPIPE}, expectedCategory: NetworkUnreachable, expectedCode: "broken_pipe"},
		// net.Error.Timeout() is checked before the socket cases, so this never reaches syscallCode
		{name: "errno that reports a timeout", err: &net.OpError{Op: "dial", Err: syscall.ETIMEDOUT}, expectedCategory: Timeout},
		// an errno with no entry reports nothing rather than a raw number
		{name: "errno with no entry", err: &net.OpError{Op: "dial", Err: syscall.EACCES}, expectedCategory: NetworkUnreachable},
		{name: "op error with an opaque cause", err: &net.OpError{Op: "dial", Err: errors.New("opaque")}, expectedCategory: NetworkUnreachable},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := Standard(tc.err)
			assert.Equal(t, tc.expectedCategory, got.Category, "category")
			assert.Equal(t, tc.expectedCode, got.Code, "code")
			assert.Equal(t, ClassifiedByStdlib, got.ClassifiedBy, "classified_by")
			assert.Empty(t, got.ErrorType, "error_type is only reported when nothing matched")
		})
	}

	t.Run("nil error", func(t *testing.T) {
		got := Standard(nil)
		assert.Equal(t, Unclassified, got.Category)
		assert.Equal(t, ClassifiedByDefault, got.ClassifiedBy)
	})
}

// TestRootType covers the error_type field, which is reported only when nothing classified the
// error and is therefore the sole remaining clue about which rule to write next.
func TestRootType(t *testing.T) {
	specific := fmt.Errorf("wrapped: %w", &leaf{"specific"})

	testCases := []struct {
		name              string
		err               error
		expectedErrorType string
	}{
		// the outermost is almost always *fmt.wrapError, which says nothing
		{name: "reaches the leaf through a chain", err: fmt.Errorf("a: %w", fmt.Errorf("b: %w", &leaf{"x"})), expectedErrorType: "*errs.leaf"},
		{name: "bare error", err: errors.New("x"), expectedErrorType: genericErrorType},
		// joined branches are searched, so a branch bottoming out in errors.New cannot mask a
		// specific type beside it
		{name: "join, specific type second", err: errors.Join(errors.New("generic"), specific), expectedErrorType: "*errs.leaf"},
		{name: "join, specific type first", err: errors.Join(specific, errors.New("generic")), expectedErrorType: "*errs.leaf"},
		{name: "nested join", err: errors.Join(errors.New("a"), errors.Join(errors.New("b"), specific)), expectedErrorType: "*errs.leaf"},
		// nothing better on offer, so the generic type is reported rather than nothing
		{name: "join, nothing specific anywhere", err: errors.Join(errors.New("a"), errors.New("b")), expectedErrorType: genericErrorType},
		// two %w verbs are the same tree shape
		{name: "two %w verbs", err: fmt.Errorf("%w: %w", errors.New("a"), specific), expectedErrorType: "*errs.leaf"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := Standard(tc.err)
			require.Equal(t, Unclassified, got.Category, "these inputs must reach the default branch")
			assert.Equal(t, tc.expectedErrorType, got.ErrorType)
		})
	}
}

// TestClassify covers the entry point ReportFailure uses: it must not override a deeper
// classification, must fall through to the shared rules, and must never alter the message.
func TestClassify(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		assert.Nil(t, Classify(nil))
	})

	t.Run("message is unchanged", func(t *testing.T) {
		original := errors.New("exact operator text")
		assert.Equal(t, original.Error(), Classify(original).Error())
	})

	t.Run("a deeper classification is kept", func(t *testing.T) {
		inner := classified(CDCPreconditionFailed, "postgres.slot_missing")
		got := From(Classify(fmt.Errorf("setup failed: %w", inner)))
		assert.Equal(t, CDCPreconditionFailed, got.Category)
		assert.Equal(t, "postgres.slot_missing", got.Code)
	})

	t.Run("falls through to the shared rules", func(t *testing.T) {
		got := From(Classify(fmt.Errorf("dial: %w", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED})))
		assert.Equal(t, NetworkUnreachable, got.Category)
		assert.Equal(t, ClassifiedByStdlib, got.ClassifiedBy)
		assert.Equal(t, "connection_refused", got.Code)
	})

	t.Run("unmatched reports the concrete type", func(t *testing.T) {
		got := From(Classify(fmt.Errorf("wrapped: %w", &leaf{"x"})))
		assert.Equal(t, Unclassified, got.Category)
		assert.Equal(t, ClassifiedByDefault, got.ClassifiedBy)
		assert.Equal(t, "*errs.leaf", got.ErrorType)
	})

	t.Run("classifying twice is stable", func(t *testing.T) {
		once := Classify(context.Canceled)
		assert.Equal(t, From(once), From(Classify(once)))
	})
}

// TestRegister covers the per-connector classifiers: the component is stamped from the
// registration, a nil classifier is ignored, and an unrecognized error still reaches the
// shared rules.
func TestRegister(t *testing.T) {
	Register("testcomponent", func(err error) *Failure {
		var l *leaf
		if errors.As(err, &l) && l.msg == "registered" {
			return &Failure{Category: CatalogError, ClassifiedBy: ClassifiedByVendor, Code: "tc.1"}
		}
		return nil
	})
	Register("nilclassifier", nil)

	t.Run("component is stamped from the registration", func(t *testing.T) {
		got := From(Classify(fmt.Errorf("wrapped: %w", &leaf{"registered"})))
		assert.Equal(t, CatalogError, got.Category)
		assert.Equal(t, ClassifiedByVendor, got.ClassifiedBy)
		assert.Equal(t, "testcomponent", got.Component)
	})

	t.Run("an unrecognized error falls through", func(t *testing.T) {
		assert.Equal(t, Canceled, From(Classify(context.Canceled)).Category)
	})

	t.Run("a panicking classifier cannot replace the real error", func(t *testing.T) {
		Register("panicking", func(err error) *Failure {
			var l *leaf
			if errors.As(err, &l) && l.msg == "trips the classifier" {
				panic("classifier blew up")
			}
			return nil
		})
		original := fmt.Errorf("wrapped: %w", &leaf{"trips the classifier"})
		assert.NotPanics(t, func() {
			assert.Same(t, original, Classify(original), "the original error is returned unchanged")
		})
	})

	t.Run("shared rules leave the component empty", func(t *testing.T) {
		// They cannot know whose endpoint was on the far end.
		assert.Empty(t, From(Classify(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED})).Component)
	})
}

// TestClassifiedByDefaultAccompaniesUnclassified pins the invariant every classifier relies on:
// the value reported when no rule matched, and that it never carries a real category.
func TestClassifiedByDefaultAccompaniesUnclassified(t *testing.T) {
	assert.Equal(t, "unclassified", ClassifiedByDefault,
		"the value reaches telemetry as classified_by and dashboards query it")

	for _, err := range []error{
		nil,
		errors.New("x"),
		fmt.Errorf("wrapped: %w", &leaf{"x"}),
		errors.Join(errors.New("a"), errors.New("b")),
	} {
		got := From(err)
		if got.ClassifiedBy == ClassifiedByDefault {
			assert.Equal(t, Unclassified, got.Category,
				"classified_by=%q must only ever accompany an unclassified category", ClassifiedByDefault)
		}
	}
}

// TestMultierrorIsNotWalked documents a known gap rather than asserting desired behavior:
// hashicorp/go-multierror unwraps to a `chain`, whose Unwrap returns the next chain rather than
// the error itself, so the by-hand walk never sees the *Error inside. errors.As does find it.
// Live only if ErrExecSequential gains a caller on a path that reaches ReportFailure.
func TestMultierrorIsNotWalked(t *testing.T) {
	inner := classified(AuthFailed, "postgres.auth_failed")

	t.Run("single error unwraps to the error itself", func(t *testing.T) {
		var multi error
		multi = multierror.Append(multi, inner)
		assert.Equal(t, AuthFailed, From(multi).Category)
	})

	t.Run("two or more errors are not reached by the walk", func(t *testing.T) {
		var multi error
		multi = multierror.Append(multi, errors.New("plain"))
		multi = multierror.Append(multi, inner)

		require.True(t, errors.As(multi, new(*Error)), "errors.As still finds it")
		assert.Equal(t, Unclassified, From(multi).Category,
			"KNOWN GAP: switching the type assertion in classificationOf to errors.As would close this")
	})
}
