package protocol

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSignalAwareRootContextCancelsOnSignal verifies that signalAwareRootContext
// cancels the returned context when the process receives SIGINT or SIGTERM.
//
// The test re-execs itself in a child process per signal case so that we can
// safely deliver a real OS signal without affecting the parent test runner.
// The OLAKE_SIGNAL_CONTEXT_HELPER env var switches the binary into "helper"
// mode; OLAKE_TEST_SIGNAL selects which signal the helper sends to itself.
func TestSignalAwareRootContextCancelsOnSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process signal behavior differs on windows")
	}

	tests := []struct {
		name   string
		signal syscall.Signal
		env    string
	}{
		{
			name:   "SIGTERM cancellation",
			signal: syscall.SIGTERM,
			env:    "SIGTERM",
		},
		{
			name:   "SIGINT cancellation",
			signal: syscall.SIGINT,
			env:    "SIGINT",
		},
	}

	// Helper-mode branch: runs inside the re-execed child. Installs the
	// signal handler under test, sends the chosen signal to ourselves, and
	// asserts the context cancels with context.Canceled within a bounded
	// timeout.
	if os.Getenv("OLAKE_SIGNAL_CONTEXT_HELPER") == "1" {
		ctx := signalAwareRootContext(context.Background())

		var signal syscall.Signal

		switch os.Getenv("OLAKE_TEST_SIGNAL") {
		case "SIGTERM":
			signal = syscall.SIGTERM
		case "SIGINT":
			signal = syscall.SIGINT
		default:
			t.Fatal("unknown test signal")
		}

		currentProcess, err := os.FindProcess(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}

		if err := currentProcess.Signal(signal); err != nil {
			t.Fatal(err)
		}

		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				t.Fatalf("expected canceled context, got %v", ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatalf("context was not canceled after %v", signal)
		}

		return
	}

	// Parent-mode branch: spawns one helper child per signal case and fails
	// the subtest if the child exits non-zero.
	// os.Executable rather than os.Args[0]: the parent process controls argv, the kernel controls this.
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(self, "-test.run=TestSignalAwareRootContextCancelsOnSignal")
			cmd.Env = append(os.Environ(), "OLAKE_SIGNAL_CONTEXT_HELPER=1", "OLAKE_TEST_SIGNAL="+tt.env)

			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("signal helper failed: %v\n%s", err, output)
			}
		})
	}
}

// TestSignalAwareRootContextPreservesParentCancellation verifies that
// canceling the parent context still propagates through the signal-aware
// wrapper. Without this, callers that cancel via context.WithCancel /
// context.WithTimeout would be silently ignored after the wrap.
func TestSignalAwareRootContextPreservesParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	ctx := signalAwareRootContext(parent)
	cancel()

	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			t.Fatalf("expected canceled context, got %v", ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("context was not canceled after parent cancellation")
	}
}

// runUnderRecover runs body with recoverToError deferred over an error that starts as prior.
// It reports the error a caller would be left with and the value that continued unwinding.
func runUnderRecover(prior error, body func()) (panicked any, err error) {
	defer func() { panicked = recover() }()

	err = prior
	func() {
		defer recoverToError(&err)
		body()
	}()
	return panicked, err
}

// uninitialisedCounts returns a nil map through a function boundary, so the write below is a real
// runtime panic rather than something an analyzer folds away.
func uninitialisedCounts() map[string]int { return nil }

type readStats struct{ rows int }

// noStats returns a nil pointer through a function boundary, for the same reason.
func noStats() *readStats { return nil }

// nilPanicValue returns an untyped nil, so panic(nilPanicValue()) is a genuine nil panic. Go
// 1.21+ turns that into *runtime.PanicNilError, which is what recover() then observes.
func nilPanicValue() any { return nil }

// TestRecoverToError covers the helper syncCmd defers so a panic cannot be reported as a
// successful run: it must classify the panic, write it through the pointer, and re-panic so the
// existing exit path is unchanged. Without a panic it must do nothing at all.
func TestRecoverToError(t *testing.T) {
	sentinel := errors.New("error occurred while reading records")
	classifiedCause := errs.Precondition(errs.CDCPositionLost, "mssql.lsn_lost", errors.New("lsn gone"))

	testCases := []struct {
		name             string
		prior            error         // the error already set when the deferred recover runs
		body             func()        // may panic
		expectedCategory errs.Category // empty means err must be left exactly as prior
		expectedMessage  string        // substring an operator must still be able to read
		expectedRePanic  any           // value safego.Recovery must see; nil skips the check
	}{
		// nothing panicked, so the helper must not manufacture an error
		{
			name: "no panic, no prior error",
			body: func() {},
		},
		// nothing panicked, so an error already set must survive byte for byte
		{
			name:  "no panic, prior error untouched",
			prior: sentinel,
			body:  func() {},
		},
		// the reported defect: a panic left err nil and the run reported SUCCESS
		{
			name:             "panic with a string",
			body:             func() { panic("connector.Read blew up") },
			expectedRePanic:  "connector.Read blew up",
			expectedCategory: errs.InternalError,
			expectedMessage:  "connector.Read blew up",
		},
		// a panic value that is itself an error must still be classified as the bug it is
		{
			name:             "panic with an error value",
			body:             func() { panic(sentinel) },
			expectedRePanic:  sentinel,
			expectedCategory: errs.InternalError,
			expectedMessage:  sentinel.Error(),
		},
		// a classified panic value must not have its classification promoted over the panic
		{
			name:             "panic with a classified error",
			body:             func() { panic(classifiedCause) },
			expectedRePanic:  classifiedCause,
			expectedCategory: errs.InternalError,
		},
		// a panic overrides an error already set: the run crashed, it did not merely fail
		{
			name:             "panic over a prior error",
			prior:            sentinel,
			body:             func() { panic("boom") },
			expectedCategory: errs.InternalError,
			expectedMessage:  "boom",
		},
		// non-error panic values must not break the %v formatting
		{
			name:             "panic with an int",
			body:             func() { panic(42) },
			expectedRePanic:  42,
			expectedCategory: errs.InternalError,
			expectedMessage:  "42",
		},
		{
			name:             "panic with a struct",
			body:             func() { panic(struct{ Stream string }{"users"}) },
			expectedRePanic:  struct{ Stream string }{"users"},
			expectedCategory: errs.InternalError,
			expectedMessage:  "users",
		},
		// panic(nil) becomes *runtime.PanicNilError in Go 1.21+, so recover() still sees non-nil
		{
			name:             "panic with nil",
			body:             func() { panic(nilPanicValue()) },
			expectedCategory: errs.InternalError,
		},
		// the runtime panics a driver bug actually produces, rather than explicit panic() calls
		{
			name:             "runtime panic, nil map write",
			body:             func() { uninitialisedCounts()["x"] = 1 },
			expectedCategory: errs.InternalError,
			expectedMessage:  "nil map",
		},
		{
			name:             "runtime panic, nil pointer dereference",
			body:             func() { _ = noStats().rows },
			expectedCategory: errs.InternalError,
			expectedMessage:  "nil pointer dereference",
		},
		{
			name:             "runtime panic, index out of range",
			body:             func() { s := []int{}; _ = s[len(s)] },
			expectedCategory: errs.InternalError,
			expectedMessage:  "index out of range",
		},
		// the shape utils.Ternary(...).(error) can hit
		{
			name:             "runtime panic, failed type assertion",
			body:             func() { var v any = "not an error"; _ = v.(error) },
			expectedCategory: errs.InternalError,
			expectedMessage:  "interface conversion",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			panicked, err := runUnderRecover(tc.prior, tc.body)

			// no classification expected means nothing panicked and err is exactly what it was
			if tc.expectedCategory == "" {
				assert.Nil(t, panicked)
				if tc.prior == nil {
					assert.NoError(t, err)
				} else {
					assert.Same(t, tc.prior, err)
				}
				return
			}

			// the panic must continue so safego.Recovery still logs the stack and exits non-zero
			assert.NotNil(t, panicked, "the panic must not be swallowed")
			// and it must be the original value, not the classified error built from it
			if tc.expectedRePanic != nil {
				assert.Equal(t, tc.expectedRePanic, panicked)
			}

			require.Error(t, err)
			got := errs.From(errs.Classify(err))
			assert.Equal(t, tc.expectedCategory, got.Category, "category")
			// every panic reaches the same code, so these are constant across the panic cases
			assert.Equal(t, codePanicRecovered, got.Code, "code")
			assert.Equal(t, "sync", got.Component, "component comes from the code prefix")

			// classification must never change what an operator reads
			assert.True(t, strings.HasPrefix(err.Error(), "panic during sync:"),
				"message should say a panic happened, got %q", err.Error())
			if tc.expectedMessage != "" {
				assert.Contains(t, err.Error(), tc.expectedMessage)
			}
		})
	}
}
