package protocol

import (
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deferOrder selects how recoverToError is registered relative to the telemetry defer.
type deferOrder int

const (
	orderCorrect deferOrder = iota // registered last, so LIFO runs it first — what sync.go does
	orderWrong                     // registered first, so it runs after telemetry has read err
	orderAbsent                    // no recoverToError at all, the state before the fix
)

// runRunE mirrors syncCmd's RunE: a named result, the TrackSyncCompleted defer that reads
// err == nil, and recoverToError placed according to order. It reports the sync_status the
// telemetry defer would send, and the value that continued unwinding.
func runRunE(order deferOrder, body func() error) (status string, panicked any) {
	defer func() { panicked = recover() }()

	inner := func() (err error) {
		if order == orderWrong {
			defer recoverToError(&err)
		}
		// stands in for the TrackSyncCompleted defer at sync.go:158
		defer func() {
			status = map[bool]string{true: "SUCCESS", false: "FAILED"}[err == nil]
		}()
		if order == orderCorrect {
			defer recoverToError(&err)
		}
		return body()
	}
	_ = inner()
	return status, panicked
}

// TestRunEStatus covers every way syncCmd's RunE can finish and the sync_status each produces.
// How the panic is classified is recoverToError's contract, covered in root_test.go; what
// matters here is that no way of dying can be reported as a success.
func TestRunEStatus(t *testing.T) {
	classifiedCause := errs.Precondition(errs.CDCPositionLost, "mssql.lsn_lost", errors.New("lsn gone"))

	testCases := []struct {
		name           string
		body           func() error
		expectedStatus string
		expectedPanic  bool
	}{
		// a clean run must not be turned into a failure by the recover
		{
			name:           "returns nil",
			body:           func() error { return nil },
			expectedStatus: "SUCCESS",
		},
		// an ordinary read failure was already reported correctly before the fix
		{
			name:           "returns a plain error",
			body:           func() error { return errors.New("error occurred while reading records") },
			expectedStatus: "FAILED",
		},
		// a driver-classified failure is still just a failure as far as sync_status goes
		{
			name:           "returns a classified error",
			body:           func() error { return classifiedCause },
			expectedStatus: "FAILED",
		},
		// the wrapping RunE applies to a read failure must not change the status either
		{
			name:           "returns a wrapped error",
			body:           func() error { return fmt.Errorf("error occurred while reading records: %w", classifiedCause) },
			expectedStatus: "FAILED",
		},
		// the reported defect: a panic left err nil, so the dying run reported SUCCESS
		{
			name:           "panics with a string",
			body:           func() error { panic("connector.Read blew up") },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		// a panic value that is itself an error must not be mistaken for a clean return
		{
			name:           "panics with an error value",
			body:           func() error { panic(errors.New("boom")) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		// nor one that is already classified
		{
			name:           "panics with a classified error",
			body:           func() error { panic(classifiedCause) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		// non-error panic values must reach the same status
		{
			name:           "panics with an int",
			body:           func() error { panic(42) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		{
			name:           "panics with a struct",
			body:           func() error { panic(struct{ Stream string }{"users"}) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		// panic(nil) becomes *runtime.PanicNilError in Go 1.21+, so recover() still sees non-nil
		{
			name:           "panics with nil",
			body:           func() error { panic(nilPanicValue()) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		// the runtime panics a driver bug actually produces, rather than explicit panic() calls
		{
			name:           "runtime panic, nil map write",
			body:           func() error { uninitialisedCounts()["x"] = 1; return nil },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		{
			name:           "runtime panic, nil pointer dereference",
			body:           func() error { return fmt.Errorf("unreachable: %d", noStats().rows) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		{
			name:           "runtime panic, index out of range",
			body:           func() error { s := []int{}; return fmt.Errorf("%d", s[len(s)]) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
		// the shape utils.Ternary(...).(error) can hit
		{
			name:           "runtime panic, failed type assertion",
			body:           func() error { var v any = "not an error"; return v.(error) },
			expectedStatus: "FAILED",
			expectedPanic:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, panicked := runRunE(orderCorrect, tc.body)

			assert.Equal(t, tc.expectedStatus, status, "sync_status")
			// the panic must continue so safego.Recovery still logs the stack and exits non-zero
			assert.Equal(t, tc.expectedPanic, panicked != nil, "panic continued")
		})
	}
}

// TestRunEDeferOrder covers all three registrations against all three outcomes. The wrong and
// absent orders must still reproduce the defect: if either stops doing so, the LIFO reasoning
// the fix in sync.go depends on has changed.
func TestRunEDeferOrder(t *testing.T) {
	testCases := []struct {
		name           string
		order          deferOrder
		body           func() error
		expectedStatus string
	}{
		// only a panic is sensitive to registration order
		{name: "panic, correct order", order: orderCorrect, body: func() error { panic("boom") }, expectedStatus: "FAILED"},
		{name: "panic, wrong order", order: orderWrong, body: func() error { panic("boom") }, expectedStatus: "SUCCESS"},
		{name: "panic, no recover", order: orderAbsent, body: func() error { panic("boom") }, expectedStatus: "SUCCESS"},
		// a clean run reports SUCCESS whatever the order
		{name: "clean run, correct order", order: orderCorrect, body: func() error { return nil }, expectedStatus: "SUCCESS"},
		{name: "clean run, wrong order", order: orderWrong, body: func() error { return nil }, expectedStatus: "SUCCESS"},
		{name: "clean run, no recover", order: orderAbsent, body: func() error { return nil }, expectedStatus: "SUCCESS"},
		// so does an ordinary error, which never depended on the recover
		{name: "error, correct order", order: orderCorrect, body: func() error { return errors.New("x") }, expectedStatus: "FAILED"},
		{name: "error, wrong order", order: orderWrong, body: func() error { return errors.New("x") }, expectedStatus: "FAILED"},
		{name: "error, no recover", order: orderAbsent, body: func() error { return errors.New("x") }, expectedStatus: "FAILED"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, _ := runRunE(tc.order, tc.body)
			assert.Equal(t, tc.expectedStatus, status, "sync_status")
		})
	}
}

// TestRunEIsDeterministic runs each outcome repeatedly. Nothing here may depend on goroutine
// scheduling, so the same input must always produce the same sync_status.
func TestRunEIsDeterministic(t *testing.T) {
	const runs = 200

	testCases := []struct {
		name string
		body func() error
	}{
		{name: "nil", body: func() error { return nil }},
		{name: "error", body: func() error { return errors.New("x") }},
		{name: "panic", body: func() error { panic("boom") }},
		{name: "panic with nil", body: func() error { panic(nilPanicValue()) }},
		{name: "runtime panic", body: func() error { uninitialisedCounts()["x"] = 1; return nil }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			first, _ := runRunE(orderCorrect, tc.body)
			for i := range runs {
				status, _ := runRunE(orderCorrect, tc.body)
				require.Equal(t, first, status, "run %d disagreed on sync_status", i)
			}
		})
	}
}

// TestRunEReportsTheClassifiedError covers the join between the two files: the error
// recoverToError writes is the same one the telemetry defer reads to derive FAILED.
func TestRunEReportsTheClassifiedError(t *testing.T) {
	var observed error

	func() {
		defer func() { _ = recover() }()
		inner := func() (err error) {
			defer func() { observed = err }()
			defer recoverToError(&err)
			panic("boom")
		}
		_ = inner()
	}()

	require.Error(t, observed, "the telemetry defer must see a non-nil error, which is what makes it FAILED")
	assert.Equal(t, errs.InternalError, errs.From(errs.Classify(observed)).Category)
}
