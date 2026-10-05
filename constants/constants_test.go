package constants

import (
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
)

// TestRetryMarkers checks what IsRetryable and IsNonRetryable report for each shape an error
// can take on its way from a driver to connector.go.
func TestRetryMarkers(t *testing.T) {
	retryable := fmt.Errorf("%w: kafka sync aborted due to partition loss", ErrRetryable)
	nonRetryable := fmt.Errorf("%w: publication %q does not exist", ErrNonRetryable, "olake_pub")

	testCases := []struct {
		name                 string
		err                  error
		expectedRetryable    bool
		expectedNonRetryable bool
	}{
		{name: "nil error", err: nil},
		{name: "no marker", err: errors.New("connection reset by peer")},
		{name: "ErrRetryable", err: retryable, expectedRetryable: true},
		// ErrNonRetryable wraps ErrRetryable, so it also stops the retry loop
		{name: "ErrNonRetryable", err: nonRetryable, expectedRetryable: true, expectedNonRetryable: true},
		// every layer between a driver and connector.go wraps with %w
		{
			name:              "ErrRetryable through several %w layers",
			err:               fmt.Errorf("error occurred while waiting for connections: %w", fmt.Errorf("reader[0]: %w", retryable)),
			expectedRetryable: true,
		},
		{
			name:                 "ErrNonRetryable through several %w layers",
			err:                  fmt.Errorf("error occurred while reading records: %w", fmt.Errorf("failed in pre cdc run: %w", nonRetryable)),
			expectedRetryable:    true,
			expectedNonRetryable: true,
		},
		// state errors are wrapped in errs.Precondition for telemetry
		{
			name:              "ErrRetryable inside errs.Precondition",
			err:               errs.Precondition(errs.StateInvalid, "kafka.partition_metadata_absent", fmt.Errorf("%w: partition missing", ErrRetryable)),
			expectedRetryable: true,
		},
		{
			name:                 "ErrNonRetryable inside errs.Precondition",
			err:                  errs.Precondition(errs.StateInvalid, "kafka.offset_mismatch", fmt.Errorf("%w: run clear destination", ErrNonRetryable)),
			expectedRetryable:    true,
			expectedNonRetryable: true,
		},
		// writer cleanup joins errors
		{
			name:                 "ErrNonRetryable inside errors.Join",
			err:                  errors.Join(errors.New("failed to close writer"), nonRetryable),
			expectedRetryable:    true,
			expectedNonRetryable: true,
		},
		// %s keeps only the text, so the marker is lost: every wrap must use %w
		{name: "ErrRetryable lost through %s", err: fmt.Errorf("outer: %s", retryable)},
		{name: "ErrNonRetryable lost through %s", err: fmt.Errorf("outer: %s", nonRetryable)},
		// the same words without the marker are not the marker
		{name: "matching text only", err: errors.New("manual intervention required")},
		{name: "ErrGlobalContextGroup is not a marker", err: fmt.Errorf("%w: x", ErrGlobalContextGroup)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedRetryable, IsRetryable(tc.err), "IsRetryable")
			assert.Equal(t, tc.expectedNonRetryable, IsNonRetryable(tc.err), "IsNonRetryable")
		})
	}
}

// TestExitCodes checks the exit code contract with the process that runs OLake.
func TestExitCodes(t *testing.T) {
	assert.Equal(t, 1, ExitCodeFailure)
	assert.Equal(t, 3, ExitCodeManualIntervention)
	// the Go runtime exits 2 on a crash, so 2 must not mean "manual intervention"
	assert.NotEqual(t, 2, ExitCodeManualIntervention)
}
