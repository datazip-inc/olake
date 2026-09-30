package constants

import (
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
)

// TestRetryMarkers pins what IsRetryable and IsNonRetryable report for each shape an error can
// take on its way from a driver to connector.go.
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
		{name: "plain error has no marker", err: errors.New("connection reset by peer")},
		{name: "ErrRetryable itself", err: ErrRetryable, expectedRetryable: true},
		{name: "wrapped ErrRetryable", err: retryable, expectedRetryable: true},
		// ErrNonRetryable wraps ErrRetryable, so it also stops in-process retries
		{name: "ErrNonRetryable itself", err: ErrNonRetryable, expectedRetryable: true, expectedNonRetryable: true},
		{name: "wrapped ErrNonRetryable", err: nonRetryable, expectedRetryable: true, expectedNonRetryable: true},
		// every layer between the driver and connector.go wraps with %w
		{
			name:                 "ErrNonRetryable through several %w layers",
			err:                  fmt.Errorf("error occurred while reading records: %w", fmt.Errorf("failed in pre cdc run for driver[postgres]: %w", nonRetryable)),
			expectedRetryable:    true,
			expectedNonRetryable: true,
		},
		{
			name:              "ErrRetryable through several %w layers",
			err:               fmt.Errorf("error occurred while waiting for connections: %w", fmt.Errorf("reader[0]: %w", retryable)),
			expectedRetryable: true,
		},
		// the kafka and postgres state errors are wrapped in errs.Precondition
		{
			name:                 "ErrNonRetryable inside errs.Precondition",
			err:                  errs.Precondition(errs.StateInvalid, "kafka.offset_mismatch", fmt.Errorf("%w: run clear destination and restart", ErrNonRetryable)),
			expectedRetryable:    true,
			expectedNonRetryable: true,
		},
		{
			name:              "ErrRetryable inside errs.Precondition",
			err:               errs.Precondition(errs.StateInvalid, "kafka.partition_metadata_absent", fmt.Errorf("%w: partition missing", ErrRetryable)),
			expectedRetryable: true,
		},
		// writer cleanup and post cdc join errors
		{
			name:                 "ErrNonRetryable inside errors.Join",
			err:                  errors.Join(errors.New("failed to close writer"), nonRetryable),
			expectedRetryable:    true,
			expectedNonRetryable: true,
		},
		{
			name:              "ErrRetryable inside errors.Join",
			err:               errors.Join(errors.New("failed to close writer"), retryable),
			expectedRetryable: true,
		},
		// %s keeps only the text, so the marker is lost: every wrap on these paths must use %w
		{name: "ErrNonRetryable lost through %s", err: fmt.Errorf("outer: %s", nonRetryable)},
		{name: "ErrRetryable lost through %s", err: fmt.Errorf("outer: %s", retryable)},
		// a message that only mentions the words is not the marker
		{name: "matching text without the marker", err: errors.New("manual intervention required")},
		// other sentinels are not retry markers
		{name: "ErrGlobalContextGroup is not a marker", err: fmt.Errorf("%w: x", ErrGlobalContextGroup)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedRetryable, IsRetryable(tc.err), "IsRetryable")
			assert.Equal(t, tc.expectedNonRetryable, IsNonRetryable(tc.err), "IsNonRetryable")
		})
	}
}

// TestExitCodes pins the exit code contract with process supervisors.
func TestExitCodes(t *testing.T) {
	assert.Equal(t, 1, ExitCodeFailure)
	assert.Equal(t, 3, ExitCodeManualIntervention)
	// the Go runtime exits 2 on an unrecovered panic or a fatal runtime error
	assert.NotEqual(t, 2, ExitCodeManualIntervention)
	assert.NotEqual(t, ExitCodeFailure, ExitCodeManualIntervention)
}
