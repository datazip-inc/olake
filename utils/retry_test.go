package utils

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetryOnBackoffMarkers pins how many times RetryOnBackoff runs f for each kind of error:
// unmarked errors are retried, ErrRestartRequired and ErrManualIntervention stop after the first attempt.
func TestRetryOnBackoffMarkers(t *testing.T) {
	plain := errors.New("connection reset by peer")
	restartRequired := fmt.Errorf("%w: LSN not updated after 5 minutes", constants.ErrRestartRequired)
	manualIntervention := fmt.Errorf("%w: publication does not exist", constants.ErrManualIntervention)

	testCases := []struct {
		name          string
		attempts      int
		errs          []error // error returned by each call; calls past the end return nil
		expectedCalls int
		expectedErr   error
	}{
		{name: "success on first attempt", attempts: 3, errs: nil, expectedCalls: 1},
		{name: "plain error is retried until attempts run out", attempts: 3, errs: []error{plain, plain, plain, plain}, expectedCalls: 4, expectedErr: plain},
		{name: "plain error then success", attempts: 3, errs: []error{plain}, expectedCalls: 2},
		{name: "ErrRestartRequired stops after one attempt", attempts: 3, errs: []error{restartRequired}, expectedCalls: 1, expectedErr: restartRequired},
		{name: "ErrManualIntervention stops after one attempt", attempts: 3, errs: []error{manualIntervention}, expectedCalls: 1, expectedErr: manualIntervention},
		{name: "plain error then ErrManualIntervention stops at the marker", attempts: 3, errs: []error{plain, manualIntervention}, expectedCalls: 2, expectedErr: manualIntervention},
		{
			name:          "ErrManualIntervention inside errs.Precondition stops",
			attempts:      3,
			errs:          []error{errs.Precondition(errs.StateInvalid, "kafka.offset_mismatch", fmt.Errorf("%w: clear destination", constants.ErrManualIntervention))},
			expectedCalls: 1,
		},
		{
			name:          "ErrRestartRequired wrapped by an outer %w layer stops",
			attempts:      3,
			errs:          []error{fmt.Errorf("reader[0]: sync committed offsets failed: %w", restartRequired)},
			expectedCalls: 1,
		},
		// a %s wrap drops the marker, so the error is retried like a plain one
		{
			name:          "marker lost through %s is retried",
			attempts:      3,
			errs:          []error{fmt.Errorf("outer: %s", manualIntervention), fmt.Errorf("outer: %s", manualIntervention), fmt.Errorf("outer: %s", manualIntervention), fmt.Errorf("outer: %s", manualIntervention)},
			expectedCalls: 4,
		},
		{name: "single attempt is not retried", attempts: 0, errs: []error{plain}, expectedCalls: 1, expectedErr: plain},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := RetryOnBackoff(context.Background(), tc.attempts, time.Millisecond, func(context.Context) error {
				calls++
				if calls <= len(tc.errs) {
					return tc.errs[calls-1]
				}
				return nil
			})

			assert.Equal(t, tc.expectedCalls, calls, "calls")
			if tc.expectedCalls > len(tc.errs) {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err, "the error from the last call is returned unchanged")
			}
		})
	}
}

// TestRetryOnBackoffCanceled: a canceled context ends the loop with ctx.Err(), never with a marker.
func TestRetryOnBackoffCanceled(t *testing.T) {
	testCases := []struct {
		name              string
		cancelBeforeStart bool
		sleep             time.Duration
		expectedCalls     int
	}{
		{name: "canceled before the first attempt", cancelBeforeStart: true, sleep: time.Millisecond, expectedCalls: 0},
		// the loop would wait an hour to retry; cancellation must end it
		{name: "canceled during the backoff wait", sleep: time.Hour, expectedCalls: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelBeforeStart {
				cancel()
			}

			calls := 0
			err := RetryOnBackoff(ctx, 3, tc.sleep, func(context.Context) error {
				calls++
				cancel()
				return errors.New("connection reset by peer")
			})

			assert.Equal(t, tc.expectedCalls, calls)
			assert.ErrorIs(t, err, context.Canceled)
			assert.False(t, IsRestartRequired(err))
			assert.False(t, IsManualIntervention(err))
		})
	}
}
