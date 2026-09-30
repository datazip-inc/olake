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
// unmarked errors are retried, ErrRetryable and ErrNonRetryable stop after the first attempt.
func TestRetryOnBackoffMarkers(t *testing.T) {
	plain := errors.New("connection reset by peer")
	retryable := fmt.Errorf("%w: LSN not updated after 5 minutes", constants.ErrRetryable)
	nonRetryable := fmt.Errorf("%w: publication does not exist", constants.ErrNonRetryable)

	testCases := []struct {
		name          string
		attempts      int
		errs          []error // error returned by each call; calls past the end return nil
		expectedCalls int
		expectedErr   error
	}{
		{name: "success on first attempt", attempts: 3, errs: nil, expectedCalls: 1},
		{name: "plain error is retried until attempts run out", attempts: 3, errs: []error{plain, plain, plain}, expectedCalls: 3, expectedErr: plain},
		{name: "plain error then success", attempts: 3, errs: []error{plain}, expectedCalls: 2},
		{name: "ErrRetryable stops after one attempt", attempts: 3, errs: []error{retryable}, expectedCalls: 1, expectedErr: retryable},
		{name: "ErrNonRetryable stops after one attempt", attempts: 3, errs: []error{nonRetryable}, expectedCalls: 1, expectedErr: nonRetryable},
		{name: "plain error then ErrNonRetryable stops at the marker", attempts: 3, errs: []error{plain, nonRetryable}, expectedCalls: 2, expectedErr: nonRetryable},
		{
			name:          "ErrNonRetryable inside errs.Precondition stops",
			attempts:      3,
			errs:          []error{errs.Precondition(errs.StateInvalid, "kafka.offset_mismatch", fmt.Errorf("%w: clear destination", constants.ErrNonRetryable))},
			expectedCalls: 1,
		},
		{
			name:          "ErrRetryable wrapped by an outer %w layer stops",
			attempts:      3,
			errs:          []error{fmt.Errorf("reader[0]: sync committed offsets failed: %w", retryable)},
			expectedCalls: 1,
		},
		// a %s wrap drops the marker, so the error is retried like a plain one
		{
			name:          "marker lost through %s is retried",
			attempts:      3,
			errs:          []error{fmt.Errorf("outer: %s", nonRetryable), fmt.Errorf("outer: %s", nonRetryable), fmt.Errorf("outer: %s", nonRetryable)},
			expectedCalls: 3,
		},
		{name: "single attempt is not retried", attempts: 1, errs: []error{plain}, expectedCalls: 1, expectedErr: plain},
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
	t.Run("canceled before the first attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		calls := 0
		err := RetryOnBackoff(ctx, 3, time.Millisecond, func(context.Context) error {
			calls++
			return nil
		})

		assert.Equal(t, 0, calls)
		assert.ErrorIs(t, err, context.Canceled)
		assert.False(t, constants.IsRetryable(err))
		assert.False(t, constants.IsNonRetryable(err))
	})

	t.Run("canceled during the backoff wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		calls := 0
		err := RetryOnBackoff(ctx, 3, time.Hour, func(context.Context) error {
			calls++
			cancel() // the loop is now waiting an hour to retry; cancellation must end it
			return errors.New("connection reset by peer")
		})

		assert.Equal(t, 1, calls)
		assert.ErrorIs(t, err, context.Canceled)
		assert.False(t, constants.IsNonRetryable(err))
	})
}
