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

// TestRetryOnBackoffMarkers checks how many times RetryOnBackoff runs f: errors with no marker
// are retried, ErrRetryable and ErrNonRetryable stop after the first attempt.
func TestRetryOnBackoffMarkers(t *testing.T) {
	plain := errors.New("connection reset by peer")
	retryable := fmt.Errorf("%w: LSN not updated after 5 minutes", constants.ErrRetryable)
	nonRetryable := fmt.Errorf("%w: publication does not exist", constants.ErrNonRetryable)

	testCases := []struct {
		name          string
		errs          []error // error returned by each call; calls past the end succeed
		expectedCalls int
		expectedErr   error // nil = success expected
	}{
		{name: "success on the first attempt", errs: nil, expectedCalls: 1},
		{name: "no marker is retried until attempts run out", errs: []error{plain, plain, plain}, expectedCalls: 3, expectedErr: plain},
		{name: "no marker, then success", errs: []error{plain}, expectedCalls: 2},
		{name: "ErrRetryable stops after one attempt", errs: []error{retryable}, expectedCalls: 1, expectedErr: retryable},
		{name: "ErrNonRetryable stops after one attempt", errs: []error{nonRetryable}, expectedCalls: 1, expectedErr: nonRetryable},
		{name: "no marker, then ErrNonRetryable stops there", errs: []error{plain, nonRetryable}, expectedCalls: 2, expectedErr: nonRetryable},
		{
			name:          "marker inside errs.Precondition stops",
			errs:          []error{errs.Precondition(errs.StateInvalid, "postgres.lsn_mismatch", fmt.Errorf("%w: clear destination", constants.ErrNonRetryable))},
			expectedCalls: 1,
		},
		{
			name:          "marker wrapped by an outer %w layer stops",
			errs:          []error{fmt.Errorf("reader[0]: sync failed: %w", retryable)},
			expectedCalls: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := RetryOnBackoff(context.Background(), 3, time.Millisecond, func(context.Context) error {
				calls++
				if calls <= len(tc.errs) {
					return tc.errs[calls-1]
				}
				return nil
			})

			assert.Equal(t, tc.expectedCalls, calls, "attempts")
			if tc.expectedCalls > len(tc.errs) {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.expectedErr != nil {
				assert.Equal(t, tc.expectedErr, err, "the last error is returned unchanged")
			}
		})
	}
}

// TestRetryOnBackoffCanceled: a canceled context ends the loop with ctx.Err(), never with a marker.
func TestRetryOnBackoffCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	err := RetryOnBackoff(ctx, 3, time.Hour, func(context.Context) error {
		calls++
		cancel() // the loop now waits an hour before retrying; cancel must end it
		return errors.New("connection reset by peer")
	})

	assert.Equal(t, 1, calls)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, constants.IsNonRetryable(err))
}
