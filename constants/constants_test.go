package constants

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsNonRetryable(t *testing.T) {
	t.Run("plain sentinel is detected", func(t *testing.T) {
		require.True(t, IsNonRetryable(ErrNonRetryable))
	})

	t.Run("wrapped sentinel is detected", func(t *testing.T) {
		err := fmt.Errorf("%w: stream orders has invalid state", ErrNonRetryable)
		require.True(t, IsNonRetryable(err))
	})

	t.Run("generic message with same wording is not treated as non retryable", func(t *testing.T) {
		err := errors.New("failed with non retryable error in a generic log line")
		require.False(t, IsNonRetryable(err))
	})

	t.Run("nil error is not non retryable", func(t *testing.T) {
		require.False(t, IsNonRetryable(nil))
	})
}
