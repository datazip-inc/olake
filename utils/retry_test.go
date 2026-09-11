package utils

import (
	"errors"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/require"
)

func TestIsNonRetryable(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		require.False(t, IsNonRetryable(nil))
	})

	t.Run("unrelated error", func(t *testing.T) {
		require.False(t, IsNonRetryable(errors.New("connection reset")))
	})

	t.Run("wrapped with %w", func(t *testing.T) {
		err := fmt.Errorf("%w: lsn mismatch", constants.ErrNonRetryable)
		require.True(t, IsNonRetryable(err))
	})

	t.Run("message-only match, e.g. wrapped with %s instead of %w", func(t *testing.T) {
		err := fmt.Errorf("%s: clear destination and restart", constants.ErrNonRetryable)
		require.True(t, IsNonRetryable(err))
	})
}
