package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchExitState pins the marker each rebalance exit mode returns: a lost partition stops the
// reader without in-process retries (ErrRetryable, exit 1), and never asks for manual intervention.
func TestFetchExitState(t *testing.T) {
	testCases := []struct {
		name         string
		exitMode     int32
		expectedStop bool
		expectedErr  bool
	}{
		{name: "normal processing continues", exitMode: normalProcessing},
		{name: "graceful exit stops without an error", exitMode: gracefulExit, expectedStop: true},
		{name: "partition loss stops with ErrRetryable", exitMode: nonRetryableExit, expectedStop: true, expectedErr: true},
		{name: "unknown exit mode stops with ErrRetryable", exitMode: 99, expectedStop: true, expectedErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReaderManager(ReaderConfig{})
			r.exitMode.Store(tc.exitMode)

			stop, err := r.FetchExitState()
			assert.Equal(t, tc.expectedStop, stop, "stop")
			if !tc.expectedErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, constants.IsRetryable(err), "stops in-process retries")
			assert.False(t, constants.IsNonRetryable(err), "a restart can recover, so it must not exit 3")
		})
	}

	// discover runs without a reader manager
	t.Run("nil reader manager continues", func(t *testing.T) {
		var r *ReaderManager
		stop, err := r.FetchExitState()
		assert.False(t, stop)
		assert.NoError(t, err)
	})
}
