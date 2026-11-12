package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchExitState pins the marker each rebalance exit mode returns: a lost partition stops the
// reader without in-process retries (ErrRestartRequired, exit 1), and never asks for manual intervention.
func TestFetchExitState(t *testing.T) {
	testCases := []struct {
		name         string
		nilManager   bool // discover runs without a reader manager
		exitMode     int32
		expectedStop bool
		expectedErr  bool
	}{
		{name: "normal processing continues", exitMode: normalProcessing},
		{name: "graceful exit stops without an error", exitMode: gracefulExit, expectedStop: true},
		{name: "partition loss stops with ErrRestartRequired", exitMode: partitionLostExit, expectedStop: true, expectedErr: true},
		{name: "unknown exit mode stops with ErrRestartRequired", exitMode: 99, expectedStop: true, expectedErr: true},
		{name: "nil reader manager continues", nilManager: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var r *ReaderManager
			if !tc.nilManager {
				r = NewReaderManager(ReaderConfig{})
				r.exitMode.Store(tc.exitMode)
			}

			stop, err := r.FetchExitState()
			assert.Equal(t, tc.expectedStop, stop, "stop")
			if !tc.expectedErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, utils.IsRestartRequired(err), "stops in-process retries")
			assert.False(t, utils.IsManualIntervention(err), "a restart can recover, so it must not exit 3")
		})
	}
}
