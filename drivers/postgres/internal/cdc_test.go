package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/types"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateGlobalState(t *testing.T) {
	t.Parallel()

	t.Run("matching LSN passes validation", func(t *testing.T) {
		lsn := "0/1234568"
		parsed, err := pglogrepl.ParseLSN(lsn)
		require.NoError(t, err)

		globalState := &types.GlobalState{
			State: waljs.WALState{LSN: lsn},
		}

		err = validateGlobalState(globalState, parsed)
		assert.NoError(t, err)
	})

	t.Run("OLake cursor ahead of slot returns non-retryable error", func(t *testing.T) {
		// This is the exact scenario that our fix prevents:
		// OLake state has a more recent LSN than the replication slot
		olakeLSN := "0/2000000"
		slotLSN, err := pglogrepl.ParseLSN("0/1000000")
		require.NoError(t, err)

		globalState := &types.GlobalState{
			State: waljs.WALState{LSN: olakeLSN},
		}

		err = validateGlobalState(globalState, slotLSN)
		assert.Error(t, err)
		assert.ErrorIs(t, err, constants.ErrNonRetryable)
		assert.Contains(t, err.Error(), "lsn mismatch")
	})

	t.Run("slot ahead of OLake cursor returns non-retryable error", func(t *testing.T) {
		olakeLSN := "0/1000000"
		slotLSN, err := pglogrepl.ParseLSN("0/2000000")
		require.NoError(t, err)

		globalState := &types.GlobalState{
			State: waljs.WALState{LSN: olakeLSN},
		}

		err = validateGlobalState(globalState, slotLSN)
		assert.Error(t, err)
		assert.ErrorIs(t, err, constants.ErrNonRetryable)
		assert.Contains(t, err.Error(), "lsn mismatch")
	})

	t.Run("empty LSN in state returns non-retryable error", func(t *testing.T) {
		slotLSN, err := pglogrepl.ParseLSN("0/1000000")
		require.NoError(t, err)

		globalState := &types.GlobalState{
			State: waljs.WALState{LSN: ""},
		}

		err = validateGlobalState(globalState, slotLSN)
		assert.Error(t, err)
		assert.ErrorIs(t, err, constants.ErrNonRetryable)
		assert.Contains(t, err.Error(), "lsn is empty")
	})

	t.Run("invalid LSN format in state returns error", func(t *testing.T) {
		slotLSN, err := pglogrepl.ParseLSN("0/1000000")
		require.NoError(t, err)

		globalState := &types.GlobalState{
			State: waljs.WALState{LSN: "not-a-valid-lsn"},
		}

		err = validateGlobalState(globalState, slotLSN)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse stored lsn")
	})
}
