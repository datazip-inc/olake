package driver

import (
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestPersistedCDCCheckpoint(t *testing.T) {
	stream := types.NewStream("users", "public", nil).Wrap(0)
	postgres := &Postgres{
		state: &types.State{
			RWMutex: &sync.RWMutex{},
			Global:  &types.GlobalState{State: map[string]any{"lsn": "0/16B6C50"}},
		},
	}

	require.Equal(t, "0-16B6C50", postgres.PersistedCDCCheckpoint(stream))
}
