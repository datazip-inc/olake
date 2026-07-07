package driver

import (
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestPersistedCDCCheckpoint(t *testing.T) {
	stream := types.NewStream("users", "dbo", nil).Wrap(0)
	streamState := &types.StreamState{
		Stream:    stream.Name(),
		Namespace: stream.Namespace(),
		State:     sync.Map{},
	}
	streamState.State.Store(cdcCursorKey, "0000002a0000005d0003")
	mssql := &MSSQL{
		state: &types.State{
			RWMutex: &sync.RWMutex{},
			Streams: []*types.StreamState{
				streamState,
			},
		},
	}

	require.Equal(t, "0000002a0000005d0003", mssql.PersistedCDCCheckpoint(stream))
}
