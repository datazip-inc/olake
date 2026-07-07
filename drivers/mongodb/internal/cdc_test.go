package driver

import (
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestPersistedCDCCheckpoint(t *testing.T) {
	stream := types.NewStream("users", "public", nil).Wrap(0)
	streamState := &types.StreamState{
		Stream:    stream.Name(),
		Namespace: stream.Namespace(),
		State:     sync.Map{},
	}
	streamState.State.Store(cdcCursorField, "resume-token-1")
	mongo := &Mongo{
		state: &types.State{
			RWMutex: &sync.RWMutex{},
			Streams: []*types.StreamState{
				streamState,
			},
		},
	}

	require.Equal(t, "resume-token-1", mongo.PersistedCDCCheckpoint(stream))
}
