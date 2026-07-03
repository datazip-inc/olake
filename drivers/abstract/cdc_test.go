package abstract

import (
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestCDCThreadHashes(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "uses stable initial hash without source state",
			run: func(t *testing.T) {
				stream := types.NewStream("users", "public", nil).Wrap(0)
				driver := &AbstractDriver{state: &types.State{RWMutex: &sync.RWMutex{}}}

				firstHash := driver.cdcThreadHashes([]types.StreamInterface{stream})[stream.ID()]
				secondHash := driver.cdcThreadHashes([]types.StreamInterface{stream})[stream.ID()]

				require.NotEmpty(t, firstHash)
				require.Equal(t, firstHash, secondHash)
				require.Equal(t, generateThreadID(stream.ID(), firstHash), generateThreadID(stream.ID(), secondHash))
			},
		},
		{
			name: "uses global source state for selected streams",
			run: func(t *testing.T) {
				users := types.NewStream("users", "public", nil).Wrap(0)
				orders := types.NewStream("orders", "public", nil).Wrap(0)
				globalState := map[string]any{"lsn": "1/1"}
				driver := &AbstractDriver{
					state: &types.State{
						RWMutex: &sync.RWMutex{},
						Global:  &types.GlobalState{State: globalState},
					},
				}

				hashes := driver.cdcThreadHashes([]types.StreamInterface{users, orders})
				expectedHash := cdcThreadHash(globalState)

				require.Equal(t, expectedHash, hashes[users.ID()])
				require.Equal(t, expectedHash, hashes[orders.ID()])
			},
		},
		{
			name: "uses stream state without backfill chunks",
			run: func(t *testing.T) {
				stream := types.NewStream("users", "public", nil).Wrap(0)
				streamState := &types.StreamState{
					Stream:    stream.Name(),
					Namespace: stream.Namespace(),
					State:     sync.Map{},
				}
				streamState.State.Store("resume_token", "token-1")
				streamState.State.Store(types.ChunksKey, []types.Chunk{{Min: 1, Max: 10}})
				driver := &AbstractDriver{
					state: &types.State{
						RWMutex: &sync.RWMutex{},
						Streams: []*types.StreamState{streamState},
					},
				}

				firstHash := driver.cdcThreadHashes([]types.StreamInterface{stream})[stream.ID()]
				streamState.State.Store(types.ChunksKey, []types.Chunk{{Min: 11, Max: 20}})
				secondHash := driver.cdcThreadHashes([]types.StreamInterface{stream})[stream.ID()]
				streamState.State.Store("resume_token", "token-2")
				thirdHash := driver.cdcThreadHashes([]types.StreamInterface{stream})[stream.ID()]

				require.Equal(t, firstHash, secondHash)
				require.NotEqual(t, secondHash, thirdHash)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
