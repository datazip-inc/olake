package driver

import (
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestPersistedCDCCheckpoint(t *testing.T) {
	stream := types.NewStream("users", "public", nil).Wrap(0)
	mysql := &MySQL{
		state: &types.State{
			RWMutex: &sync.RWMutex{},
			Global: &types.GlobalState{State: map[string]any{
				"server_id": float64(1001),
				"state": map[string]any{
					"position": map[string]any{
						"name": "mysql-bin.000001",
						"pos":  float64(154),
					},
				},
			}},
		},
	}

	require.Equal(t, "mysql-bin.000001-154", mysql.PersistedCDCCheckpoint(stream))
}
