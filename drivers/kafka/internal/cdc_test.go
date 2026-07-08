package driver

import (
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

func TestPersistedCDCCheckpoint(t *testing.T) {
	stream := types.NewStream("events", "topics", nil).Wrap(0)

	tests := []struct {
		name     string
		kafka    *Kafka
		expected string
	}{
		{
			name: "uses consumer group from state",
			kafka: &Kafka{
				state: &types.State{
					RWMutex: &sync.RWMutex{},
					Global: &types.GlobalState{State: map[string]any{
						consumerGroupIDKey: "olake-group-state",
					}},
				},
				config: &Config{ConsumerGroupID: "olake-group-config"},
			},
			expected: "olake-group-state",
		},
		{
			name: "uses configured consumer group without state",
			kafka: &Kafka{
				config: &Config{ConsumerGroupID: "olake-group-config"},
			},
			expected: "olake-group-config",
		},
		{
			name:     "returns empty checkpoint without state or config",
			kafka:    &Kafka{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.kafka.PersistedCDCCheckpoint(stream))
		})
	}
}
