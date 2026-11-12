package driver

import (
	"context"
	"testing"

	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncCommittedOffsetsWithMetadata: partition metadata is rebuilt when a new process starts, so an
// assigned partition missing from it stops without retries (ErrRestartRequired) but may recover on
// restart, and must not exit 3. With nothing assigned there is nothing to check.
func TestSyncCommittedOffsetsWithMetadata(t *testing.T) {
	testCases := []struct {
		name        string
		partitions  []types.PartitionKey
		expectedErr string // empty means no error
	}{
		{name: "assigned partition missing from metadata is ErrRestartRequired", partitions: []types.PartitionKey{{Topic: "orders", Partition: 0}}, expectedErr: "orders:0 missing from partition metadata"},
		{name: "no assigned partitions has nothing to check", partitions: nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			k := &Kafka{readerManager: kafkapkg.NewReaderManager(kafkapkg.ReaderConfig{})}

			recovered, err := k.syncCommittedOffsetsWithMetadata(context.Background(), 0, nil, map[string]any{}, tc.partitions)

			assert.False(t, recovered)
			if tc.expectedErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)
			assert.True(t, utils.IsRestartRequired(err), "stops in-process retries")
			assert.False(t, utils.IsManualIntervention(err), "must not exit 3")
		})
	}
}
