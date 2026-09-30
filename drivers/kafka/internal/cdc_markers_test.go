package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncCommittedOffsetsMissingPartition: partition metadata is rebuilt when a new process starts,
// so an assigned partition missing from it stops without retries (ErrRetryable) but may recover
// on restart, and must not exit 3.
func TestSyncCommittedOffsetsMissingPartition(t *testing.T) {
	k := &Kafka{readerManager: kafkapkg.NewReaderManager(kafkapkg.ReaderConfig{})}

	recovered, err := k.syncCommittedOffsetsWithMetadata(context.Background(), 0, nil, map[string]any{},
		[]types.PartitionKey{{Topic: "orders", Partition: 0}})

	require.Error(t, err)
	assert.False(t, recovered)
	assert.Contains(t, err.Error(), "orders:0 missing from partition metadata")
	assert.True(t, constants.IsRetryable(err), "stops in-process retries")
	assert.False(t, constants.IsNonRetryable(err), "must not exit 3")
}

// TestSyncCommittedOffsetsNoPartitions: with nothing assigned there is nothing to check.
func TestSyncCommittedOffsetsNoPartitions(t *testing.T) {
	k := &Kafka{readerManager: kafkapkg.NewReaderManager(kafkapkg.ReaderConfig{})}

	recovered, err := k.syncCommittedOffsetsWithMetadata(context.Background(), 0, nil, map[string]any{}, nil)

	assert.NoError(t, err)
	assert.False(t, recovered)
}
