package driver

import (
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncrementalSync(t *testing.T) {
	// Test that incremental sync mode is properly supported
	stream := types.NewStream("test_collection", "test_db")
	stream.WithSyncMode(types.INCREMENTAL)
	stream.WithCursorField("created_at", "updated_at", "_id")

	// Verify that incremental sync mode is supported
	assert.True(t, stream.SupportedSyncModes.Exists(types.INCREMENTAL))

	// Verify that cursor fields are available
	assert.True(t, stream.AvailableCursorFields.Exists("created_at"))
	assert.True(t, stream.AvailableCursorFields.Exists("updated_at"))
	assert.True(t, stream.AvailableCursorFields.Exists("_id"))
}

func TestIncrementalConfig(t *testing.T) {
	// Test that config properly handles batch size
	config := &Config{
		BatchSize: 5000,
	}

	err := config.Validate()
	require.NoError(t, err)
	assert.Equal(t, 5000, config.BatchSize)

	// Test default batch size
	config = &Config{}
	err = config.Validate()
	require.NoError(t, err)
	assert.Equal(t, 10000, config.BatchSize)
}

func TestIncrementalInterface(t *testing.T) {
	// Test that MongoDB driver implements DriverInterface interface
	mongo := &Mongo{}

	// This should compile without errors
	var _ abstract.DriverInterface = mongo
}
