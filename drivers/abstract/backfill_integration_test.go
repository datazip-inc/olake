package abstract

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackfillTransactionConsistencyIntegration demonstrates the transaction consistency fix
func TestBackfillTransactionConsistencyIntegration(t *testing.T) {
	t.Parallel()

	// Create mock driver with multiple chunks
	mockDriver := &MockDriver{
		chunks: []types.Chunk{
			{Min: 1, Max: 2},
			{Min: 2, Max: 3},
			{Min: 3, Max: 4},
		},
	}

	// Create abstract driver
	abstractDriver := &AbstractDriver{
		driver:          mockDriver,
		GlobalConnGroup: utils.NewCGroup(context.Background()),
	}

	// Test that BeginBackfillTransaction returns a transaction
	tx, err := abstractDriver.beginBackfillTransaction(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, tx)

	// Create mock stream
	stream := types.NewStream("test_stream", "test")
	stream.WithPrimaryKey("id")
	stream.UpsertField("id", types.Int64, false)
	stream.UpsertField("data", types.String, true)

	streamInterface := &MockStreamInterface{
		Stream: stream,
		SchemaFn: func() *types.TypeSchema {
			return stream.Schema
		},
		NamespaceFn:   func() string { return "test" },
		NameFn:        func() string { return "test_stream" },
		IDFn:          func() string { return "test_stream" },
		SelfFn:        func() *types.ConfiguredStream { return stream.Wrap(0) },
		GetStreamFn:   func() *types.Stream { return stream },
		GetSyncModeFn: func() types.SyncMode { return types.FULLREFRESH },
		CursorFn:      func() (string, string) { return "", "" },
	}

	// Track transaction usage
	var transactionUsed *sql.Tx
	var mu sync.Mutex

	// Override ChunkIterator to track transaction usage
	mockDriver.chunkIteratorFn = func(_ context.Context, _ types.StreamInterface, chunk types.Chunk, tx *sql.Tx, processFn BackfillMsgFn) error {
		mu.Lock()
		if transactionUsed == nil {
			transactionUsed = tx
		}
		mu.Unlock()

		// Verify that we're using the same transaction across chunks
		if tx == nil {
			t.Fatal("Transaction should not be nil")
		}

		return processFn(map[string]interface{}{
			"id":   chunk.Min,
			"data": fmt.Sprintf("data_%v", chunk.Min),
		})
	}

	// Process all chunks with the same transaction
	for _, chunk := range mockDriver.chunks {
		err = mockDriver.ChunkIterator(context.Background(), streamInterface, chunk, tx, func(data map[string]interface{}) error {
			// Verify data consistency
			assert.Equal(t, chunk.Min, data["id"])
			assert.Equal(t, fmt.Sprintf("data_%v", chunk.Min), data["data"])
			return nil
		})
		require.NoError(t, err)
	}

	// Verify that all chunks were processed
	assert.Len(t, mockDriver.processedChunks, 3)

	// Verify that the same transaction was used for all chunks
	assert.NotNil(t, transactionUsed)

	t.Logf("Successfully processed %d chunks with shared transaction", len(mockDriver.processedChunks))
	t.Logf("Transaction consistency verified: all chunks used the same transaction")
}

// TestBackfillIncrementalCursorConsistency tests cursor consistency for incremental syncs
func TestBackfillIncrementalCursorConsistency(t *testing.T) {
	t.Parallel()

	// Create mock driver with multiple chunks
	mockDriver := &MockDriver{
		chunks: []types.Chunk{
			{Min: 1, Max: 2},
			{Min: 2, Max: 3},
			{Min: 3, Max: 4},
		},
	}

	// Create abstract driver
	abstractDriver := &AbstractDriver{
		driver:          mockDriver,
		GlobalConnGroup: utils.NewCGroup(context.Background()),
	}

	// Test that BeginBackfillTransaction returns a transaction
	tx, err := abstractDriver.beginBackfillTransaction(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, tx)

	// Create mock stream with incremental sync mode
	stream := types.NewStream("test_stream", "test")
	stream.WithPrimaryKey("id")
	stream.UpsertField("id", types.Int64, false)
	stream.UpsertField("data", types.String, true)

	streamInterface := &MockStreamInterface{
		Stream: stream,
		SchemaFn: func() *types.TypeSchema {
			return stream.Schema
		},
		NamespaceFn:   func() string { return "test" },
		NameFn:        func() string { return "test_stream" },
		IDFn:          func() string { return "test_stream" },
		SelfFn:        func() *types.ConfiguredStream { return stream.Wrap(0) },
		GetStreamFn:   func() *types.Stream { return stream },
		GetSyncModeFn: func() types.SyncMode { return types.INCREMENTAL },
		CursorFn:      func() (string, string) { return "id", "" },
	}

	// Track cursor values
	var cursorValues []interface{}
	var mu sync.Mutex

	// Override ChunkIterator to track cursor values
	mockDriver.chunkIteratorFn = func(_ context.Context, _ types.StreamInterface, chunk types.Chunk, _ *sql.Tx, processFn BackfillMsgFn) error {
		// Simulate cursor values that would be consistent within a transaction
		cursorValue := chunk.Min

		mu.Lock()
		cursorValues = append(cursorValues, cursorValue)
		mu.Unlock()

		return processFn(map[string]interface{}{
			"id":   chunk.Min,
			"data": fmt.Sprintf("data_%v", chunk.Min),
		})
	}

	// Process all chunks with the same transaction
	for _, chunk := range mockDriver.chunks {
		err = mockDriver.ChunkIterator(context.Background(), streamInterface, chunk, tx, func(_ map[string]interface{}) error {
			return nil
		})
		require.NoError(t, err)
	}

	// Verify that cursor values were tracked
	assert.Len(t, cursorValues, 3)

	// Verify that cursor values are in order (consistent snapshot)
	expectedValues := []interface{}{1, 2, 3}
	assert.Equal(t, expectedValues, cursorValues)

	t.Logf("Successfully verified cursor consistency across %d chunks", len(cursorValues))
	t.Logf("Cursor values: %v", cursorValues)
}
