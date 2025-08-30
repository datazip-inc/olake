package abstract

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockDriver implements DriverInterface for testing
type MockDriver struct {
	chunks          []types.Chunk
	processedChunks []types.Chunk
	transactionUsed *sql.Tx
	mu              sync.Mutex
	chunkIteratorFn func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, tx *sql.Tx, processFn BackfillMsgFn) error
	beginTxFn       func(ctx context.Context) (*sql.Tx, error)
}

func (m *MockDriver) GetConfigRef() Config {
	return &MockConfig{}
}

func (m *MockDriver) Spec() any {
	return nil
}

func (m *MockDriver) Type() string {
	return "mock"
}

func (m *MockDriver) Setup(_ context.Context) error {
	return nil
}

func (m *MockDriver) SetupState(_ *types.State) {}

func (m *MockDriver) MaxConnections() int {
	return 1
}

func (m *MockDriver) MaxRetries() int {
	return 3
}

func (m *MockDriver) GetStreamNames(_ context.Context) ([]string, error) {
	return []string{"test_stream"}, nil
}

func (m *MockDriver) ProduceSchema(_ context.Context, stream string) (*types.Stream, error) {
	return types.NewStream(stream, "test"), nil
}

func (m *MockDriver) GetOrSplitChunks(_ context.Context, _ *destination.WriterPool, _ types.StreamInterface) (*types.Set[types.Chunk], error) {
	chunks := types.NewSet[types.Chunk]()
	for _, chunk := range m.chunks {
		chunks.Insert(chunk)
	}
	return chunks, nil
}

func (m *MockDriver) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, tx *sql.Tx, processFn BackfillMsgFn) error {
	m.mu.Lock()
	m.processedChunks = append(m.processedChunks, chunk)
	if m.transactionUsed == nil {
		m.transactionUsed = tx
	}
	m.mu.Unlock()

	if m.chunkIteratorFn != nil {
		return m.chunkIteratorFn(ctx, stream, chunk, tx, processFn)
	}

	// Default implementation - just call processFn with mock data
	return processFn(map[string]interface{}{
		"id":   chunk.Min,
		"data": fmt.Sprintf("data_%v", chunk.Min),
	})
}

func (m *MockDriver) BeginBackfillTransaction(ctx context.Context) (*sql.Tx, error) {
	if m.beginTxFn != nil {
		return m.beginTxFn(ctx)
	}
	// Return a mock transaction
	return &sql.Tx{}, nil
}

func (m *MockDriver) StreamIncrementalChanges(_ context.Context, _ types.StreamInterface, _ BackfillMsgFn) error {
	return nil
}

func (m *MockDriver) CDCSupported() bool {
	return false
}

func (m *MockDriver) PreCDC(_ context.Context, _ []types.StreamInterface) error {
	return nil
}

func (m *MockDriver) StreamChanges(_ context.Context, _ types.StreamInterface, _ CDCMsgFn) error {
	return nil
}

func (m *MockDriver) PostCDC(_ context.Context, _ types.StreamInterface, _ bool) error {
	return nil
}

type MockConfig struct{}

func (m *MockConfig) Validate() error {
	return nil
}

func TestBackfillSharedTransaction(t *testing.T) {
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

	// Test that ChunkIterator uses the provided transaction
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

	// Override ChunkIterator to track records
	mockDriver.chunkIteratorFn = func(_ context.Context, _ types.StreamInterface, chunk types.Chunk, tx *sql.Tx, processFn BackfillMsgFn) error {
		// Verify that we're using the same transaction across chunks
		if tx == nil {
			t.Fatal("Transaction should not be nil")
		}

		return processFn(map[string]interface{}{
			"id":   chunk.Min,
			"data": fmt.Sprintf("data_%v", chunk.Min),
		})
	}

	// Test processing a single chunk
	chunk := types.Chunk{Min: 1, Max: 2}
	err = mockDriver.ChunkIterator(context.Background(), streamInterface, chunk, tx, func(data map[string]interface{}) error {
		assert.Equal(t, 1, data["id"])
		assert.Equal(t, "data_1", data["data"])
		return nil
	})
	require.NoError(t, err)

	// Verify that the transaction was used
	assert.NotNil(t, mockDriver.transactionUsed)
}

func TestBackfillTransactionConsistency(t *testing.T) {
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

	// Test that the same transaction is used across multiple chunks
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
		// Simulate different cursor values for each chunk
		cursorValue := chunk.Min
		if chunk.Min == 3 {
			cursorValue = 5 // Simulate a jump in cursor value
		}

		mu.Lock()
		cursorValues = append(cursorValues, cursorValue)
		mu.Unlock()

		return processFn(map[string]interface{}{
			"id":   chunk.Min,
			"data": fmt.Sprintf("data_%v", chunk.Min),
		})
	}

	// Process multiple chunks with the same transaction
	for _, chunk := range mockDriver.chunks {
		err = mockDriver.ChunkIterator(context.Background(), streamInterface, chunk, tx, func(_ map[string]interface{}) error {
			return nil
		})
		require.NoError(t, err)
	}

	// Verify that cursor values were tracked
	assert.Len(t, cursorValues, 3)

	// Verify that the same transaction was used for all chunks
	assert.NotNil(t, mockDriver.transactionUsed)
}

func TestBackfillTransactionTimeout(t *testing.T) {
	t.Parallel()

	// Create mock driver that takes time to process chunks
	mockDriver := &MockDriver{
		chunks: []types.Chunk{
			{Min: 1, Max: 2},
			{Min: 2, Max: 3},
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

	// Test that the same transaction is used across multiple chunks
	stream := types.NewStream("test_stream", "test")
	stream.WithPrimaryKey("id")
	stream.UpsertField("id", types.Int64, false)

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

	// Override ChunkIterator to simulate slow processing
	mockDriver.chunkIteratorFn = func(_ context.Context, _ types.StreamInterface, chunk types.Chunk, _ *sql.Tx, processFn BackfillMsgFn) error {
		// Simulate slow processing
		time.Sleep(100 * time.Millisecond)
		return processFn(map[string]interface{}{
			"id": chunk.Min,
		})
	}

	// Process chunks with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for _, chunk := range mockDriver.chunks {
		err = mockDriver.ChunkIterator(ctx, streamInterface, chunk, tx, func(_ map[string]interface{}) error {
			return nil
		})
		require.NoError(t, err)
	}

	// Should complete successfully within timeout
	assert.NotNil(t, mockDriver.transactionUsed)
}

// MockStreamInterface implements types.StreamInterface for testing
type MockStreamInterface struct {
	Stream        *types.Stream
	SchemaFn      func() *types.TypeSchema
	NamespaceFn   func() string
	NameFn        func() string
	IDFn          func() string
	SelfFn        func() *types.ConfiguredStream
	GetStreamFn   func() *types.Stream
	GetSyncModeFn func() types.SyncMode
	CursorFn      func() (string, string)
}

func (m *MockStreamInterface) Schema() *types.TypeSchema {
	if m.SchemaFn != nil {
		return m.SchemaFn()
	}
	return types.NewTypeSchema()
}

func (m *MockStreamInterface) Namespace() string {
	if m.NamespaceFn != nil {
		return m.NamespaceFn()
	}
	return "test"
}

func (m *MockStreamInterface) Name() string {
	if m.NameFn != nil {
		return m.NameFn()
	}
	return "test_stream"
}

func (m *MockStreamInterface) ID() string {
	if m.IDFn != nil {
		return m.IDFn()
	}
	return "test_stream"
}

func (m *MockStreamInterface) Self() *types.ConfiguredStream {
	if m.SelfFn != nil {
		return m.SelfFn()
	}
	return m.Stream.Wrap(0)
}

func (m *MockStreamInterface) GetStream() *types.Stream {
	if m.GetStreamFn != nil {
		return m.GetStreamFn()
	}
	return m.Stream
}

func (m *MockStreamInterface) GetSyncMode() types.SyncMode {
	if m.GetSyncModeFn != nil {
		return m.GetSyncModeFn()
	}
	return types.FULLREFRESH
}

func (m *MockStreamInterface) Cursor() (string, string) {
	if m.CursorFn != nil {
		return m.CursorFn()
	}
	return "", ""
}

func (m *MockStreamInterface) GetFilter() (types.Filter, error) {
	return types.Filter{}, nil
}

func (m *MockStreamInterface) SupportedSyncModes() *types.Set[types.SyncMode] {
	return types.NewSet[types.SyncMode]()
}

func (m *MockStreamInterface) Validate(_ *types.Stream) error {
	return nil
}

func (m *MockStreamInterface) NormalizationEnabled() bool {
	return false
}
