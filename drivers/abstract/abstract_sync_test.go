package abstract

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for Read operations

func TestRead_BackfillOnly(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet(types.Chunk{Min: 1, Max: 100}), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			return processFn(ctx, map[string]any{"id": 1, "name": "test"})
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createConfiguredStream("users", "public", types.FULLREFRESH)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream.ID()})
	require.NoError(t, err)
	
	err = abstractDriver.Read(ctx, pool, []types.StreamInterface{stream}, nil, nil)

	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
}

func TestRead_IncrementalOnly(t *testing.T) {
	t.Skip("Skipping: Requires integration testing - errgroup goroutines panic and cannot be caught in unit tests")
	// These tests spawn goroutines via errgroup that call pool.NewWriter() asynchronously.
	// Panics in errgroup goroutines kill the entire process and cannot be recovered.
	// These operations require integration tests with real database connections.
}

func TestRead_CDCOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var cdcCalled int32
	mockDriver := &MockDriver{
		cdcSupportedFunc: func() bool {
			return true
		},
		preCDCFunc: func(ctx context.Context, streams []types.StreamInterface) error {
			return nil
		},
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			// Return empty chunks for CDC streams
			return types.NewSet[types.Chunk](), nil
		},
		streamChangesFunc: func(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error {
			atomic.StoreInt32(&cdcCalled, 1)
			// Call the callback once with test data
			return processFn(ctx, CDCChange{
				Stream:    stream,
				Timestamp: time.Now(),
				Kind:      "insert",
				Data:      map[string]interface{}{"id": 1},
			})
		},
		postCDCFunc: func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("public.users"),
		},
	}
	abstractDriver.SetupState(state)

	stream := createConfiguredStream("users", "public", types.CDC)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream.ID()})
	require.NoError(t, err)
	
	// Use Read which handles the full CDC flow
	err = abstractDriver.Read(ctx, pool, nil, []types.StreamInterface{stream}, nil)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify CDC was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&cdcCalled))
}

func TestRead_CDCNotSupported(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		cdcSupportedFunc: func() bool {
			return false
		},
		typeFunc: func() string {
			return "postgres"
		},
	}

	_ = &MockWriterPool{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	stream := createConfiguredStream("users", "public", types.CDC)
	err := abstractDriver.Read(ctx, nil, nil, []types.StreamInterface{stream}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cdc configuration not provided")
}

func TestRead_MixedModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var backfillCalled, cdcCalled, incrementalCalled int32
	
	mockDriver := &MockDriver{
		cdcSupportedFunc: func() bool {
			return true
		},
		preCDCFunc: func(ctx context.Context, streams []types.StreamInterface) error {
			return nil
		},
		fetchMaxCursorValuesFunc: func(ctx context.Context, stream types.StreamInterface) (any, any, error) {
			return time.Now().Unix(), nil, nil
		},
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			// Return chunks for backfill streams, empty for CDC and incremental
			if stream.GetSyncMode() == types.FULLREFRESH {
				return types.NewSet(types.Chunk{Min: 1, Max: 100}), nil
			}
			return types.NewSet[types.Chunk](), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			atomic.StoreInt32(&backfillCalled, 1)
			return processFn(ctx, map[string]any{"id": 1})
		},
		streamIncrementalChangesFunc: func(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error {
			atomic.StoreInt32(&incrementalCalled, 1)
			return cb(ctx, map[string]any{"id": 1, "updated_at": time.Now().Unix()})
		},
		streamChangesFunc: func(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error {
			atomic.StoreInt32(&cdcCalled, 1)
			return processFn(ctx, CDCChange{
				Stream:    stream,
				Timestamp: time.Now(),
				Kind:      "insert",
				Data:      map[string]interface{}{"id": 1},
			})
		},
		postCDCFunc: func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("public.cdc_stream"),
		},
	}
	abstractDriver.SetupState(state)

	backfillStream := createConfiguredStream("backfill_stream", "public", types.FULLREFRESH)
	cdcStream := createConfiguredStream("cdc_stream", "public", types.CDC)
	incrementalStream := createConfiguredStream("incremental_stream", "public", types.INCREMENTAL)

	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{backfillStream.ID(), cdcStream.ID(), incrementalStream.ID()})
	require.NoError(t, err)

	err = abstractDriver.Read(ctx, pool,
		[]types.StreamInterface{backfillStream},
		[]types.StreamInterface{cdcStream},
		[]types.StreamInterface{incrementalStream})

	require.NoError(t, err)
	
	// Wait for all processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	err = abstractDriver.GlobalCtxGroup.Block()
	require.NoError(t, err)
	
	// Verify all modes were called
	assert.Equal(t, int32(1), atomic.LoadInt32(&backfillCalled))
	assert.Equal(t, int32(1), atomic.LoadInt32(&cdcCalled))
	assert.Equal(t, int32(1), atomic.LoadInt32(&incrementalCalled))
}

// Tests for Backfill

func TestBackfill_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var recordCount int32
	var chunkIteratorCalled int32
	mockDriver := &MockDriver{
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			// Return chunks for FULLREFRESH mode
			return types.NewSet(types.Chunk{Min: 1, Max: 100}), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			atomic.StoreInt32(&chunkIteratorCalled, 1)
			for i := 0; i < 5; i++ {
				if err := processFn(ctx, map[string]any{"id": i, "name": fmt.Sprintf("user%d", i)}); err != nil {
					return err
				}
				atomic.AddInt32(&recordCount, 1)
			}
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createMockStream("users", "public", types.FULLREFRESH)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{configuredStream.ID()})
	require.NoError(t, err)

	err = abstractDriver.Backfill(ctx, nil, pool, configuredStream)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify chunk iterator was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&chunkIteratorCalled), "ChunkIterator should have been called")
	assert.Equal(t, int32(5), atomic.LoadInt32(&recordCount))
}

func TestBackfill_EmptyChunks(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet[types.Chunk](), nil
		},
	}

	_ = &MockWriterPool{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createMockStream("users", "public", types.FULLREFRESH)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	backfilledChan := make(chan string, 1)

	err := abstractDriver.Backfill(ctx, backfilledChan, nil, configuredStream)

	require.NoError(t, err)
	select {
	case streamID := <-backfilledChan:
		assert.Equal(t, configuredStream.ID(), streamID)
	case <-time.After(time.Second):
		t.Fatal("Expected stream ID in backfilled channel")
	}
}

func TestBackfill_ChunkIteratorError(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{
		maxRetriesFunc: func() int {
			return 1
		},
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet(types.Chunk{Min: 1, Max: 100}), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			return errors.New("iterator failed")
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createMockStream("users", "public", types.FULLREFRESH)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{configuredStream.ID()})
	require.NoError(t, err)

	err = abstractDriver.Backfill(ctx, nil, pool, configuredStream)

	// Backfill returns nil even on chunk errors (errors are logged but not propagated)
	require.NoError(t, err)
	
	// Wait for processing
	err = abstractDriver.GlobalConnGroup.Block()
	// The error should be captured here
	assert.Error(t, err)
}

// Tests for Incremental

func TestIncremental_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var incrementalCalled int32
	mockDriver := &MockDriver{
		fetchMaxCursorValuesFunc: func(ctx context.Context, stream types.StreamInterface) (any, any, error) {
			return time.Now().Unix(), nil, nil
		},
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			// Return empty chunks to skip backfill
			return types.NewSet[types.Chunk](), nil
		},
		streamIncrementalChangesFunc: func(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error {
			atomic.StoreInt32(&incrementalCalled, 1)
			return cb(ctx, map[string]any{"id": 1, "updated_at": time.Now().Unix()})
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createConfiguredStream("users", "public", types.INCREMENTAL)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream.ID()})
	require.NoError(t, err)
	
	err = abstractDriver.Incremental(ctx, pool, stream)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify incremental was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&incrementalCalled))
}

func TestIncremental_CompletedBackfill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var incrementalCalled int32
	mockDriver := &MockDriver{
		streamIncrementalChangesFunc: func(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error {
			atomic.StoreInt32(&incrementalCalled, 1)
			return cb(ctx, map[string]any{"id": 1, "updated_at": time.Now().Unix()})
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	stream := createMockStream("users", "public", types.INCREMENTAL)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet(configuredStream.ID()),
		},
	}
	abstractDriver.SetupState(state)
	// Set cursor to indicate backfill is completed
	state.SetCursor(configuredStream, "updated_at", time.Now().Unix())

	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{configuredStream.ID()})
	require.NoError(t, err)

	err = abstractDriver.Incremental(ctx, pool, configuredStream)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify incremental was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&incrementalCalled))
}

// Tests for CDC

func TestRunChangeStream_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var cdcCalled int32
	mockDriver := &MockDriver{
		cdcSupportedFunc: func() bool {
			return true
		},
		preCDCFunc: func(ctx context.Context, streams []types.StreamInterface) error {
			return nil
		},
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet[types.Chunk](), nil
		},
		streamChangesFunc: func(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error {
			atomic.StoreInt32(&cdcCalled, 1)
			return processFn(ctx, CDCChange{
				Stream:    stream,
				Timestamp: time.Now(),
				Kind:      "insert",
				Data:      map[string]interface{}{"id": 1, "name": "test"},
			})
		},
		postCDCFunc: func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
			assert.True(t, success)
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("public.users"),
		},
	}
	abstractDriver.SetupState(state)

	stream := createConfiguredStream("users", "public", types.CDC)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream.ID()})
	require.NoError(t, err)
	
	err = abstractDriver.RunChangeStream(ctx, pool, stream)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify CDC was called
	assert.Equal(t, int32(1), atomic.LoadInt32(&cdcCalled))
}

func TestRunChangeStream_StrictCDC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var changesCalled int32
	mockDriver := &MockDriver{
		preCDCFunc: func(ctx context.Context, streams []types.StreamInterface) error {
			return nil
		},
		streamChangesFunc: func(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error {
			atomic.StoreInt32(&changesCalled, 1)
			return processFn(ctx, CDCChange{
				Stream:    stream,
				Timestamp: time.Now(),
				Kind:      "insert",
				Data:      map[string]interface{}{"id": 1},
			})
		},
		postCDCFunc: func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createConfiguredStream("users", "public", types.STRICTCDC)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream.ID()})
	require.NoError(t, err)
	
	err = abstractDriver.RunChangeStream(ctx, pool, stream)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	assert.Equal(t, int32(1), atomic.LoadInt32(&changesCalled))
}

func TestRunChangeStream_MongoDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var streamChangesCallCount int32
	mockDriver := &MockDriver{
		typeFunc: func() string {
			return string(constants.MongoDB)
		},
		preCDCFunc: func(ctx context.Context, streams []types.StreamInterface) error {
			return nil
		},
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet[types.Chunk](), nil
		},
		streamChangesFunc: func(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error {
			atomic.AddInt32(&streamChangesCallCount, 1)
			return processFn(ctx, CDCChange{
				Stream:    stream,
				Timestamp: time.Now(),
				Kind:      "insert",
				Data:      map[string]interface{}{"id": 1},
			})
		},
		postCDCFunc: func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
			return nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("db.collection1", "db.collection2"),
		},
	}
	abstractDriver.SetupState(state)

	stream1 := createConfiguredStream("collection1", "db", types.CDC)
	stream2 := createConfiguredStream("collection2", "db", types.CDC)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream1.ID(), stream2.ID()})
	require.NoError(t, err)
	
	err = abstractDriver.RunChangeStream(ctx, pool, stream1, stream2)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	assert.Equal(t, int32(2), atomic.LoadInt32(&streamChangesCallCount))
}

func TestRunChangeStream_KafkaDriver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	var partitionChangesCalled int32
	mockKafkaDriver := &MockKafkaDriver{
		MockDriver: MockDriver{
			preCDCFunc: func(ctx context.Context, streams []types.StreamInterface) error {
				return nil
			},
			getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
				return types.NewSet[types.Chunk](), nil
			},
			postCDCFunc: func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
				return nil
			},
		},
		getReaderIDsFunc: func() []string {
			return []string{"reader1", "reader2"}
		},
		partitionStreamChangesFunc: func(ctx context.Context, readerID string, processFn CDCMsgFn) error {
			atomic.AddInt32(&partitionChangesCalled, 1)
			stream := createConfiguredStream("topic1", "kafka", types.CDC)
			return processFn(ctx, CDCChange{
				Stream:    stream,
				Timestamp: time.Now(),
				Kind:      "insert",
				Data:      map[string]interface{}{"id": 1},
			})
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockKafkaDriver)
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("kafka.topic1"),
		},
	}
	abstractDriver.SetupState(state)

	stream := createConfiguredStream("topic1", "kafka", types.CDC)
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{stream.ID()})
	require.NoError(t, err)
	
	err = abstractDriver.RunChangeStream(ctx, pool, stream)
	require.NoError(t, err)
	
	// Wait for processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify partition changes were called for both readers
	assert.Equal(t, int32(2), atomic.LoadInt32(&partitionChangesCalled))
}

// Tests for utility functions

func TestRetryOnBackoff_Success(t *testing.T) {
	attempts := 0
	err := RetryOnBackoff(3, 10*time.Millisecond, func() error {
		attempts++
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetryOnBackoff_AllFail(t *testing.T) {
	attempts := 0
	err := RetryOnBackoff(3, 10*time.Millisecond, func() error {
		attempts++
		return errors.New("persistent error")
	})

	require.Error(t, err)
	assert.Equal(t, 3, attempts)
	assert.Contains(t, err.Error(), "persistent error")
}

func TestRetryOnBackoff_EventualSuccess(t *testing.T) {
	attempts := 0
	err := RetryOnBackoff(5, 10*time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestReformatCursorValue_EmptyCursor(t *testing.T) {
	stream := createConfiguredStream("users", "public", types.INCREMENTAL)
	value, err := ReformatCursorValue("", "test", stream)

	require.NoError(t, err)
	assert.Equal(t, "test", value)
}

func TestReformatCursorValue_ValidCursor(t *testing.T) {
	stream := createConfiguredStream("users", "public", types.INCREMENTAL)
	value, err := ReformatCursorValue("updated_at", int64(1234567890), stream)

	require.NoError(t, err)
	assert.NotNil(t, value)
}

func TestIsParallelChangeStream(t *testing.T) {
	testCases := []struct {
		driverType string
		expected   bool
	}{
		{string(constants.MongoDB), true},
		{"postgres", false},
		{"mysql", false},
		{"oracle", false},
		{"kafka", false},
	}

	for _, tc := range testCases {
		t.Run(tc.driverType, func(t *testing.T) {
			result := isParallelChangeStream(tc.driverType)
			assert.Equal(t, tc.expected, result)
		})
	}
}
