package abstract

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for concurrent operations and race conditions

func TestConcurrentDiscovery(t *testing.T) {
	ctx := context.Background()
	var callCount int32
	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"stream1", "stream2", "stream3"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			// Simulate concurrent access
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&callCount, 1)
			return createMockStream(stream, "public", types.FULLREFRESH), nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	streams, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	assert.Len(t, streams, 3)
	assert.Equal(t, int32(3), callCount)
}

func TestConcurrentBackfill(t *testing.T) {
	ctx := context.Background()
	var processedChunks int32
	mockDriver := &MockDriver{
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet(
				types.Chunk{Min: 1, Max: 100},
				types.Chunk{Min: 101, Max: 200},
				types.Chunk{Min: 201, Max: 300},
			), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			atomic.AddInt32(&processedChunks, 1)
			return processFn(ctx, map[string]any{"id": 1})
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
	
	// Wait for concurrent chunk processing to complete
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify chunks were processed
	assert.Equal(t, int32(3), processedChunks)
}

func TestMaxConnectionsEnforcement(t *testing.T) {
	ctx := context.Background()
	maxConns := 2
	var activeConns int32
	var maxActiveConns int32
	var mu sync.Mutex

	mockDriver := &MockDriver{
		maxConnectionsFunc: func() int {
			return maxConns
		},
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"s1", "s2", "s3", "s4", "s5"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			// Track concurrent connections
			current := atomic.AddInt32(&activeConns, 1)
			mu.Lock()
			if current > maxActiveConns {
				maxActiveConns = current
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&activeConns, -1)
			return createMockStream(stream, "public", types.FULLREFRESH), nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	_, err := abstractDriver.Discover(ctx)

	require.NoError(t, err)
	assert.LessOrEqual(t, maxActiveConns, int32(maxConns))
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockDriver := &MockDriver{
		getStreamNamesFunc: func(ctx context.Context) ([]string, error) {
			return []string{"stream1"}, nil
		},
		produceSchemaFunc: func(ctx context.Context, stream string) (*types.Stream, error) {
			// Cancel context during discovery
			cancel()
			time.Sleep(100 * time.Millisecond)
			return createMockStream(stream, "public", types.FULLREFRESH), nil
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	_, err := abstractDriver.Discover(ctx)

	// Should handle context cancellation gracefully
	if err != nil {
		assert.Contains(t, err.Error(), "context")
	}
}

func TestConcurrentStateAccess(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	
	// Use StreamType state which doesn't trigger global state logging
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createMockStream("users", "public", types.INCREMENTAL)
	configuredStream := &types.ConfiguredStream{Stream: stream}

	// Test concurrent read operations
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Concurrent reads should be safe
			state.GetCursor(configuredStream, "id")
			state.GetChunks(configuredStream)
			state.HasCompletedBackfill(configuredStream)
		}()
	}

	wg.Wait()
	// If we get here without panic or deadlock, the test passes
}

func TestConcurrentChunkProcessing(t *testing.T) {
	ctx := context.Background()
	var processedCount int32
	var mu sync.Mutex
	processedChunks := make(map[string]bool)

	mockDriver := &MockDriver{
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet(
				types.Chunk{Min: 1, Max: 10},
				types.Chunk{Min: 11, Max: 20},
				types.Chunk{Min: 21, Max: 30},
				types.Chunk{Min: 31, Max: 40},
				types.Chunk{Min: 41, Max: 50},
			), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			chunkKey := fmt.Sprintf("%v-%v", chunk.Min, chunk.Max)
			mu.Lock()
			processedChunks[chunkKey] = true
			mu.Unlock()
			atomic.AddInt32(&processedCount, 1)
			time.Sleep(10 * time.Millisecond)
			return processFn(ctx, map[string]any{"id": 1})
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
	
	// Wait for concurrent processing
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify all chunks were processed
	assert.Equal(t, int32(5), processedCount)
	assert.Equal(t, 5, len(processedChunks))
}

func TestConcurrentStreamProcessing(t *testing.T) {
	ctx := context.Background()
	var processedStreams int32
	
	mockDriver := &MockDriver{
		getOrSplitChunksFunc: func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
			return types.NewSet(types.Chunk{Min: 1, Max: 100}), nil
		},
		chunkIteratorFunc: func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
			atomic.AddInt32(&processedStreams, 1)
			return processFn(ctx, map[string]any{"id": 1})
		},
	}

	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	// Create multiple streams
	stream1 := createMockStream("users", "public", types.FULLREFRESH)
	stream2 := createMockStream("orders", "public", types.FULLREFRESH)
	stream3 := createMockStream("products", "public", types.FULLREFRESH)
	
	configuredStream1 := &types.ConfiguredStream{Stream: stream1}
	configuredStream2 := &types.ConfiguredStream{Stream: stream2}
	configuredStream3 := &types.ConfiguredStream{Stream: stream3}
	
	// Create real WriterPool with noop writer
	pool, err := createTestWriterPool(ctx, []string{
		configuredStream1.ID(),
		configuredStream2.ID(),
		configuredStream3.ID(),
	})
	require.NoError(t, err)

	// Process streams concurrently using Read
	err = abstractDriver.Read(ctx, pool, []types.StreamInterface{
		configuredStream1,
		configuredStream2,
		configuredStream3,
	}, nil, nil)

	require.NoError(t, err)
	
	// Wait for all processing to complete
	err = abstractDriver.GlobalCtxGroup.Block()
	require.NoError(t, err)
	
	err = abstractDriver.GlobalConnGroup.Block()
	require.NoError(t, err)
	
	// Verify all streams were processed
	assert.Equal(t, int32(3), atomic.LoadInt32(&processedStreams))
}
