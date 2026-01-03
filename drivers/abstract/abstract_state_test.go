package abstract

import (
	"context"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for state management functionality

func TestClearState_NilState(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	stream := createMockStream("users", "public", types.FULLREFRESH)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	state, err := abstractDriver.ClearState([]types.StreamInterface{configuredStream})

	require.NoError(t, err)
	assert.NotNil(t, state)
	assert.Nil(t, state.Global)
	assert.Len(t, state.Streams, 0)
}

func TestClearState_GlobalState(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("public.users", "public.orders"),
		},
	}
	abstractDriver.SetupState(state)

	stream := createMockStream("users", "public", types.FULLREFRESH)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	clearedState, err := abstractDriver.ClearState([]types.StreamInterface{configuredStream})

	require.NoError(t, err)
	assert.False(t, clearedState.Global.Streams.Exists("public.users"))
	assert.True(t, clearedState.Global.Streams.Exists("public.orders"))
}

func TestClearState_StreamState(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	stream := createMockStream("users", "public", types.INCREMENTAL)
	configuredStream := &types.ConfiguredStream{Stream: stream}
	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.StreamType,
		Streams: []*types.StreamState{
			{
				Stream:    "users",
				Namespace: "public",
				State:     sync.Map{},
			},
		},
	}
	state.Streams[0].HoldsValue.Store(true)
	abstractDriver.SetupState(state)

	clearedState, err := abstractDriver.ClearState([]types.StreamInterface{configuredStream})

	require.NoError(t, err)
	assert.False(t, clearedState.Streams[0].HoldsValue.Load())
}

func TestClearState_MultipleStreams(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)

	state := &types.State{
		RWMutex: &sync.RWMutex{},
		Type:    types.GlobalType,
		Global: &types.GlobalState{
			Streams: types.NewSet("public.users", "public.orders", "public.products"),
		},
	}
	abstractDriver.SetupState(state)

	streams := []types.StreamInterface{
		&types.ConfiguredStream{Stream: createMockStream("users", "public", types.FULLREFRESH)},
		&types.ConfiguredStream{Stream: createMockStream("orders", "public", types.FULLREFRESH)},
	}

	clearedState, err := abstractDriver.ClearState(streams)

	require.NoError(t, err)
	assert.False(t, clearedState.Global.Streams.Exists("public.users"))
	assert.False(t, clearedState.Global.Streams.Exists("public.orders"))
	assert.True(t, clearedState.Global.Streams.Exists("public.products"))
}

func TestStateRaceConditions(t *testing.T) {
	ctx := context.Background()
	mockDriver := &MockDriver{}
	abstractDriver := NewAbstractDriver(ctx, mockDriver)
	
	// Use StreamType state which doesn't trigger global state logging
	state := &types.State{RWMutex: &sync.RWMutex{}, Type: types.StreamType}
	abstractDriver.SetupState(state)

	stream := createMockStream("users", "public", types.INCREMENTAL)
	configuredStream := &types.ConfiguredStream{Stream: stream}

	// Run concurrent operations on state
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			// These operations should not cause race conditions
			state.GetCursor(configuredStream, "updated_at")
			// Note: SetCursor may trigger logging, so we avoid it in this test
		}(i)
	}

	wg.Wait()
	// If we get here without panic or deadlock, the test passes
}
