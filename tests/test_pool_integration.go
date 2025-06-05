package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/types"
)

// MockWriterPool simulates the WriterPool for testing
type MockWriterPool struct {
	totalRecords  atomic.Int64
	recordCount   atomic.Int64
	threadCounter atomic.Int64
}

func NewMockWriterPool() *MockWriterPool {
	return &MockWriterPool{}
}

func (w *MockWriterPool) SyncedRecords() int64 {
	return w.recordCount.Load()
}

func (w *MockWriterPool) AddRecordsToSync(recordCount int64) {
	w.totalRecords.Add(recordCount)
}

func (w *MockWriterPool) GetRecordsToSync() int64 {
	return w.totalRecords.Load()
}

// MockDriver simulates a driver for testing
type MockDriver struct {
	state *types.State
}

func (m *MockDriver) SetupState(state *types.State) {
	m.state = state
}

// MockStream simulates a stream for testing
type MockStream struct {
	name      string
	namespace string
}

func (m *MockStream) Name() string {
	return m.name
}

func (m *MockStream) Namespace() string {
	return m.namespace
}

func (m *MockStream) Self() *types.ConfiguredStream {
	return &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:      m.name,
			Namespace: m.namespace,
		},
	}
}

func main() {
	fmt.Println("=== Testing Pool Integration with State Total Records ===")
	
	// Create stream states with HoldsValue set
	stream1 := &types.StreamState{
		Stream:       "table1",
		Namespace:    "db1",
		TotalRecords: 1000,
	}
	stream1.HoldsValue.Store(true)
	
	stream2 := &types.StreamState{
		Stream:       "table2",
		Namespace:    "db1",
		TotalRecords: 500,
	}
	stream2.HoldsValue.Store(true)
	
	// Create a test state with stream record counts
	state := &types.State{
		Type:    types.StreamType,
		RWMutex: &sync.RWMutex{},
		Streams: []*types.StreamState{stream1, stream2},
	}
	
	// Create mock driver and pool
	driver := &MockDriver{}
	driver.SetupState(state)
	
	pool := NewMockWriterPool()
	
	// Define mock streams for the current sync session
	streams := []MockStream{
		{name: "table1", namespace: "db1"},
		{name: "table2", namespace: "db1"},
		{name: "table3", namespace: "db1"}, // not in state
	}
	
	// Simulate the sync.go logic for restoring total records
	fmt.Println("Initial pool total records:", pool.GetRecordsToSync())
	
	// Restore total records from state
	for _, streamState := range state.Streams {
		for _, stream := range streams {
			if stream.Name() == streamState.Stream && stream.Namespace() == streamState.Namespace {
				if streamState.TotalRecords > 0 {
					fmt.Printf("Restoring record count for stream %s.%s: %d records\n", 
						streamState.Namespace, streamState.Stream, streamState.TotalRecords)
					pool.AddRecordsToSync(streamState.TotalRecords)
				}
			}
		}
	}
	
	// Verify the total records count
	fmt.Println("Final pool total records:", pool.GetRecordsToSync())
	
	if pool.GetRecordsToSync() != 1500 {
		fmt.Printf("FAILED: Expected 1500 total records (1000+500), got %d\n", pool.GetRecordsToSync())
	} else {
		fmt.Println("SUCCESS: Total records correctly loaded into pool")
	}
	
	// Simulate monitoring stats
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
				syncedRecords := pool.SyncedRecords()
				recordsToSync := pool.GetRecordsToSync()
				
				// Add some simulated progress
				pool.recordCount.Add(100)
				
				progress := float64(syncedRecords) / float64(recordsToSync) * 100
				fmt.Printf("Progress: %.1f%% (%d/%d records)\n", 
					progress, syncedRecords, recordsToSync)
			}
		}
	}()
	
	<-ctx.Done()
} 