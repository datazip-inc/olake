package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/datazip-inc/olake/types"
)

func main() {
	// Create a test state file with total records
	fmt.Println("=== Testing State Total Records Implementation ===")
	
	// Create stream state with HoldsValue set
	streamState := &types.StreamState{
		Stream:       "test_table",
		Namespace:    "test_db",
		SyncMode:     "full_refresh",
		TotalRecords: 1000,
		State:        sync.Map{},
	}
	streamState.HoldsValue.Store(true)
	
	// Create state with stream record counts
	state := &types.State{
		Type:    types.StreamType,
		RWMutex: &sync.RWMutex{},
		Streams: []*types.StreamState{streamState},
	}
	
	// Marshal and save to file
	stateJSON, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		fmt.Printf("Failed to marshal state: %v\n", err)
		os.Exit(1)
	}
	
	err = ioutil.WriteFile("test_state.json", stateJSON, 0644)
	if err != nil {
		fmt.Printf("Failed to write state file: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Println("Created test state file with TotalRecords=1000")
	fmt.Println("State file content:")
	fmt.Println(string(stateJSON))
	
	// Now load the state file back
	loadedData, err := ioutil.ReadFile("test_state.json")
	if err != nil {
		fmt.Printf("Failed to read state file: %v\n", err)
		os.Exit(1)
	}
	
	loadedState := &types.State{
		RWMutex: &sync.RWMutex{},
	}
	
	err = json.Unmarshal(loadedData, loadedState)
	if err != nil {
		fmt.Printf("Failed to unmarshal state: %v\n", err)
		os.Exit(1)
	}
	
	// Verify the total records were preserved
	if len(loadedState.Streams) != 1 {
		fmt.Printf("Expected 1 stream, got %d\n", len(loadedState.Streams))
		os.Exit(1)
	}
	
	streamState = loadedState.Streams[0]
	fmt.Printf("Loaded stream: %s.%s\n", streamState.Namespace, streamState.Stream)
	fmt.Printf("Total records: %d\n", streamState.TotalRecords)
	
	if streamState.TotalRecords != 1000 {
		fmt.Printf("FAILED: Expected 1000 total records, got %d\n", streamState.TotalRecords)
		os.Exit(1)
	}
	
	fmt.Println("SUCCESS: Total records correctly serialized and deserialized")
	
	// Clean up
	os.Remove("test_state.json")
} 