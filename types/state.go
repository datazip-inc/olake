package types

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
	"github.com/goccy/go-json"
)

type StateType string

const (
	// Global Type indicates that the connector solely acts on Globally shared state across streams
	GlobalType StateType = "GLOBAL"
	// Streme Type indicates that the connector solely acts on individual stream state
	StreamType StateType = "STREAM"
	// Mixed type indicates that the connector works with a mix of Globally shared and
	// Individual stream state (Note: not being used yet but in plan)
	MixedType StateType = "MIXED"
	// constant key for chunks
	ChunksKey = "chunks"
)

// TODO: Add validation tags; Write custom unmarshal that triggers validation
// State is a dto for airbyte state serialization
type State struct {
	*sync.RWMutex `json:"-"`
	Type          StateType      `json:"type"`
	Global        any            `json:"global,omitempty"`
	Streams       []*StreamState `json:"streams,omitempty"` // TODO: make it set
}

var (
	ErrStateMissing       = errors.New("stream missing from state")
	ErrStateCursorMissing = errors.New("cursor field missing from state")
)

func (s *State) SetType(typ StateType) {
	s.Type = typ
}

func (s *State) InitialState(stream *ConfiguredStream) *StreamState {
	return &StreamState{
		Stream:     stream.Name(),
		Namespace:  stream.Namespace(),
		State:      sync.Map{},
		HoldsValue: atomic.Bool{},
	}
}

func (s *State) ResetStreams() {
	s.Lock()
	defer s.Unlock()
	s.Streams = nil
	s.LogState()
}

func (s *State) SetCursor(stream *ConfiguredStream, key string, value any) {
	s.Lock()
	defer s.Unlock()

	index, contains := utils.ArrayContains(s.Streams, func(elem *StreamState) bool {
		return elem.Namespace == stream.Namespace() && elem.Stream == stream.Name()
	})
	if contains {
		s.Streams[index].State.Store(key, value)
		s.Streams[index].HoldsValue.Store(true)
	} else {
		newStream := s.InitialState(stream)
		newStream.State.Store(key, value)
		newStream.HoldsValue.Store(true)
		s.Streams = append(s.Streams, newStream)
	}
	s.LogState()
}

func (s *State) GetCursor(stream *ConfiguredStream, key string) any {
	s.RLock()
	defer s.RUnlock()
	index, contains := utils.ArrayContains(s.Streams, func(elem *StreamState) bool {
		return elem.Namespace == stream.Namespace() && elem.Stream == stream.Name()
	})
	if contains {
		val, _ := s.Streams[index].State.Load(key)
		return val
	}
	return nil
}

// GetStateChunks retrieves all chunks from the state.
func (s *State) GetChunks(stream *ConfiguredStream) *Set[Chunk] {
	s.RLock()
	defer s.RUnlock()

	index, contains := utils.ArrayContains(s.Streams, func(elem *StreamState) bool {
		return elem.Namespace == stream.Namespace() && elem.Stream == stream.Name()
	})
	if contains {
		chunks, _ := s.Streams[index].State.Load(ChunksKey)
		if chunks != nil {
			chunksSet, converted := chunks.(*Set[Chunk])
			if converted {
				return chunksSet
			}
		}
	}
	return nil
}

// set chunks
func (s *State) SetChunks(stream *ConfiguredStream, chunks *Set[Chunk]) {
	s.Lock()
	defer s.Unlock()

	index, contains := utils.ArrayContains(s.Streams, func(elem *StreamState) bool {
		return elem.Namespace == stream.Namespace() && elem.Stream == stream.Name()
	})
	if contains {
		s.Streams[index].State.Store(ChunksKey, chunks)
		s.Streams[index].HoldsValue.Store(true)
	} else {
		newStream := s.InitialState(stream)
		newStream.State.Store(ChunksKey, chunks)
		newStream.HoldsValue.Store(true)
		s.Streams = append(s.Streams, newStream)
	}
	s.LogState()
}

// remove chunk
func (s *State) RemoveChunk(stream *ConfiguredStream, chunk Chunk) {
	s.Lock()
	defer s.Unlock()

	index, contains := utils.ArrayContains(s.Streams, func(elem *StreamState) bool {
		return elem.Namespace == stream.Namespace() && elem.Stream == stream.Name()
	})
	if contains {
		stateChunks, loaded := s.Streams[index].State.LoadAndDelete(ChunksKey)
		if loaded {
			stateChunks.(*Set[Chunk]).Remove(chunk)
			s.Streams[index].State.Store(ChunksKey, stateChunks)
		}
	}
	s.LogState()
}

func (s *State) SetGlobalState(globalState any) {
	s.Lock()
	defer s.Unlock()
	s.Global = globalState
	s.LogState()
}

func (s *State) isZero() bool {
	return s.Global == nil && len(s.Streams) == 0
}

func (s *State) MarshalJSON() ([]byte, error) {
	if s.isZero() {
		return json.Marshal(nil)
	}

	type Alias State
	p := Alias(*s)

	populatedStreams := []*StreamState{}
	for _, stream := range p.Streams {
		if stream.HoldsValue.Load() {
			populatedStreams = append(populatedStreams, stream)
		}
	}

	p.Streams = populatedStreams
	return json.Marshal(p)
}

func (s *State) LogWithLock() {
	s.Lock()
	defer s.Unlock()
	s.LogState()
}

func (s *State) LogState() {
	// function need to be called after state lock
	if s.isZero() {
		logger.Info("state is empty")
		return
	}

	// message := Message{
	// 	Type:  StateMessage,
	// 	State: s,
	// }
	// TODO: Only Log in logs file, not in CLI
	// logger.Info(message)

	// log to state file
	err := logger.FileLogger(s, "state", ".json")
	if err != nil {
		logger.Fatalf("failed to create state file: %s", err)
	}
}

// Chunk struct that holds status, min, and max values
type Chunk struct {
	Min any `json:"min"`
	Max any `json:"max"`
}

type StreamState struct {
	HoldsValue atomic.Bool `json:"-"` // If State holds some value and should not be excluded during unmarshaling then value true

	Stream    string   `json:"stream"`
	Namespace string   `json:"namespace"`
	SyncMode  string   `json:"sync_mode"`
	State     sync.Map `json:"state"`
}

// MarshalJSON custom marshaller to handle sync.Map encoding
func (s *StreamState) MarshalJSON() ([]byte, error) {
	// Create a map to temporarily store data for JSON marshaling
	stateMap := make(map[string]interface{})
	s.State.Range(func(key, value interface{}) bool {
		strKey, ok := key.(string)
		if !ok {
			return false
		}
		stateMap[strKey] = value
		return true
	})

	// Create an alias to avoid infinite recursion
	type Alias StreamState
	return json.Marshal(&struct {
		*Alias
		State map[string]interface{} `json:"state"`
	}{
		Alias: (*Alias)(s),
		State: stateMap,
	})
}

// UnmarshalJSON custom unmarshaller to handle sync.Map decoding
func (s *StreamState) UnmarshalJSON(data []byte) error {
	// Create a temporary structure to unmarshal JSON into
	type Alias StreamState
	aux := &struct {
		*Alias
		State map[string]interface{} `json:"state"`
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Populate sync.Map with the data from temporary map
	for key, value := range aux.State {
		s.State.Store(key, value)
	}

	if len(aux.State) > 0 {
		s.HoldsValue.Store(true)
	}
	if rawChunks, exists := aux.State[ChunksKey]; exists {
		if chunkList, ok := rawChunks.([]interface{}); ok {
			chunksJSON, err := json.Marshal(chunkList)
			if err != nil {
				return err
			}

			var chunks []Chunk
			if err := json.Unmarshal(chunksJSON, &chunks); err != nil {
				return err
			}

			// Create a new Set[Chunk] and add chunks to it
			chunkSet := NewSet[Chunk](chunks...) // Assuming you have a NewSet function
			// Store the *Set[Chunk] in State
			s.State.Store(ChunksKey, chunkSet)
		}
	}
	return nil
}

func NewGlobalState[T GlobalState](state T) *Global[T] {
	return &Global[T]{
		State:   state,
		Streams: NewSet[string](),
	}
}

type GlobalState interface {
	IsEmpty() bool
}

type Global[T GlobalState] struct {
	// Global State shared by streams
	State T `json:"state"`
	// Attaching Streams to Global State helps in recognizing the tables that the state belongs to.
	//
	// This results in helping connector determine what streams were synced during the last sync in
	// Group read. and also helps connectors to migrate from incremental to CDC Read without the need to
	// full load with the help of using cursor value and field as recovery cursor for CDC
	Streams *Set[string] `json:"streams"`
}

func (g *Global[T]) MarshalJSON() ([]byte, error) {
	if any(g.State).(GlobalState).IsEmpty() {
		return json.Marshal(nil)
	}

	type Alias Global[T]
	p := Alias(*g)

	return json.Marshal(p)
}

func (g *Global[T]) UnmarshalJSON(data []byte) error {
	// Define a type alias to avoid recursion
	type Alias Global[T]

	// Create a temporary alias value to unmarshal into
	var temp Alias

	temp.Streams = NewSet[string]()

	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	*g = Global[T](temp)
	return nil
}
