package types

import (
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/utils"
)

// Input/Processed object for Stream
type ConfiguredStream struct {
	streamState             *StreamState `json:"-"` // in-memory state copy for individual stream
	InitialCursorStateValue any          `json:"-"` // Cached initial state value

	Stream *Stream `json:"stream,omitempty"`

	// Column that's being used as cursor; MUST NOT BE mutated
	//
	// Cursor field is used in Incremental and in Mixed type CDC Read where connector uses
	// this field as recovery column incase of some inconsistencies
	CursorField    string   `json:"cursor_field,omitempty"`
	ExcludeColumns []string `json:"exclude_columns,omitempty"` // TODO: Implement excluding columns from fetching
}

func (s *ConfiguredStream) ID() string {
	return s.Stream.ID()
}

func (s *ConfiguredStream) Self() *ConfiguredStream {
	return s
}

func (s *ConfiguredStream) Name() string {
	return s.Stream.Name
}

func (s *ConfiguredStream) GetStream() *Stream {
	return s.Stream
}

func (s *ConfiguredStream) Namespace() string {
	return s.Stream.Namespace
}

func (s *ConfiguredStream) Schema() *TypeSchema {
	return s.Stream.Schema
}

func (s *ConfiguredStream) SupportedSyncModes() *Set[SyncMode] {
	return s.Stream.SupportedSyncModes
}

func (s *ConfiguredStream) GetSyncMode() SyncMode {
	return s.Stream.SyncMode
}

func (s *ConfiguredStream) Cursor() string {
	return s.CursorField
}

// Returns empty and missing
func (s *ConfiguredStream) SetupState(state *State) {
	// Initialize a state or map the already present state
	if !state.IsZero() {
		i, contains := utils.ArrayContains(state.Streams, func(elem *StreamState) bool {
			return elem.Namespace == s.Namespace() && elem.Stream == s.Name()
		})

		if contains {
			s.InitialCursorStateValue, _ = state.Streams[i].State.Load(s.CursorField)
			s.streamState = state.Streams[i]
			return
		}
	}

	ss := &StreamState{
		Stream:    s.Name(),
		Namespace: s.Namespace(),
		State:     sync.Map{},
	}

	// save references of stream state and add it to connector state
	s.streamState = ss
	state.Streams = append(state.Streams, ss)
}

func (s *ConfiguredStream) InitialState() any {
	return s.InitialCursorStateValue
}

func (s *ConfiguredStream) SetStateCursor(value any) {
	s.streamState.HoldsValue.Store(true)
	s.streamState.State.Store(s.Cursor(), value)
}

func (s *ConfiguredStream) SetStateKey(key string, value any) {
	s.streamState.HoldsValue.Store(true)
	s.streamState.State.Store(key, value)
}

func (s *ConfiguredStream) GetStateCursor() any {
	val, _ := s.streamState.State.Load(s.Cursor())
	return val
}

func (s *ConfiguredStream) GetStateKey(key string) any {
	val, _ := s.streamState.State.Load(key)
	return val
}

// Delete keys from Stream State
func (s *ConfiguredStream) DeleteStateKeys(keys ...string) []any {
	values := []any{}
	for _, key := range keys {
		val, _ := s.streamState.State.Load(key)
		values = append(values, val) // cache

		s.streamState.State.Delete(key) // delete
	}

	return values
}

// Validate Configured Stream with Source Stream
func (s *ConfiguredStream) Validate(source *Stream) error {
	if !source.SupportedSyncModes.Exists(s.Stream.SyncMode) {
		return fmt.Errorf("invalid sync mode[%s]; valid are %v", s.Stream.SyncMode, source.SupportedSyncModes)
	}

	// no cursor validation in cdc and backfill sync
	if s.Stream.SyncMode == INCREMENTAL && !source.AvailableCursorFields.Exists(s.CursorField) {
		return fmt.Errorf("invalid cursor field [%s]; valid are %v", s.CursorField, source.AvailableCursorFields)
	}

	if source.SourceDefinedPrimaryKey.ProperSubsetOf(s.Stream.SourceDefinedPrimaryKey) {
		return fmt.Errorf("differnce found with primary keys: %v", source.SourceDefinedPrimaryKey.Difference(s.Stream.SourceDefinedPrimaryKey).Array())
	}

	return nil
}
