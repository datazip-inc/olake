package types

import (
	"fmt"

	"github.com/datazip-inc/olake/utils"
)

// Input/Processed object for Stream
type ConfiguredStream struct {
	Stream   *Stream  `json:"stream,omitempty"`
	SyncMode SyncMode `json:"sync_mode,omitempty"` // Mode being used for syncing data
	// Column that's being used as cursor; MUST NOT BE mutated
	//
	// Cursor field is used in Incremental and in Mixed type GroupRead where connector uses
	// this field as recovery column incase of some inconsistencies
	CursorField    string       `json:"cursor_field,omitempty"`
	ExcludeColumns []string     `json:"exclude_columns,omitempty"` // TODO: Implement excluding columns from fetching
	CursorValue    any          `json:"-"`                         // Cached initial state value
	batchSize      int          `json:"-"`                         // Batch size for syncing data
	state          *StreamState `json:"-"`                         // in-memory state copy for individual stream
	connectorState *State       `json:"-"`                         // in-memory pointer to central state

	// DestinationSyncMode string   `json:"destination_sync_mode,omitempty"`
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
	return s.SyncMode
}

func (s *ConfiguredStream) Cursor() string {
	return s.CursorField
}

// Returns empty and missing
func (s *ConfiguredStream) SetupState(state *State, batchSize int) error {
	s.SetBatchSize(batchSize)
	s.connectorState = state

	if !state.IsZero() {
		i, contains := utils.ArrayContains(state.Streams, func(elem *StreamState) bool {
			return elem.Namespace == s.Namespace() && elem.Stream == s.Name()
		})
		if contains {
			value, found := state.Streams[i].State[s.CursorField]
			if !found {
				return ErrStateCursorMissing
			}

			s.CursorValue = value
			s.state = state.Streams[i]

			return nil
		}

		return ErrStateMissing
	}

	return nil
}

func (s *ConfiguredStream) InitialState() any {
	return s.CursorValue
}

func (s *ConfiguredStream) SetState(value any) {
	s.connectorState.Lock()
	defer s.connectorState.Unlock()

	if s.state == nil {
		ss := &StreamState{
			Stream:    s.Name(),
			Namespace: s.Namespace(),
			State: map[string]any{
				s.Cursor(): value,
			},
		}

		// save references of state
		s.state = ss
		s.connectorState.Streams = append(s.connectorState.Streams, ss)
		return
	}

	s.state.State[s.Cursor()] = value
}

func (s *ConfiguredStream) GetState() any {
	s.connectorState.Lock()
	defer s.connectorState.Unlock()

	if s.state == nil || s.state.State == nil {
		return nil
	}
	return s.state.State[s.Cursor()]
}

func (s *ConfiguredStream) BatchSize() int {
	return s.batchSize
}

func (s *ConfiguredStream) SetBatchSize(size int) {
	s.batchSize = size
}

// Validate Configured Stream with Source Stream
func (s *ConfiguredStream) Validate(source *Stream) error {
	if !source.SupportedSyncModes.Exists(s.SyncMode) {
		return fmt.Errorf("invalid sync mode[%s]; valid are %v", s.SyncMode, source.SupportedSyncModes)
	}

	if !source.AvailableCursorFields.Exists(s.CursorField) {
		return fmt.Errorf("invalid cursor field [%s]; valid are %v", s.CursorField, source.AvailableCursorFields)
	}

	if source.SourceDefinedPrimaryKey.ProperSubsetOf(s.Stream.SourceDefinedPrimaryKey) {
		return fmt.Errorf("differnce found with primary keys: %v", source.SourceDefinedPrimaryKey.Difference(s.Stream.SourceDefinedPrimaryKey).Array())
	}

	return nil
}
