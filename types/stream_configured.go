package types

import (
	"fmt"
	"strings"
)

// Input/Processed object for Stream
type ConfiguredStream struct {
	StreamMetadata          StreamMetadata `json:"-"`
	InitialCursorStateValue any            `json:"-"` // Cached initial state value

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

// Delete keys from Stream State
// func (s *ConfiguredStream) DeleteStateKeys(keys ...string) []any {
// 	values := []any{}
// 	for _, key := range keys {
// 		val, _ := s.streamState.State.Load(key)
// 		values = append(values, val) // cache

// 		s.streamState.State.Delete(key) // delete
// 	}

// 	return values
// }

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
func (s *ConfiguredStream) ValidateTrackingFieldInSchema(source *Stream) error {
	if s.Stream.SyncMode == INCREMENTAL {
		tf := strings.ToLower(s.Stream.TrackingField)
		schemaProps := map[string]struct{}{}
		var schemaFields []string
		source.Schema.Properties.Range(func(k, _ any) bool {
			field := strings.ToLower(k.(string))
			schemaProps[field] = struct{}{}
			schemaFields = append(schemaFields, field)
			return true
		})
		if _, ok := schemaProps[tf]; !ok {
			s.Stream.SyncMode = FULLREFRESH
			source.AvailableCursorFields = NewSet("_id")
			return fmt.Errorf("invalid tracking field %q: not present in schema fields. "+"Available fields for cursor are: [%s]", tf, strings.Join(schemaFields, ", "))
		}
		source.AvailableCursorFields = NewSet(s.Stream.TrackingField)
	}

	return nil
}
