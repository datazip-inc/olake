package types

import (
	"fmt"
	"regexp"
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

// Condition represents a single condition in a filter
type Condition struct {
	Column   string
	Operator string
	Value    string
}

// Filter represents the parsed filter
type Filter struct {
	Conditions      []Condition // a > b, a < b
	LogicalOperator string      // condition[0] and/or condition[1], single and/or supported
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

func (s *ConfiguredStream) GetFilter() (Filter, error) {
	filter := s.StreamMetadata.Filter
	if filter == "" {
		return Filter{}, nil
	}
	// example: a>b, a>=b, a<b, a<=b, a!=b, a=b, a="b", a=\"b\" and c>d, a="b" or c>d
	var FilterRegex = regexp.MustCompile(`^(\w+)\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\w+)\s*(?:(and|or)\s*(\w+)\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\w+))?\s*$`)
	matches := FilterRegex.FindStringSubmatch(filter)
	if matches == nil {
		return Filter{}, fmt.Errorf("invalid filter format: %s", filter)
	}
	var conditions []Condition
	conditions = append(conditions, Condition{
		Column:   matches[1],
		Operator: matches[2],
		Value:    matches[3],
	})

	if matches[4] != "" {
		conditions = append(conditions, Condition{
			Column:   matches[5],
			Operator: matches[6],
			Value:    matches[7],
		})
	}

	return Filter{
		Conditions:      conditions,
		LogicalOperator: matches[4],
	}, nil
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

	_, err := s.GetFilter()
	if err != nil {
		return fmt.Errorf("failed to parse filter: %s", err)
	}

	return nil
}

func (s *ConfiguredStream) NormalizationEnabled() bool {
	return s.StreamMetadata.Normalization
}
