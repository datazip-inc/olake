package types

import (
	"fmt"
	"regexp"
)

// Input/Processed object for Stream
type ConfiguredStream struct {
	StreamMetadata StreamMetadata `json:"-"`
	Stream         *Stream        `json:"stream,omitempty"`
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
	return s.Stream.CursorField
}

func (s *ConfiguredStream) GetFilter() (Filter, error) {
	filter := s.StreamMetadata.Filter
	if filter == "" {
		return Filter{}, nil
	}
	// example: a>b, a>=b, a<b, a<=b, a!=b, a=b, a="b", a=\"b\" and c>d, a="b" or c>d
	var FilterRegex = regexp.MustCompile(`^(\w+)\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\d*\.?\d+|\w+)\s*(?:(and|or)\s*(\w+)\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\d*\.?\d+|\w+))?\s*$`)
	matches := FilterRegex.FindStringSubmatch(filter)
	if len(matches) == 0 {
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
	if s.Stream.SyncMode == INCREMENTAL && !source.AvailableCursorFields.Exists(s.Cursor()) {
		return fmt.Errorf("invalid cursor field [%s]; valid are %v", s.Cursor(), source.AvailableCursorFields)
	}

	if source.SourceDefinedPrimaryKey.ProperSubsetOf(s.Stream.SourceDefinedPrimaryKey) {
		return fmt.Errorf("differnce found with primary keys: %v", source.SourceDefinedPrimaryKey.Difference(s.Stream.SourceDefinedPrimaryKey).Array())
	}

	_, err := s.GetFilter()
	if err != nil {
		return fmt.Errorf("failed to parse filter %s: %s", s.StreamMetadata.Filter, err)
	}

	return nil
}

func (s *ConfiguredStream) NormalizationEnabled() bool {
	return s.StreamMetadata.Normalization
}
