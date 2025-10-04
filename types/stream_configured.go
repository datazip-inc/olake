package types

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/datazip-inc/olake/utils"
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

func (s *ConfiguredStream) GetDestinationDatabase(icebergDB *string) string {
	if s.Stream.DestinationDatabase != "" {
		return utils.Reformat(s.Stream.DestinationDatabase)
	}
	if icebergDB != nil && *icebergDB != "" {
		return *icebergDB
	}
	return s.Stream.Namespace
}

func (s *ConfiguredStream) GetDestinationTable() string {
	return utils.Ternary(s.Stream.DestinationTable == "", s.Stream.Name, s.Stream.DestinationTable).(string)
}

// returns primary and secondary cursor
func (s *ConfiguredStream) Cursor() (string, string) {
	cursorFields := strings.Split(s.Stream.CursorField, ":")
	primaryCursor := cursorFields[0]
	secondaryCursor := ""
	if len(cursorFields) > 1 {
		secondaryCursor = cursorFields[1]
	}
	return primaryCursor, secondaryCursor
}

func (s *ConfiguredStream) GetFilter() (Filter, error) {
	filter := s.StreamMetadata.Filter
	if filter == "" {
		return Filter{}, nil
	}
	// Column names can now be:
	// 1. Regular word characters: column_name
	// 2. Quoted with double quotes: "column-name" or "column name"
	// 3. Quoted with backticks: `column-name` or `column name`
	// example: "user-id">5, `order date`="2024-01-01", status="active" and `item.price`>100
	var FilterRegex = regexp.MustCompile(`^("([^"]*)"|` + "`([^`]*)`" + `|(\w+))\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\d*\.?\d+|\w+)\s*(?:(and|or)\s*("([^"]*)"|` + "`([^`]*)`" + `|(\w+))\s*(>=|<=|!=|>|<|=)\s*(\"[^\"]*\"|\d*\.?\d+|\w+))?\s*$`)
	matches := FilterRegex.FindStringSubmatch(filter)
	if len(matches) == 0 {
		return Filter{}, fmt.Errorf("invalid filter format: %s", filter)
	}

	// Helper function to extract column name from multiple capture groups
	extractColumn := func(groups ...string) string {
		for _, group := range groups {
			if group != "" {
				return group
			}
		}
		return ""
	}

	var conditions []Condition

	// Extract first column name (could be in matches[2], matches[3], or matches[4])
	column1 := extractColumn(matches[2], matches[3], matches[4])
	conditions = append(conditions, Condition{
		Column:   column1,
		Operator: matches[5],
		Value:    matches[6],
	})

	if matches[7] != "" {
		// Extract second column name (could be in matches[9], matches[10], or matches[11])
		column2 := extractColumn(matches[9], matches[10], matches[11])
		conditions = append(conditions, Condition{
			Column:   column2,
			Operator: matches[12],
			Value:    matches[13],
		})
	}

	return Filter{
		Conditions:      conditions,
		LogicalOperator: matches[7],
	}, nil
}

// Validate Configured Stream with Source Stream
func (s *ConfiguredStream) Validate(source *Stream) error {
	if !source.SupportedSyncModes.Exists(s.Stream.SyncMode) {
		return fmt.Errorf("invalid sync mode[%s]; valid are %v", s.Stream.SyncMode, source.SupportedSyncModes)
	}

	// no cursor validation in cdc and backfill sync
	if s.Stream.SyncMode == INCREMENTAL {
		primaryCursor, secondaryCursor := s.Cursor()
		if !source.AvailableCursorFields.Exists(primaryCursor) {
			return fmt.Errorf("invalid cursor field [%s]; valid are %v", primaryCursor, source.AvailableCursorFields)
		}
		if secondaryCursor != "" && !source.AvailableCursorFields.Exists(secondaryCursor) {
			return fmt.Errorf("invalid secondary cursor field [%s]; valid are %v", secondaryCursor, source.AvailableCursorFields)
		}
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
