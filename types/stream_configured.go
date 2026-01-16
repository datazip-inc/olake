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

// // Condition represents a single condition in a filter
// type Condition struct {
// 	Column   string
// 	Operator string
// 	Value    string
// }

// // Filter represents the parsed filter
// type Filter struct {
// 	Conditions      []Condition // a > b, a < b
// 	LogicalOperator string      // condition[0] and/or condition[1], single and/or supported
// }

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

func (s *ConfiguredStream) GetFilter() (FilterInput, bool, error) {
	//new filter input
	if s.StreamMetadata.FilterInput != nil && len(s.StreamMetadata.FilterInput.Conditions) > 0 {
		s.StreamMetadata.FilterInput.LogicalOperator = utils.Reformat(s.StreamMetadata.FilterInput.LogicalOperator)
		return *s.StreamMetadata.FilterInput, false, nil
	}

	// legacy filter input
	filter := strings.TrimSpace(s.StreamMetadata.Filter)
	if filter == "" {
		return FilterInput{}, true, nil
	}
	// FilterRegex supports the following filter patterns:
	// Single condition:
	//   - Normal columns: age > 18, status = \"active\", count != 0
	//   - Special char columns (quoted): \"user-name\" = \"john\", \"email@domain\" = \"test@example.com\"
	//   - Numeric values: price >= 99.99, discount <= 0.5, id = 123
	//   - Quoted string values: name = \"John Doe\", city = \"New York\", a = \"val\"
	//   - Mixed special chars: \"column.name\" > 10, \"data[0]\" = \"value\"
	// Two conditions with logical operators:
	//   - AND operator: age > 18 AND status = \"active\"
	//   - OR operator: role != \"admin\" OR role = \"moderator\"
	//   - Mixed types: \"user-id\" = 123 AND \"is-active\" = true
	//   - Special chars both sides: \"first name\" = "John" AND \"last-name\" = \"Doe\"
	//   - Case insensitive: age > 18 and status = active, price < 100 or discount > 0
	// Supported operators: =, !=, <, >, <=, >=
	// Value types: quoted strings, integers, floats (including negative), decimals, unquoted words
	var FilterRegex = regexp.MustCompile(`^(?:"([^"]*)"|(\w+))\s*(>=|<=|!=|>|<|=)\s*((?:"[^"]*"|-?\d+\.\d+|-?\d+|\.\d+|\w+))\s*(?:((?i:and|or))\s*(?:"([^"]*)"|(\w+))\s*(>=|<=|!=|>|<|=)\s*((?:"[^"]*"|-?\d+\.\d+|-?\d+|\.\d+|\w+)))?\s*$`)
	matches := FilterRegex.FindStringSubmatch(filter)
	if len(matches) == 0 {
		return FilterInput{}, true, fmt.Errorf("invalid filter format: %s", filter)
	}

	var conditions []FilterCondition
	conditions = append(conditions, FilterCondition{
		Column:   utils.ExtractColumnName(matches[1], matches[2]),
		Operator: matches[3],
		Value:    matches[4],
	})

	// Check if there's a logical operator (and/or)
	logicalOp := matches[5]
	if logicalOp != "" {
		conditions = append(conditions, FilterCondition{
			Column:   utils.ExtractColumnName(matches[6], matches[7]),
			Operator: matches[8],
			Value:    matches[9],
		})
	}

	return FilterInput{
		Conditions:      conditions,
		LogicalOperator: logicalOp,
	}, true, nil
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

	_, _, err := s.GetFilter()
	if err != nil {
		return fmt.Errorf("failed to parse filter %s: %s", s.StreamMetadata.Filter, err)
	}

	return nil
}

func (s *ConfiguredStream) NormalizationEnabled() bool {
	return s.StreamMetadata.Normalization
}
