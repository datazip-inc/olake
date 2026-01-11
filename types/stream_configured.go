package types

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/datazip-inc/olake/constants"
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

func (s *ConfiguredStream) GetSelectedColumns() []string {
	return s.StreamMetadata.SelectedColumns.Columns
}

func (s *ConfiguredStream) GetSelectedColumnsMap() map[string]struct{} {
	return s.StreamMetadata.SelectedColumns.Map
}

func (s *ConfiguredStream) GetSelectedColumnsAllSelected() bool {
	return s.StreamMetadata.SelectedColumns.AllSelected
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

// parseCursorField parses a cursor field string and returns primary and secondary cursor fields
// Format: "primary_cursor" or "primary_cursor:secondary_cursor"
func parseCursorField(cursorField string) (string, string) {
	cursorFields := strings.Split(cursorField, ":")
	primaryCursor := cursorFields[0]
	secondaryCursor := ""
	if len(cursorFields) > 1 {
		secondaryCursor = cursorFields[1]
	}
	return primaryCursor, secondaryCursor
}

// returns primary and secondary cursor
func (s *ConfiguredStream) Cursor() (string, string) {
	return parseCursorField(s.Stream.CursorField)
}

func (s *ConfiguredStream) GetFilter() (Filter, error) {
	filter := strings.TrimSpace(s.StreamMetadata.Filter)
	if filter == "" {
		return Filter{}, nil
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
		return Filter{}, fmt.Errorf("invalid filter format: %s", filter)
	}

	var conditions []Condition
	conditions = append(conditions, Condition{
		Column:   utils.ExtractColumnName(matches[1], matches[2]),
		Operator: matches[3],
		Value:    matches[4],
	})

	// Check if there's a logical operator (and/or)
	logicalOp := matches[5]
	if logicalOp != "" {
		conditions = append(conditions, Condition{
			Column:   utils.ExtractColumnName(matches[6], matches[7]),
			Operator: matches[8],
			Value:    matches[9],
		})
	}

	return Filter{
		Conditions:      conditions,
		LogicalOperator: logicalOp,
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

	if len(s.StreamMetadata.SelectedColumns.Columns) == 0 {
		s.StreamMetadata.SelectedColumns.AllSelected = true
	} else {
		setSelectedColumnsMap(s.StreamMetadata.SelectedColumns)
		// set all columns selected flag
		s.StreamMetadata.SelectedColumns.AllSelected = s.checkAllColumnsSelected()
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

// checkAllColumnsSelected checks if all columns in the schema are selected by the user
func (s *ConfiguredStream) checkAllColumnsSelected() bool {
	selectedMap := s.StreamMetadata.SelectedColumns.Map
	if len(selectedMap) == 0 {
		return true
	}

	var (
		schemaColumnCount int
		allSelected       bool
	)

	allSelected = true

	s.Stream.Schema.Properties.Range(func(key, _ interface{}) bool {
		schemaColumnCount++

		colName, ok := key.(string)
		if !ok {
			allSelected = false
			return false
		}

		if _, exists := selectedMap[colName]; !exists {
			allSelected = false
			return false
		}

		return true
	})

	return allSelected && len(selectedMap) == schemaColumnCount
}

func setSelectedColumnsMap(selectedColumns *SelectedColumns) {
	selectedMap := make(map[string]struct{})
	for _, col := range selectedColumns.Columns {
		// if column already exists, skip
		if _, exists := selectedMap[col]; exists {
			continue
		}
		selectedMap[col] = struct{}{}
	}
	selectedColumns.Map = selectedMap
}

// FilterDataBySelectedColumns filters the data based on the selected columns
// Returns the original data if no columns are selected or all columns are selected
func FilterDataBySelectedColumns(data map[string]interface{}, selectedMap map[string]struct{}, allSelected bool) map[string]interface{} {
	if len(selectedMap) == 0 || allSelected {
		return data
	}

	filtered := make(map[string]interface{})
	for key, value := range data {
		if _, exists := selectedMap[key]; exists {
			filtered[key] = value
		}
	}
	return filtered
}

// ensureMandatoryColumns ensures that mandatory columns are always in SelectedColumns:
// 1. cursor fields,
// 2. CDC columns,
// 3. source defined primary key columns,
// 4. system generated fields
func ensureMandatoryColumns(streamMetadata *StreamMetadata, stream *Stream) {
	setSelectedColumnsMap(streamMetadata.SelectedColumns)

	selectedMap := streamMetadata.SelectedColumns.Map

	// Add system generated fields
	systemFields := []string{constants.OlakeID, constants.OlakeTimestamp, constants.OpType}
	for _, sysField := range systemFields {
		if _, exists := selectedMap[sysField]; !exists {
			streamMetadata.SelectedColumns.Columns = append(streamMetadata.SelectedColumns.Columns, sysField)
			selectedMap[sysField] = struct{}{}
		}
	}

	// Add cursor fields for incremental sync
	if stream.SyncMode == INCREMENTAL && stream.CursorField != "" {
		primaryCursor, secondaryCursor := parseCursorField(stream.CursorField)
		if primaryCursor != "" {
			if _, exists := selectedMap[primaryCursor]; !exists {
				streamMetadata.SelectedColumns.Columns = append(streamMetadata.SelectedColumns.Columns, primaryCursor)
				selectedMap[primaryCursor] = struct{}{}
			}
		}
		if secondaryCursor != "" {
			if _, exists := selectedMap[secondaryCursor]; !exists {
				streamMetadata.SelectedColumns.Columns = append(streamMetadata.SelectedColumns.Columns, secondaryCursor)
				selectedMap[secondaryCursor] = struct{}{}
			}
		}
	}

	// Add CDC columns if CDC sync mode
	if stream.SyncMode == CDC || stream.SyncMode == STRICTCDC {
		if _, exists := selectedMap[constants.CdcTimestamp]; !exists {
			streamMetadata.SelectedColumns.Columns = append(streamMetadata.SelectedColumns.Columns, constants.CdcTimestamp)
			selectedMap[constants.CdcTimestamp] = struct{}{}
		}
	}

	// Add source defined primary key columns
	for _, pk := range stream.SourceDefinedPrimaryKey.Array() {
		if _, exists := selectedMap[pk]; !exists {
			streamMetadata.SelectedColumns.Columns = append(streamMetadata.SelectedColumns.Columns, pk)
			selectedMap[pk] = struct{}{}
		}
	}
}
