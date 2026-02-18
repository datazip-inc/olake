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

// GetUnSelectedColumnsSet returns a set of columns that are NOT selected, based on the provided
// selected columns list and the stream's current schema snapshot.
//
// Note: this relies on the stream schema representing only the previously known columns. If the
// schema already includes newly discovered columns, we cannot distinguish explicit unselection
// from newly discovered columns.
func (s *ConfiguredStream) GetUnSelectedColumnsSet(columns []string) *Set[string] {
	if len(columns) == 0 {
		return NewSet[string]()
	}

	selectedColumnsSet := NewSet(columns...)
	unselectedColumnsSet := NewSet[string]()

	s.Stream.Schema.Properties.Range(func(col, _ interface{}) bool {
		colName, ok := col.(string)
		if !ok {
			return true
		}
		if !selectedColumnsSet.Exists(colName) {
			unselectedColumnsSet.Insert(colName)
		}
		return true
	})

	return unselectedColumnsSet
}

// RetainSelectedColumns returns a function that filters record data based on the stream's
// SelectedColumns configuration.
func (s *ConfiguredStream) RetainSelectedColumns() func(map[string]interface{}) map[string]interface{} {
	selectedColumns := s.StreamMetadata.SelectedColumns

	// Backward compatibility:
	// Older catalogs (streams.json) may not have the selected_columns field at all.
	// In that case SelectedColumns will be nil after unmarshalling, so we return the data as is.
	if selectedColumns == nil {
		return func(data map[string]interface{}) map[string]interface{} {
			return data
		}
	}

	selectedColumnsSet := NewSet(selectedColumns.Columns...)
	unselectedColumnsSet := s.GetUnSelectedColumnsSet(selectedColumns.Columns)
	syncNewColumns := selectedColumns.SyncNewColumns

	return func(data map[string]interface{}) map[string]interface{} {
		if len(selectedColumns.Columns) == 0 {
			return data
		}

		if syncNewColumns {
			// emit all columns except those that are unselected
			// this ensures all columns that are new are selected by default
			unselectedColumnsSet.Range(func(col string) {
				delete(data, col)
			})
		} else {
			// emit only columns that are selected
			for col := range data {
				if !selectedColumnsSet.Exists(col) {
					delete(data, col)
				}
			}
		}
		return data
	}
}

// IsSelectedColumn returns a predicate that tells whether a column should be emitted for this stream,
// based on the stream's SelectedColumns configuration.
func (s *ConfiguredStream) IsSelectedColumn() func(string) bool {
	selectedColsCfg := s.StreamMetadata.SelectedColumns
	if selectedColsCfg == nil || len(selectedColsCfg.Columns) == 0 {
		return func(string) bool { return true }
	}
	reformateColumns := func(columnsSet *Set[string]) *Set[string] {
		out := NewSet[string]()
		for _, col := range columnsSet.Array() {
			out.Insert(utils.Reformat(col))
		}
		return out
	}

	if selectedColsCfg.SyncNewColumns {
		unselected := s.GetUnSelectedColumnsSet(selectedColsCfg.Columns)
		unselected = reformateColumns(unselected)
		return func(col string) bool {
			return !unselected.Exists(col)
		}
	}

	selected := NewSet(selectedColsCfg.Columns...)
	selected = reformateColumns(selected)
	return func(col string) bool {
		return selected.Exists(col)
	}
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

	_, err := s.GetFilter()
	if err != nil {
		return fmt.Errorf("failed to parse filter %s: %s", s.StreamMetadata.Filter, err)
	}

	return nil
}

func (s *ConfiguredStream) NormalizationEnabled() bool {
	return s.StreamMetadata.Normalization
}
