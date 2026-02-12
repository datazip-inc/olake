package types

type SelectedColumns struct {
	Columns        []string `json:"columns"`
	SyncNewColumns bool     `json:"sync_new_columns"`
}

// GetSelectedColumnsSet returns a set of selected columns
func GetSelectedColumnsSet(columns []string) *Set[string] {
	return NewSet(columns...)
}

// GetUnSelectedColumnsSet returns a set of unselected columns
func GetUnSelectedColumnsSet(columns []string, oldStream *Stream) *Set[string] {
	if len(columns) == 0 {
		return NewSet[string]()
	}

	oldSchemaColumnsSet := collectColumnsFromSchemaAsSet(oldStream.Schema)
	selectedColumnsSet := NewSet(columns...)
	return oldSchemaColumnsSet.Difference(selectedColumnsSet)
}

// FilterDataBySelectedColumns returns a function that filters data based on the following rules:
// - sync_new_columns=true:
//   - Specific columns selected: Only selected columns sync; newly added columns are automatically included
//   - All columns selected: All columns sync, including newly added columns.
//
// - sync_new_columns=false:
//   - Specific columns selected: Only explicitly selected columns sync
//   - All columns selected: All existing columns sync; newly added columns are NOT synced
func FilterDataBySelectedColumns(stream StreamInterface, oldStream *Stream) func(map[string]interface{}) map[string]interface{} {
	selectedColumns := stream.Self().StreamMetadata.SelectedColumns
	selectedColumnsSet := GetSelectedColumnsSet(selectedColumns.Columns)
	unselectedColumnsSet := GetUnSelectedColumnsSet(selectedColumns.Columns, oldStream)
	syncNewColumns := selectedColumns.SyncNewColumns

	return func(data map[string]interface{}) map[string]interface{} {
		if len(selectedColumns.Columns) == 0 {
			return data
		}

		if syncNewColumns {
			// emit all columns except those that are unselected
			// this ensures all columns that are new are selected by default
			for col := range data {
				if unselectedColumnsSet.Exists(col) {
					delete(data, col)
				}
			}
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

// collectColumnsFromSchemaAsSet collects all columns from a schema as a Set
func collectColumnsFromSchemaAsSet(schema *TypeSchema) *Set[string] {
	columns := []string{}
	schema.Properties.Range(func(col, _ interface{}) bool {
		if colName, isColTypeString := col.(string); isColTypeString {
			columns = append(columns, colName)
		}
		return true
	})
	return NewSet(columns...)
}

// collectColumnsFromSchema collects all columns from a schema
func collectColumnsFromSchema(schema *TypeSchema) []string {
	return collectColumnsFromSchemaAsSet(schema).Array()
}

// MergeSelectedColumns merges the selected columns based on the following rules:
// 1. if selectedColumns property is not present or empty, initialize with columns from new schema
// 2. if selectedColumns property is present and not empty, filter the selected columns to only those present in both old and new schemas
// 3. if sync_new_columns is true, add newly discovered columns to the selected columns
// 4. set the selected columns set
// 5. set the unselected columns set
// 6. set the all selected flag
func MergeSelectedColumns(
	metadata *StreamMetadata,
	oldStream *Stream,
	newStream *Stream,
) {
	oldSchemaColumnsSet := collectColumnsFromSchemaAsSet(oldStream.Schema)
	newSchemaColumnsSet := collectColumnsFromSchemaAsSet(newStream.Schema)

	var columns []string

	// no previous selection: initialize with all columns from new schema
	if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Columns) == 0 {
		columns = collectColumnsFromSchema(newStream.Schema)
	} else {
		previouslySelectedColumnsSet := NewSet(metadata.SelectedColumns.Columns...)
		if metadata.SelectedColumns.SyncNewColumns {
			// add newly discovered columns when sync_new_columns is true
			newColumnsSet := newSchemaColumnsSet.Difference(oldSchemaColumnsSet)
			columns = previouslySelectedColumnsSet.Union(newColumnsSet).Array()
		} else {
			columns = previouslySelectedColumnsSet.Array()
		}
	}

	metadata.SelectedColumns = &SelectedColumns{
		Columns:        columns,
		SyncNewColumns: metadata.SelectedColumns.SyncNewColumns,
	}
}
