package types

type SelectedColumns struct {
	Columns        []string `json:"columns"`
	SyncNewColumns bool     `json:"sync_new_columns"`
}

// GetUnSelectedColumnsSet returns a set of unselected columns
func GetUnSelectedColumnsSet(columns []string, stream *Stream) *Set[string] {
	if len(columns) == 0 {
		return NewSet[string]()
	}
	selectedColumnsSet := NewSet(columns...)
	unselectedColumnsSet := NewSet[string]()

	// the provided stream schema snapshot should represent only the previously known columns
	// if it already includes newly added columns, we cannot distinguish which ones were explicitly unselected vs newly discovered
	stream.Schema.Properties.Range(func(col, _ interface{}) bool {
		colName, isColTypeString := col.(string)
		if !isColTypeString {
			return true
		}
		if !selectedColumnsSet.Exists(colName) {
			unselectedColumnsSet.Insert(colName)
		}
		return true
	})

	return unselectedColumnsSet
}

// collectColumnsFromSchema collects all columns from a schema
func collectColumnsFromSchema(schema *TypeSchema) []string {
	var columns []string
	schema.Properties.Range(func(col, _ interface{}) bool {
		if colName, isColTypeString := col.(string); isColTypeString {
			columns = append(columns, colName)
		}
		return true
	})
	return columns
}

// FilterDataBySelectedColumns returns a function that filters data based on the following rules:
// - sync_new_columns=true:
//   - Specific columns selected: Only selected columns sync; newly added columns are automatically included
//   - All columns selected: All columns sync, including newly added columns.
//
// - sync_new_columns=false:
//   - Specific columns selected: Only explicitly selected columns sync
//   - All columns selected: All existing columns sync; newly added columns are NOT synced
func FilterDataBySelectedColumns(stream StreamInterface) func(map[string]interface{}) map[string]interface{} {
	selectedColumns := stream.Self().StreamMetadata.SelectedColumns

	// Backward compatibility:
	// Older catalogs (streams.json) may not have the selected_columns field at all.
	// In that case SelectedColumns will be nil after unmarshalling, so we return the data as is.
	if selectedColumns == nil {
		return func(data map[string]interface{}) map[string]interface{} {
			return data
		}
	}

	selectedColumnsSet := NewSet(selectedColumns.Columns...)
	unselectedColumnsSet := GetUnSelectedColumnsSet(selectedColumns.Columns, stream.GetStream())
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

// MergeSelectedColumns merges the selected columns based on the following rules:
// if selectedColumns property is not present or empty, initialize with columns from new schema
// preserve previously selected columns
// if sync_new_columns is true, add newly discovered columns to the selected columns
func MergeSelectedColumns(
	metadata *StreamMetadata,
	oldStream *Stream,
	newStream *Stream,
) {
	var columns []string
	// no previous selection: initialize with all columns from new schema
	if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Columns) == 0 {
		columns = collectColumnsFromSchema(newStream.Schema)
	} else {
		previouslySelectedSet := NewSet(metadata.SelectedColumns.Columns...)
		oldSchemaCols := NewSet(collectColumnsFromSchema(oldStream.Schema)...)

		// iterate new schema: retain previously selected columns, add new ones if sync_new_columns enabled
		newStream.Schema.Properties.Range(func(key, _ interface{}) bool {
			col, ok := key.(string)
			if !ok {
				return true
			}
			if previouslySelectedSet.Exists(col) || (metadata.SelectedColumns.SyncNewColumns && !oldSchemaCols.Exists(col)) {
				columns = append(columns, col)
			}
			return true
		})
	}
	syncNewColumns := true
	if metadata.SelectedColumns != nil {
		syncNewColumns = metadata.SelectedColumns.SyncNewColumns
	}
	metadata.SelectedColumns = &SelectedColumns{
		Columns:        columns,
		SyncNewColumns: syncNewColumns,
	}
}

// IsSelectedColumn returns a function that checks if a column is selected
func IsSelectedColumn(stream StreamInterface) func(string) bool {
	selectedColsCfg := stream.Self().StreamMetadata.SelectedColumns
	if selectedColsCfg == nil || len(selectedColsCfg.Columns) == 0 {
		return func(string) bool { return true }
	}

	if selectedColsCfg.SyncNewColumns {
		unselected := GetUnSelectedColumnsSet(selectedColsCfg.Columns, stream.GetStream())
		return func(col string) bool {
			return !unselected.Exists(col)
		}
	}

	selected := NewSet(selectedColsCfg.Columns...)
	return func(col string) bool {
		return selected.Exists(col)
	}
}
