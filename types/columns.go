package types

import (
	"github.com/datazip-inc/olake/constants"
)

type SelectedColumns struct {
	Columns            []string            `json:"columns"` // all columns that are selected
	SelectedMap        map[string]struct{} `json:"-"`       // map of columns that are selected
	UnSelectedMap      map[string]struct{} `json:"-"`       // map of columns that are unselected
	AllColumnsSelected bool                `json:"-"`       // true if all columns in the schema are selected
}

// GetSelectedColumns returns the selected columns
func (sc *SelectedColumns) GetSelectedColumns() []string {
	return sc.Columns
}

// setSelectedColumnsMap sets the selected columns map
// if no columns are selected, it means all columns are selected
func (sc *SelectedColumns) setSelectedColumnsMap() {
	sc.SelectedMap = make(map[string]struct{}, len(sc.Columns))

	if len(sc.Columns) == 0 {
		return
	}

	for _, col := range sc.Columns {
		sc.SelectedMap[col] = struct{}{}
	}
}

// setUnSelectedColumnsMap sets the unselected columns map
func (sc *SelectedColumns) setUnSelectedColumnsMap(oldStream *Stream) {
	sc.UnSelectedMap = make(map[string]struct{})

	if len(sc.Columns) == 0 {
		return
	}

	oldStream.Schema.Properties.Range(func(col, _ interface{}) bool {
		colName, isColTypeString := col.(string)
		if !isColTypeString {
			return true
		}
		// add to UnSelectedMap if column exists in old schema and is not selected
		if _, exists := sc.SelectedMap[colName]; !exists {
			sc.UnSelectedMap[colName] = struct{}{}
		}
		return true
	})
}

// GetSelectedColumnsMap returns the selected columns map
func (sc *SelectedColumns) GetSelectedColumnsMap() map[string]struct{} {
	return sc.SelectedMap
}

// GetUnSelectedColumnsMap returns the unselected columns map
func (sc *SelectedColumns) GetUnSelectedColumnsMap() map[string]struct{} {
	return sc.UnSelectedMap
}

// SetAllSelectedColumnsFlag sets the all selected flag
// Return true if no columns are selected or no columns are unselected
// Return false if some columns are selected and some columns are unselected
func (sc *SelectedColumns) SetAllSelectedColumnsFlag() {
	if len(sc.Columns) == 0 || len(sc.UnSelectedMap) == 0 {
		sc.AllColumnsSelected = true
		return
	}

	sc.AllColumnsSelected = false
}

// GetAllSelectedColumnsFlag returns the all selected flag
func (sc *SelectedColumns) GetAllSelectedColumnsFlag() bool {
	return sc.AllColumnsSelected
}

// FilterDataBySelectedColumns filters data based on the following rules:
// - sync_new_columns=true:
//   - Specific columns selected: Only selected columns sync; newly added columns are automatically included
//   - All columns selected: All columns sync, including newly added columns.
//
// - sync_new_columns=false:
//   - Specific columns selected: Only explicitly selected columns sync
//   - All columns selected: All existing columns sync; newly added columns are NOT synced
func FilterDataBySelectedColumns(data map[string]interface{}, stream StreamInterface) map[string]interface{} {
	selectedCols := stream.Self().StreamMetadata.SelectedColumns
	syncNewColumns := stream.Self().StreamMetadata.SyncNewColumns
	allSelected := selectedCols.GetAllSelectedColumnsFlag()
	streamSyncMode := stream.GetSyncMode()
	isCDC := streamSyncMode == CDC || streamSyncMode == STRICTCDC

	if allSelected {
		if !isCDC {
			return data
		}

		if isCDC && syncNewColumns {
			return data
		}
	}

	filtered := make(map[string]interface{})
	if syncNewColumns {
		// emit all columns except those that are unselected
		// this ensures all columns that are new are selected by default
		unSelectedMap := selectedCols.GetUnSelectedColumnsMap()
		for col, value := range data {
			if _, excluded := unSelectedMap[col]; !excluded {
				filtered[col] = value
			}
		}
	} else {
		// emit only columns that are selected
		selectedMap := selectedCols.GetSelectedColumnsMap()
		for col, value := range data {
			if _, exists := selectedMap[col]; exists {
				filtered[col] = value
			}
		}
	}
	return filtered
}

// collectColumnsFromSchema collects all columns from a schema
func collectColumnsFromSchema(schema *TypeSchema) []string {
	columns := []string{}
	schema.Properties.Range(func(col, _ interface{}) bool {
		if colName, isColTypeString := col.(string); isColTypeString {
			columns = append(columns, colName)
		}
		return true
	})
	return columns
}

// MergeSelectedColumns merges the selected columns based on the following rules:
// 1. if selectedColumns property is not present or empty, initialize with columns from new schema
// 2. if selectedColumns property is present and not empty, filter the selected columns to only those present in both old and new schemas
// 3. if sync_new_columns is true, add newly discovered columns to the selected columns
// 4. ensure mandatory columns are included
// 5. set the selected columns map
// 6. set the unselected columns map
// 7. set the all selected flag
func MergeSelectedColumns(
	metadata *StreamMetadata,
	oldStream *Stream,
	newStream *Stream,
) {
	oldSchema := oldStream.Schema
	newSchema := newStream.Schema

	finalizeSelectedColumns := func() {
		metadata.SelectedColumns.setSelectedColumnsMap()
		metadata.SelectedColumns.ensureMandatoryColumns(oldStream, newStream)
		metadata.SelectedColumns.setUnSelectedColumnsMap(oldStream)
		metadata.SelectedColumns.SetAllSelectedColumnsFlag()
	}

	// when selectedColumns property is not present or empty, initialize with columns
	// default behavior is OFF (sync_new_columns = false)
	// - If sync_new_columns is true: sync all columns from new schema
	// - If sync_new_columns is false: sync only columns that existed in old schema
	// However, if oldSchema is empty or incomplete (backward compatibility), default to all columns from newSchema
	if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Columns) == 0 {
		oldSchemaColumns := collectColumnsFromSchema(oldSchema)
		newSchemaColumns := collectColumnsFromSchema(newSchema)

		// if oldSchema is empty or sync_new_columns is true, use all columns from new schema
		// ensures backward compatibility when selected_columns was not previously specified
		if len(oldSchemaColumns) == 0 || metadata.SyncNewColumns {
			metadata.SelectedColumns = &SelectedColumns{
				Columns: newSchemaColumns,
			}
		} else {
			// default behavior: select only columns that existed in old schema
			metadata.SelectedColumns = &SelectedColumns{
				Columns: oldSchemaColumns,
			}
		}
		finalizeSelectedColumns()
		return
	}

	var newSelectedColumns []string

	previouslySelectedColumnsMap := make(map[string]struct{})
	for _, col := range metadata.SelectedColumns.Columns {
		// prevent duplicate columns
		if _, exists := previouslySelectedColumnsMap[col]; !exists {
			previouslySelectedColumnsMap[col] = struct{}{}
		}
	}

	newSchema.Properties.Range(func(col, _ interface{}) bool {
		colName, isColTypeString := col.(string)
		if !isColTypeString {
			return true
		}

		_, existsInOld := oldSchema.Properties.Load(colName)
		_, wasPreviouslySelected := previouslySelectedColumnsMap[colName]

		// preserves previously selected columns that exist in both old and new schemas
		if wasPreviouslySelected && existsInOld {
			newSelectedColumns = append(newSelectedColumns, colName)
		}

		// add newly discovered columns when sync_new_columns is true
		if metadata.SyncNewColumns && !existsInOld {
			newSelectedColumns = append(newSelectedColumns, colName)
		}

		return true
	})

	metadata.SelectedColumns = &SelectedColumns{
		Columns: newSelectedColumns,
	}

	finalizeSelectedColumns()
}

// ensureMandatoryColumns ensures that mandatory columns are always in SelectedColumns:
// 1. cursor fields,
// 2. CDC columns,
// 3. source defined primary key columns,
// 4. system generated fields
func (sc *SelectedColumns) ensureMandatoryColumns(oldStream, newStream *Stream) map[string]struct{} {
	selectedMap := sc.GetSelectedColumnsMap()

	// Add system generated fields
	systemFields := []string{constants.OlakeID, constants.OlakeTimestamp, constants.OpType}
	for _, sysField := range systemFields {
		if _, exists := selectedMap[sysField]; !exists {
			sc.Columns = append(sc.Columns, sysField)
			selectedMap[sysField] = struct{}{}
		}
	}

	// Add cursor fields for incremental sync
	if oldStream.SyncMode == INCREMENTAL && oldStream.CursorField != "" {
		primaryCursor, secondaryCursor := parseCursorField(oldStream.CursorField)
		if primaryCursor != "" {
			if _, exists := selectedMap[primaryCursor]; !exists {
				sc.Columns = append(sc.Columns, primaryCursor)
				selectedMap[primaryCursor] = struct{}{}
			}
		}
		if secondaryCursor != "" {
			if _, exists := selectedMap[secondaryCursor]; !exists {
				sc.Columns = append(sc.Columns, secondaryCursor)
				selectedMap[secondaryCursor] = struct{}{}
			}
		}
	}

	// Add CDC columns if CDC sync mode
	if oldStream.SyncMode == CDC || oldStream.SyncMode == STRICTCDC {
		if _, exists := selectedMap[constants.CdcTimestamp]; !exists {
			sc.Columns = append(sc.Columns, constants.CdcTimestamp)
			selectedMap[constants.CdcTimestamp] = struct{}{}
		}
	}

	// Add source defined primary key columns
	if newStream.SourceDefinedPrimaryKey != nil {
		for _, primaryKey := range newStream.SourceDefinedPrimaryKey.Array() {
			if _, exists := selectedMap[primaryKey]; !exists {
				sc.Columns = append(sc.Columns, primaryKey)
				selectedMap[primaryKey] = struct{}{}
			}
		}
	}
	return selectedMap
}
