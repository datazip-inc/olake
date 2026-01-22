package types

import (
	"github.com/datazip-inc/olake/constants"
)

type SelectedColumns struct {
	Columns     []string            `json:"columns"`
	Map         map[string]struct{} `json:"-"`
	AllSelected bool                `json:"-"`
}

// GetSelectedColumns returns the selected columns
func (sc *SelectedColumns) GetSelectedColumns() []string {
	return sc.Columns
}

// setSelectedColumnsMap sets the selected columns map for the selected columns
func (sc *SelectedColumns) setSelectedColumnsMap() {
	sc.Map = make(map[string]struct{}, len(sc.Columns))
	for _, col := range sc.Columns {
		sc.Map[col] = struct{}{}
	}
}

// GetSelectedColumnsMap returns the selected columns map
func (sc *SelectedColumns) GetSelectedColumnsMap() map[string]struct{} {
	return sc.Map
}

// SetAllSelectedColumnsFlag sets the all selected flag for the selected columns
func (sc *SelectedColumns) SetAllSelectedColumnsFlag(newStream *Stream) {
	sc.AllSelected = sc.checkAllColumnsSelected(newStream)
}

// GetAllSelectedColumnsFlag returns the all selected flag for the selected columns
func (sc *SelectedColumns) GetAllSelectedColumnsFlag() bool {
	return sc.AllSelected
}

// checkAllColumnsSelected checks if all columns in the schema are selected by the user
// Returns true if all columns are selected or no columns are selected, otherwise returns false
func (sc *SelectedColumns) checkAllColumnsSelected(newStream *Stream) bool {
	selectedMap := sc.GetSelectedColumnsMap()
	if len(selectedMap) == 0 {
		return true
	}

	var (
		schemaColumnCount int
	)

	newStream.Schema.Properties.Range(func(key, _ interface{}) bool {
		schemaColumnCount++

		colName, ok := key.(string)
		if !ok {
			return false
		}

		if _, exists := selectedMap[colName]; !exists {
			return false
		}

		return true
	})

	return len(selectedMap) == schemaColumnCount
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

// collectColumnsFromSchema collects all columns from a schema
func collectColumnsFromSchema(schema *TypeSchema) []string {
	var columns []string
	schema.Properties.Range(func(key, _ interface{}) bool {
		if colName, ok := key.(string); ok {
			columns = append(columns, colName)
		}
		return true
	})
	return columns
}

// schemasHaveSameColumns checks if two schemas have the same columns
// Returns true if both schemas have identical column sets (ignoring order)
func schemasHaveSameColumns(oldSchema, newSchema *TypeSchema) bool {
	oldSchemaColumns := collectColumnsFromSchema(oldSchema)
	newSchemaColumns := collectColumnsFromSchema(newSchema)

	if len(oldSchemaColumns) != len(newSchemaColumns) {
		return false
	}

	oldSchemaColumnsMap := make(map[string]struct{})
	for _, col := range oldSchemaColumns {
		oldSchemaColumnsMap[col] = struct{}{}
	}

	for _, col := range newSchemaColumns {
		if _, exists := oldSchemaColumnsMap[col]; !exists {
			return false
		}
	}

	return true
}

// getColumnsDelta compares oldSchema and newSchema to identify column differences.
// Returns common columns (present in both) and newly added columns (only in newSchema).
func getColumnsDelta(oldSchema, newSchema *TypeSchema) ([]string, []string) {
	var (
		common   []string
		newAdded []string
	)

	newSchema.Properties.Range(func(k, _ interface{}) bool {
		col := k.(string)

		if _, exists := oldSchema.Properties.Load(col); exists {
			common = append(common, col)
		} else {
			newAdded = append(newAdded, col)
		}
		return true
	})

	return common, newAdded
}

// MergeSelectedColumns merges selected columns with newly discovered columns based on SyncNewColumns flag. It:
// 1. when selectedColumns property is not present or empty, use all columns from new schema or only columns that existed in old schema
// 2. when selectedColumns property is present and not empty, filter selected columns to only those present in both old and new schemas
// 3. if the old and new schemas have same columns, so no need to check for presence in both old and new schemas
// 4. add newly discovered columns if SyncNewColumns is true
// 5. ensure mandatory columns are included
// 6. set the all selected flag
// 7. set the selected columns map
func MergeSelectedColumns(
	metadata *StreamMetadata,
	oldStream *Stream,
	newStream *Stream,
) {
	oldSchema := oldStream.Schema
	newSchema := newStream.Schema

	finalizeSelectedColumns := func() {
		// set the selected columns map
		metadata.SelectedColumns.setSelectedColumnsMap()
		// ensure mandatory columns are included
		metadata.SelectedColumns.ensureMandatoryColumns(oldStream, newStream)
		// set the all selected flag
		metadata.SelectedColumns.SetAllSelectedColumnsFlag(newStream)
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

	// if the old and new schemas have same columns, so no need to check for presence in both old and new schemas
	// and call ensureMandatoryColumns to ensure mandatory columns are included
	if schemasHaveSameColumns(oldSchema, newSchema) {
		finalizeSelectedColumns()
		return
	}

	// when selectedColumns property is present and not empty
	var preservedSelectedColumns []string
	preservedSelectedColumnsMap := make(map[string]struct{})
	for _, previouslySelectedCol := range metadata.SelectedColumns.Columns {
		// check if the column exists in both old and new schemas
		_, existsInOld := oldSchema.Properties.Load(previouslySelectedCol)
		_, existsInNew := newSchema.Properties.Load(previouslySelectedCol)
		if existsInOld && existsInNew {
			// don't add duplicate columns
			if _, exists := preservedSelectedColumnsMap[previouslySelectedCol]; !exists {
				preservedSelectedColumns = append(preservedSelectedColumns, previouslySelectedCol)
				preservedSelectedColumnsMap[previouslySelectedCol] = struct{}{}
			}
		}
	}

	metadata.SelectedColumns = &SelectedColumns{
		Columns: preservedSelectedColumns,
	}

	// add newly discovered columns if SyncNewColumns is true
	if metadata.SyncNewColumns {
		_, newAddedColumns := getColumnsDelta(oldSchema, newSchema)
		if len(newAddedColumns) > 0 {
			metadata.SelectedColumns.Columns = append(metadata.SelectedColumns.Columns, newAddedColumns...)
		}
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
		for _, pk := range newStream.SourceDefinedPrimaryKey.Array() {
			if _, exists := selectedMap[pk]; !exists {
				sc.Columns = append(sc.Columns, pk)
				selectedMap[pk] = struct{}{}
			}
		}
	}
	return selectedMap
}
