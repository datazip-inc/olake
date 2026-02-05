package types

import (
	"github.com/datazip-inc/olake/constants"
)

// Config represents the user defined inputs
// these are persisted in the catalog
type Config struct {
	Columns        []string `json:"columns"`
	SyncNewColumns bool     `json:"sync_new_columns"`
}

// Runtime represents the runtime state which is derived from the config
type Runtime struct {
	SelectedColumnsSet   *Set[string] `json:"-"`
	UnSelectedColumnsSet *Set[string] `json:"-"`
	AllColumnsSelected   bool         `json:"-"`
}

// setSelectedColumnsSet sets the selected columns set
// if no columns are selected, it means all columns are selected
func (r *Runtime) setSelectedColumnsSet(config *Config) {
	r.SelectedColumnsSet = NewSet[string](config.Columns...)
}

// setUnSelectedColumnsSet sets the unselected columns set
func (r *Runtime) setUnSelectedColumnsSet(config *Config, oldStream *Stream) {
	if len(config.Columns) == 0 {
		r.UnSelectedColumnsSet = NewSet[string]()
		return
	}

	oldSchemaColumnsSet := collectColumnsFromSchemaAsSet(oldStream.Schema)
	r.UnSelectedColumnsSet = oldSchemaColumnsSet.Difference(r.SelectedColumnsSet)
}

// SetAllSelectedColumnsFlag sets the all selected flag
// Return true if no columns are selected or no columns are unselected
// Return false if some columns are selected and some columns are unselected
func (r *Runtime) SetAllSelectedColumnsFlag(config *Config) {
	if len(config.Columns) == 0 || r.UnSelectedColumnsSet.Len() == 0 {
		r.AllColumnsSelected = true
		return
	}

	r.AllColumnsSelected = false
}

type SelectedColumns struct {
	Config  Config  `json:"config"`
	Runtime Runtime `json:"-"`
}

// GetSelectedColumns returns the selected columns
func (sc *SelectedColumns) GetSelectedColumns() []string {
	return sc.Config.Columns
}

// GetSelectedColumnsSet returns the selected columns set
func (sc *SelectedColumns) GetSelectedColumnsSet() *Set[string] {
	return sc.Runtime.SelectedColumnsSet
}

// GetUnSelectedColumnsSet returns the unselected columns set
func (sc *SelectedColumns) GetUnSelectedColumnsSet() *Set[string] {
	return sc.Runtime.UnSelectedColumnsSet
}

// GetAllSelectedColumnsFlag returns the all selected flag
func (sc *SelectedColumns) GetAllSelectedColumnsFlag() bool {
	return sc.Runtime.AllColumnsSelected
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
	syncMode := stream.GetSyncMode()
	selectedColumns := stream.Self().StreamMetadata.SelectedColumns
	selectedColumnsSet := selectedColumns.GetSelectedColumnsSet()
	unselectedColumnsSet := selectedColumns.GetUnSelectedColumnsSet()
	syncNewColumns := selectedColumns.Config.SyncNewColumns
	allSelected := selectedColumns.GetAllSelectedColumnsFlag()
	isCDC := syncMode == CDC || syncMode == STRICTCDC

	return func(data map[string]interface{}) map[string]interface{} {
		// Skip filtering when all columns are selected and (not CDC, or CDC with sync_new_columns).
		if allSelected && (!isCDC || syncNewColumns) {
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
	return NewSet[string](columns...)
}

// collectColumnsFromSchema collects all columns from a schema
func collectColumnsFromSchema(schema *TypeSchema) []string {
	return collectColumnsFromSchemaAsSet(schema).Array()
}

// MergeSelectedColumns merges the selected columns based on the following rules:
// 1. if selectedColumns property is not present or empty, initialize with columns from new schema
// 2. if selectedColumns property is present and not empty, filter the selected columns to only those present in both old and new schemas
// 3. if sync_new_columns is true, add newly discovered columns to the selected columns
// 4. ensure mandatory columns are included
// 5. set the selected columns set
// 6. set the unselected columns set
// 7. set the all selected flag
func MergeSelectedColumns(
	metadata *StreamMetadata,
	oldStream *Stream,
	newStream *Stream,
) {
	oldSchema := oldStream.Schema
	newSchema := newStream.Schema
	oldSchemaColumnsSet := collectColumnsFromSchemaAsSet(oldSchema)
	newSchemaColumnsSet := collectColumnsFromSchemaAsSet(newSchema)

	finalizeSelectedColumns := func() {
		metadata.SelectedColumns.Runtime.setSelectedColumnsSet(&metadata.SelectedColumns.Config)
		metadata.SelectedColumns.Runtime.setUnSelectedColumnsSet(&metadata.SelectedColumns.Config, oldStream)
		metadata.SelectedColumns.Runtime.SetAllSelectedColumnsFlag(&metadata.SelectedColumns.Config)
	}

	// when selectedColumns property is not present or empty, initialize with columns
	// default behavior is OFF (sync_new_columns = false)
	// - If sync_new_columns is true: sync all columns from new schema
	// - If sync_new_columns is false: sync only columns that existed in old schema
	// However, if oldSchema is empty or incomplete (backward compatibility), default to all columns from newSchema
	if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Config.Columns) == 0 {
		syncNewColumns := false
		if metadata.SelectedColumns != nil {
			syncNewColumns = metadata.SelectedColumns.Config.SyncNewColumns
		}

		// if oldSchema is empty or sync_new_columns is true, use all columns from new schema
		// ensures backward compatibility when selected_columns was not previously specified
		if oldSchemaColumnsSet.Len() == 0 || syncNewColumns {
			metadata.SelectedColumns = &SelectedColumns{
				Config: Config{
					Columns:        newSchemaColumnsSet.Array(),
					SyncNewColumns: syncNewColumns,
				},
			}
		} else {
			// default behavior: select only columns that existed in old schema
			metadata.SelectedColumns = &SelectedColumns{
				Config: Config{
					Columns:        oldSchemaColumnsSet.Array(),
					SyncNewColumns: syncNewColumns,
				},
			}
		}
		finalizeSelectedColumns()
		return
	}

	previouslySelectedColumnsSet := NewSet[string](metadata.SelectedColumns.Config.Columns...)

	// preserves previously selected columns that exist in both old and new schemas
	preservedColumnsSet := previouslySelectedColumnsSet.Intersection(oldSchemaColumnsSet).Intersection(newSchemaColumnsSet)

	var newSelectedColumnsSet *Set[string]
	if metadata.SelectedColumns.Config.SyncNewColumns {
		// add newly discovered columns when sync_new_columns is true
		newColumnsSet := newSchemaColumnsSet.Difference(oldSchemaColumnsSet)
		newSelectedColumnsSet = preservedColumnsSet.Union(newColumnsSet)
	} else {
		newSelectedColumnsSet = preservedColumnsSet
	}

	metadata.SelectedColumns = &SelectedColumns{
		Config: Config{
			Columns:        newSelectedColumnsSet.Array(),
			SyncNewColumns: metadata.SelectedColumns.Config.SyncNewColumns,
		},
	}

	finalizeSelectedColumns()
}

// ComputeMandatoryColumns computes the mandatory columns for a stream based on:
// 1. System generated fields (OlakeID, OlakeTimestamp, OpType)
// 2. Cursor fields (if incremental sync)
// 3. CDC Timestamp (if CDC sync mode)
// 4. Source defined primary key columns
func ComputeMandatoryColumns(oldStream, newStream *Stream) []string {
	mandatoryColumnsSet := NewSet[string]()

	// Add system generated fields
	systemFields := []string{constants.OlakeID, constants.OlakeTimestamp, constants.OpType, constants.CdcTimestamp}
	mandatoryColumnsSet.Insert(systemFields...)

	// Remove CDC Timestamp if not CDC sync mode
	if oldStream.SyncMode != CDC && oldStream.SyncMode != STRICTCDC {
		mandatoryColumnsSet.Remove(constants.CdcTimestamp)
	}

	// Add cursor fields for incremental sync
	if oldStream.SyncMode == INCREMENTAL && oldStream.CursorField != "" {
		primaryCursor, secondaryCursor := parseCursorField(oldStream.CursorField)
		if primaryCursor != "" {
			mandatoryColumnsSet.Insert(primaryCursor)
		}
		if secondaryCursor != "" {
			mandatoryColumnsSet.Insert(secondaryCursor)
		}
	}

	// Add source defined primary key columns
	if newStream.SourceDefinedPrimaryKey != nil {
		mandatoryColumnsSet.Insert(newStream.SourceDefinedPrimaryKey.Array()...)
	}

	return mandatoryColumnsSet.Array()
}
