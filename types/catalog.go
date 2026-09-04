package types

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

// Message is a dto for olake output row representation
type Message struct {
	Type             MessageType            `json:"type"`
	Log              *Log                   `json:"log,omitempty"`
	ConnectionStatus *StatusRow             `json:"connectionStatus,omitempty"`
	State            *State                 `json:"state,omitempty"`
	Catalog          *Catalog               `json:"catalog,omitempty"`
	Action           *ActionRow             `json:"action,omitempty"`
	Spec             map[string]interface{} `json:"spec,omitempty"`
}

type ActionRow struct {
	// Type Action `json:"type"`
	// Add alter
	// add create
	// add drop
	// add truncate
}

type Log struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

type StatusRow struct {
	Status  ConnectionStatus `json:"status,omitempty"`
	Message string           `json:"message,omitempty"`
}

// SelectedColumns represents column selection configuration for a stream.
// - columns: explicit list of columns (empty means "all")
// - sync_new_columns: if true, newly discovered columns are included by default
type SelectedColumns struct {
	Columns        []string `json:"columns"`
	SyncNewColumns bool     `json:"sync_new_columns"`
}

type StreamMetadata struct {
	ChunkColumn    string `json:"chunk_column,omitempty"`
	PartitionRegex string `json:"partition_regex"`
	StreamName     string `json:"stream_name"`
	AppendMode     *bool  `json:"append_mode,omitempty"`
	Normalization  *bool  `json:"normalization,omitempty"`
	UpdateType     string `json:"update_type,omitempty"`
	// When enabled, source column names are preserved as-is; otherwise utils.Reformat() is applied to generate destination-safe lowercase column names.
	UseSourceColumnNames bool `json:"use_source_column_names,omitempty"`
	//legacy filter input
	Filter string `json:"filter,omitempty"`
	//new filter input
	FilterConfig        *FilterConfig    `json:"filter_config,omitempty"`
	SelectedColumns     *SelectedColumns `json:"selected_columns,omitempty"`
	SyncMode            SyncMode         `json:"sync_mode,omitempty"`
	CursorField         string           `json:"cursor_field,omitempty"`
	DestinationDatabase string           `json:"destination_database,omitempty"`
	DestinationTable    string           `json:"destination_table,omitempty"`
}

// resolveConfigurableField returns the first non-zero value.
// Callers pass values in priority order: selected_streams, then streams[].
func resolveConfigurableField[T comparable](values ...T) T {
	var zero T
	for _, v := range values {
		if v != zero {
			return v
		}
	}
	return zero
}

type Catalog struct {
	SelectedStreams map[string][]StreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream         `json:"streams,omitempty"`
}

// StreamMix is the per-sync breakdown of the streams a run actually syncs. Only streams that
// survived selection and validation are counted, so the sync-mode counters sum to Selected.
type StreamMix struct {
	FullRefresh             int `json:"full_refresh_streams_count"`
	Incremental             int `json:"incremental_streams_count"`
	CDC                     int `json:"cdc_streams_count"`
	StrictCDC               int `json:"strict_cdc_streams_count"`
	Selected                int `json:"selected_streams_count"`
	Normalized              int `json:"normalized_streams_count"`
	Partitioned             int `json:"partitioned_streams_count"`
	StreamWithPosUpdateType int `json:"stream_with_pos_update_type_count"`
}

// ResolveCatalog loads a catalog from disk, handling both the default (combined) and
// split (--selected-streams) file layouts.
//
// Default layout: streams.json contains both streams[] and selected_streams.
// This is the normal output of discover and is returned as-is when selectedStreamsFilePath is empty.
//
// Split layout (opt-in via --selected-streams): streams.json contains streams[];
// selected_streams lives in a separate selected_streams.json. When selectedStreamsFilePath is
// set, that file's selected_streams overlay the catalog loaded from streams.json.
func ResolveCatalog(streamsFilePath, selectedStreamsFilePath string) (*Catalog, error) {
	catalog := &Catalog{}
	if err := utils.UnmarshalFile(streamsFilePath, catalog, false); err != nil {
		return nil, fmt.Errorf("failed to read streams from %s: %w", streamsFilePath, err)
	}

	if selectedStreamsFilePath != "" {
		selectedCatalog := &Catalog{}
		if err := utils.UnmarshalFile(selectedStreamsFilePath, selectedCatalog, false); err != nil {
			return nil, fmt.Errorf("failed to read selected_streams from %s: %w", selectedStreamsFilePath, err)
		}
		if len(selectedCatalog.SelectedStreams) == 0 {
			return nil, fmt.Errorf("selected_streams file %s has no selected_streams", selectedStreamsFilePath)
		}
		catalog.SelectedStreams = selectedCatalog.SelectedStreams
	}

	if len(catalog.Streams) == 0 && len(catalog.SelectedStreams) > 0 {
		return nil, fmt.Errorf("streams file %s has selected_streams but no streams[]", streamsFilePath)
	}

	return catalog, nil
}

// splitCatalogForWrite returns two Catalog values for the opt-in split file layout:
// streams[] only, and selected_streams only.
func splitCatalogForWrite(catalog *Catalog) (streamsFile, selectedStreamsFile *Catalog) {
	return &Catalog{Streams: catalog.Streams}, &Catalog{SelectedStreams: catalog.SelectedStreams}
}

func GetWrappedCatalog(streams []*Stream, _ string) *Catalog {
	catalog := &Catalog{
		Streams:         []*ConfiguredStream{},
		SelectedStreams: make(map[string][]StreamMetadata),
	}

	for _, stream := range streams {
		stream.RefreshSelectableColumns()
		catalog.Streams = append(catalog.Streams, &ConfiguredStream{
			Stream: stream,
		})

		metadata := StreamMetadata{
			StreamName:     stream.Name,
			PartitionRegex: "",
		}
		if stream.SyncMode == INCREMENTAL {
			metadata.CursorField = stream.CursorField
		}
		catalog.SelectedStreams[stream.Namespace] = append(catalog.SelectedStreams[stream.Namespace], metadata)
	}

	return catalog
}

// MergeCatalogs merges old catalog with new catalog based on the following rules:
// 1. SelectedStreams: Retain only streams present in both oldCatalog.SelectedStreams and newStreamMap
// 2. SelectedColumns: Retain columns present in both old and new schemas, add NEW columns if sync_new_columns is true
// 3. SyncMode: Use from oldCatalog if the stream exists in old catalog
// 4. Everything else: Keep as new catalog
func mergeCatalogs(oldCatalog, newCatalog *Catalog) *Catalog {
	if oldCatalog == nil {
		return newCatalog
	}

	createStreamMap := func(catalog *Catalog) map[string]*ConfiguredStream {
		streamMap := make(map[string]*ConfiguredStream)
		for _, stream := range catalog.Streams {
			streamMap[stream.Stream.ID()] = stream
		}
		return streamMap
	}

	oldStreams := createStreamMap(oldCatalog)

	// merge selected streams
	if oldCatalog.SelectedStreams != nil {
		newStreams := createStreamMap(newCatalog)
		selectedStreams := make(map[string][]StreamMetadata)

		for namespace, metadataList := range oldCatalog.SelectedStreams {
			_ = utils.ForEach(metadataList, func(metadata StreamMetadata) error {
				streamID := fmt.Sprintf("%s.%s", namespace, metadata.StreamName)
				_, exists := newStreams[streamID]

				if exists {
					oldConfigured := oldStreams[streamID]
					newStream := newStreams[streamID].Stream
					if oldConfigured != nil {
						MergeSelectedColumns(&metadata, oldConfigured.Stream, newStream)
					}

					selectedStreams[namespace] = append(selectedStreams[namespace], metadata)
				}
				return nil
			})
		}
		newCatalog.SelectedStreams = selectedStreams
	}

	constantValue, prefix := getDestDBPrefix(oldCatalog.Streams)

	// merge streams metadata
	_ = utils.ForEach(newCatalog.Streams, func(newStream *ConfiguredStream) error {
		oldStream, exists := oldStreams[newStream.Stream.ID()]
		if exists {
			newStream.Stream.SyncMode = oldStream.Stream.SyncMode
			if oldStream.Stream.CursorField != "" {
				newStream.Stream.CursorField = oldStream.Stream.CursorField
			}
			newStream.Stream.DestinationDatabase = oldStream.Stream.DestinationDatabase
			newStream.Stream.DestinationTable = oldStream.Stream.DestinationTable
			newStream.Stream.SourceDefinedPrimaryKey = oldStream.Stream.SourceDefinedPrimaryKey
			return nil
		}

		// NOTE: new streams are not added to selected_streams, user needs to manually enable them
		// manipulate destination db in new streams according to old streams

		// prefix == "" means old stream when db normalization feature not introduced
		if constantValue {
			newStream.Stream.DestinationDatabase = oldCatalog.Streams[0].Stream.DestinationDatabase
		} else if prefix != "" {
			newStream.Stream.DestinationDatabase = fmt.Sprintf("%s:%s", prefix, utils.Reformat(newStream.Stream.Namespace))
		}

		return nil
	})

	return newCatalog
}

// MergeSelectedColumns updates an existing selected_columns list against the new schema.
// If selected_columns is absent or has no columns, it is left unset so sync keeps all columns.
// Otherwise previously selected columns are preserved, OLake columns are always kept, and
// newly discovered columns are added when sync_new_columns is true.
func MergeSelectedColumns(metadata *StreamMetadata, oldStream *Stream, newStream *Stream) {
	if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Columns) == 0 {
		return
	}

	var columns []string
	previouslySelectedSet := NewSet(metadata.SelectedColumns.Columns...)
	oldSchemaCols := NewSet(oldStream.Schema.ColumnNames()...)

	newStream.Schema.Properties.Range(func(key, value interface{}) bool {
		col, ok := key.(string)
		if !ok {
			return true
		}
		prop := value.(*Property)
		if prop.OlakeColumn || previouslySelectedSet.Exists(col) || (metadata.SelectedColumns.SyncNewColumns && !oldSchemaCols.Exists(col)) {
			columns = append(columns, col)
		}
		return true
	})

	metadata.SelectedColumns = &SelectedColumns{
		Columns:        columns,
		SyncNewColumns: metadata.SelectedColumns.SyncNewColumns,
	}
}

// getDestDBPrefix analyzes a collection of streams to determine if they share a common
// destination database prefix or constant value.
//
// The function checks if all streams have the same:
// - Destination database prefix (e.g., "PREFIX:table_name") OR
// - Constant database name (e.g., "CONSTANT_DB_NAME")
// Returns:
//
//	bool: true if the common value is a constant (no colon present),
//	      false if it's a prefix (colon present in original string)
//	string: the common prefix or constant value, or empty string if no common value exists
func getDestDBPrefix(streams []*ConfiguredStream) (constantValue bool, prefix string) {
	if len(streams) == 0 {
		return false, ""
	}

	prefixOrConstValue := strings.Split(streams[0].Stream.DestinationDatabase, ":")
	for _, s := range streams {
		streamDBPrefixOrConstValue := strings.Split(s.Stream.DestinationDatabase, ":")
		if streamDBPrefixOrConstValue[0] != prefixOrConstValue[0] {
			// Not all same → bail out
			return false, ""
		}
	}

	return len(prefixOrConstValue) == 1, prefixOrConstValue[0]
}

// GetStreamsDelta compares two catalogs and returns a new catalog with streams that have differences.
// Only selected streams are compared.
// 1. Compares properties from selected_streams: normalization, partition_regex, filter, append_mode, use_source_column_names
// 2. Compares properties from streams: destination_database, destination_table, cursor_field, sync_mode
// 3. For now, any new stream present in new catalog is added to the difference. Later collision detection will happen.
//
// Parameters:
//   - oldStreams: The previous catalog to compare against
//   - newStreams: The current catalog with potential changes
//
// Returns:
//   - A catalog containing only the streams that have differences
func GetStreamsDelta(oldStreams, newStreams *Catalog) *Catalog {
	diffStreams := &Catalog{
		Streams:         []*ConfiguredStream{},
		SelectedStreams: make(map[string][]StreamMetadata),
	}

	oldStreamsMap := make(map[string]*ConfiguredStream)
	for _, stream := range oldStreams.Streams {
		oldStreamsMap[stream.ID()] = stream
	}

	newStreamsMap := make(map[string]*ConfiguredStream)
	for _, stream := range newStreams.Streams {
		newStreamsMap[stream.ID()] = stream
	}

	oldSelectedMap := make(map[string]StreamMetadata)
	for namespace, metadatas := range oldStreams.SelectedStreams {
		for _, metadata := range metadatas {
			oldSelectedMap[fmt.Sprintf("%s.%s", namespace, metadata.StreamName)] = metadata
		}
	}

	for namespace, newMetadatas := range newStreams.SelectedStreams {
		for _, newMetadata := range newMetadatas {
			streamID := fmt.Sprintf("%s.%s", namespace, newMetadata.StreamName)

			// new stream definition from streams array
			newStream, newStreamExists := newStreamsMap[streamID]
			if !newStreamExists {
				continue
			}

			// Check if this stream existed in old catalog
			oldMetadata, oldMetadataExists := oldSelectedMap[streamID]
			oldStream, oldStreamExists := oldStreamsMap[streamID]

			// if new stream in selected_streams
			if !oldMetadataExists || !oldStreamExists {
				// addition of new streams
				diffStreams.Streams = append(diffStreams.Streams, newStream)
				diffStreams.SelectedStreams[namespace] = append(
					diffStreams.SelectedStreams[namespace],
					newMetadata,
				)
				continue
			}

			// Stream exists in both catalogs - check for differences
			// normalization difference
			// partition regex difference
			// filter difference
			// append mode change
			// destination database change
			// cursor field change , Format: "primary_cursor:secondary_cursor"
			// sync mode change
			// destination table change

			// NOTE: we are not droping table if there is delete mode change
			// TODO: log the differences for user reference
			isDifferent := func() bool {
				oldConfigured := &ConfiguredStream{Stream: oldStream.Stream, StreamMetadata: oldMetadata}
				newConfigured := &ConfiguredStream{Stream: newStream.Stream, StreamMetadata: newMetadata}

				oldSyncMode := resolveConfigurableField(oldMetadata.SyncMode, oldStream.Stream.SyncMode)
				newSyncMode := resolveConfigurableField(newMetadata.SyncMode, newStream.Stream.SyncMode)
				oldCursorField := resolveConfigurableField(oldMetadata.CursorField, oldStream.Stream.CursorField)
				newCursorField := resolveConfigurableField(newMetadata.CursorField, newStream.Stream.CursorField)
				oldDestinationDatabase := resolveConfigurableField(oldMetadata.DestinationDatabase, oldStream.Stream.DestinationDatabase)
				newDestinationDatabase := resolveConfigurableField(newMetadata.DestinationDatabase, newStream.Stream.DestinationDatabase)
				oldDestinationTable := resolveConfigurableField(oldMetadata.DestinationTable, oldStream.Stream.DestinationTable)
				newDestinationTable := resolveConfigurableField(newMetadata.DestinationTable, newStream.Stream.DestinationTable)

				// check cursor field if SyncMode is incremental
				cursorDelta := utils.Ternary(newSyncMode == INCREMENTAL, oldCursorField != newCursorField, false).(bool)

				return oldConfigured.NormalizationEnabled() != newConfigured.NormalizationEnabled() ||
					oldConfigured.AppendModeEnabled() != newConfigured.AppendModeEnabled() ||
					(oldMetadata.PartitionRegex != newMetadata.PartitionRegex) ||
					(oldMetadata.Filter != newMetadata.Filter) ||
					(oldMetadata.UseSourceColumnNames != newMetadata.UseSourceColumnNames) ||
					!reflect.DeepEqual(oldMetadata.FilterConfig, newMetadata.FilterConfig) ||
					(oldSyncMode != newSyncMode) ||
					(oldDestinationDatabase != newDestinationDatabase) ||
					(oldDestinationTable != newDestinationTable) ||
					cursorDelta
			}()

			// if any difference, add stream to diff streams
			if isDifferent {
				// copy of the new stream to modify it for the difference
				newStreamCopy := *newStream.Stream
				deltaStream := &ConfiguredStream{
					Stream: &newStreamCopy,
				}

				// keep the user's existing destination mapping in the diff output even when discover produced new values
				oldDestinationDatabase := resolveConfigurableField(oldMetadata.DestinationDatabase, oldStream.Stream.DestinationDatabase)
				oldDestinationTable := resolveConfigurableField(oldMetadata.DestinationTable, oldStream.Stream.DestinationTable)
				deltaStream.Stream.DestinationDatabase = oldDestinationDatabase
				deltaStream.Stream.DestinationTable = oldDestinationTable
				newMetadata.DestinationDatabase = oldDestinationDatabase
				newMetadata.DestinationTable = oldDestinationTable

				diffStreams.Streams = append(diffStreams.Streams, deltaStream)
				diffStreams.SelectedStreams[namespace] = append(
					diffStreams.SelectedStreams[namespace],
					newMetadata,
				)
			}
		}
	}

	return diffStreams
}

func IsDriverRelational(driver string) bool {
	_, isRelational := utils.ArrayContains(constants.RelationalDrivers, func(src constants.DriverType) bool {
		return src == constants.DriverType(driver)
	})
	return isRelational
}
