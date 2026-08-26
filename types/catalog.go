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
	AppendMode     bool   `json:"append_mode,omitempty"`
	Normalization  bool   `json:"normalization"`
	// When enabled, source column names are preserved as-is; otherwise utils.Reformat() is applied to generate destination-safe lowercase column names.
	UseSourceColumnNames bool `json:"use_source_column_names"`
	//legacy filter input
	Filter string `json:"filter,omitempty"`
	//new filter input
	FilterConfig        *FilterConfig    `json:"filter_config,omitempty"`
	SelectedColumns     *SelectedColumns `json:"selected_columns"`
	SyncMode            SyncMode         `json:"sync_mode,omitempty"`
	CursorField         string           `json:"cursor_field,omitempty"`
	DestinationDatabase string           `json:"destination_database,omitempty"`
	DestinationTable    string           `json:"destination_table,omitempty"`
}

// resolveConfigurableField returns metadata value when set, otherwise stream value.
func resolveConfigurableField[T comparable](metadataValue, streamValue T) T {
	var zero T
	if metadataValue != zero {
		return metadataValue
	}
	return streamValue
}

func migrateConfigurableFieldsFromStream(metadata *StreamMetadata, stream *Stream) {
	metadata.SyncMode = resolveConfigurableField(metadata.SyncMode, stream.SyncMode)
	metadata.CursorField = resolveConfigurableField(metadata.CursorField, stream.CursorField)
	metadata.DestinationDatabase = resolveConfigurableField(metadata.DestinationDatabase, stream.DestinationDatabase)
	metadata.DestinationTable = resolveConfigurableField(metadata.DestinationTable, stream.DestinationTable)
}

func clearStreamConfigurableFields(stream *Stream) {
	stream.SyncMode = ""
	stream.CursorField = ""
	stream.DestinationDatabase = ""
	stream.DestinationTable = ""
}

type Catalog struct {
	SelectedStreams map[string][]StreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream         `json:"streams,omitempty"`
}

// StreamMix is the per-sync breakdown of the streams a run actually syncs. Only streams that
// survived selection and validation are counted, so the sync-mode counters sum to Selected.
type StreamMix struct {
	FullRefresh int `json:"full_refresh_streams_count"`
	Incremental int `json:"incremental_streams_count"`
	CDC         int `json:"cdc_streams_count"`
	StrictCDC   int `json:"strict_cdc_streams_count"`
	Selected    int `json:"selected_streams_count"`
	Normalized  int `json:"normalized_streams_count"`
	Partitioned int `json:"partitioned_streams_count"`
}

// ResolveCatalog loads a catalog from disk, handling both the default (combined) and
// split (--schema) file layouts.
//
// Default layout: streams.json contains both streams[] and selected_streams.
// This is the normal output of discover and is returned as-is; schemaPath is ignored.
//
// Split layout (opt-in via --schema): streams.json contains only selected_streams;
// streams[] lives in a separate schema.json. When streams[] is absent, schemaPath must
// point to the schema file or an error is returned.
func ResolveCatalog(streamsFilePath, schemaFilePath string) (*Catalog, error) {
	catalog := &Catalog{}
	if err := utils.UnmarshalFile(streamsFilePath, catalog, false); err != nil {
		return nil, fmt.Errorf("failed to read streams from %s: %s", streamsFilePath, err)
	}

	// streams[] is present — this is the default combined layout
	if len(catalog.Streams) > 0 {
		return catalog, nil
	}

	// streams[] is absent — split layout (selected_streams populated, streams[] in schema.json)
	if len(catalog.SelectedStreams) > 0 {
		if schemaFilePath == "" {
			return nil, fmt.Errorf("--schema required: streams.json contains only selected_streams (split layout). Pass --schema <path> to provide the stream metadata file")
		}
		schemaCatalog := &Catalog{}
		if err := utils.UnmarshalFile(schemaFilePath, schemaCatalog, false); err != nil {
			return nil, fmt.Errorf("failed to read schema from %s: %s", schemaFilePath, err)
		}
		catalog.Streams = schemaCatalog.Streams
		return catalog, nil
	}

	return nil, fmt.Errorf("streams file %s has no streams[] and no selected_streams; file may be empty or malformed", streamsFilePath)
}

// splitCatalogForWrite returns two Catalog values for the opt-in split file layout:
// selected_streams only, and streams[] only.
func splitCatalogForWrite(catalog *Catalog) (streamsFile, schemaFile *Catalog) {
	return &Catalog{SelectedStreams: catalog.SelectedStreams}, &Catalog{Streams: catalog.Streams}
}

func GetWrappedCatalog(streams []*Stream, driver string) *Catalog {
	catalog := &Catalog{
		Streams:         []*ConfiguredStream{},
		SelectedStreams: make(map[string][]StreamMetadata),
	}

	// Loop through each stream and populate Streams and SelectedStreams
	for _, stream := range streams {
		// Create ConfiguredStream and append to Streams
		catalog.Streams = append(catalog.Streams, &ConfiguredStream{
			Stream: stream,
		})

		selectedColumns := stream.Schema.ColumnNames()
		selectedCols := &SelectedColumns{
			Columns:        selectedColumns,
			SyncNewColumns: true,
		}

		catalog.SelectedStreams[stream.Namespace] = append(catalog.SelectedStreams[stream.Namespace], StreamMetadata{
			StreamName:          stream.Name,
			AppendMode:          utils.Ternary(driver == string(constants.Kafka), true, false).(bool),
			Normalization:       IsDriverRelational(driver),
			SelectedColumns:     selectedCols,
			SyncMode:            stream.SyncMode,
			CursorField:         stream.CursorField,
			DestinationDatabase: stream.DestinationDatabase,
			DestinationTable:    stream.DestinationTable,
		})
		clearStreamConfigurableFields(stream)
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
					oldStream := oldStreams[streamID].Stream
					newStream := newStreams[streamID].Stream
					MergeSelectedColumns(&metadata, oldStream, newStream)
					migrateConfigurableFieldsFromStream(&metadata, oldStream)

					selectedStreams[namespace] = append(selectedStreams[namespace], metadata)
				}
				return nil
			})
		}
		newCatalog.SelectedStreams = selectedStreams
	}

	constantValue, prefix := getDestDBPrefix(oldCatalog)

	// merge streams metadata
	_ = utils.ForEach(newCatalog.Streams, func(newStream *ConfiguredStream) error {
		oldStream, exists := oldStreams[newStream.Stream.ID()]

		var destDB string
		if exists {
			newStream.Stream.SourceDefinedPrimaryKey = oldStream.Stream.SourceDefinedPrimaryKey
		} else {
			// NOTE: new streams are not added to selected_streams, user needs to manually enable them
			// manipulate destination db in new streams according to old streams

			// prefix == "" means old stream when db normalization feature not introduced.
			// getDestDBPrefix already resolves dest db via metadata (new format) with Stream fallback,
			// so `prefix` holds the constant value directly when constantValue is true.
			if constantValue {
				destDB = prefix
			} else if prefix != "" {
				destDB = fmt.Sprintf("%s:%s", prefix, utils.Reformat(newStream.Stream.Namespace))
			}
			// Keep discover-generated default when the job has no established dest-db pattern.
			if destDB == "" {
				destDB = newStream.Stream.DestinationDatabase
			}
		}

		// set destination database to the stream
		clearStreamConfigurableFields(newStream.Stream)
		if !exists && destDB != "" {
			newStream.Stream.DestinationDatabase = destDB
		}
		return nil
	})

	return newCatalog
}

// MergeSelectedColumns merges the selected columns based on the following rules:
// - If selectedColumns is not present or empty, initialize with columns from new schema
// - Preserve previously selected columns
// - If sync_new_columns is true, add newly discovered columns to the selected columns
// takes old stream and new stream to merge the selected columns and old stream metadata
func MergeSelectedColumns(metadata *StreamMetadata, oldStream *Stream, newStream *Stream) {
	var columns []string

	// No previous selection: initialize with all columns from new schema.
	if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Columns) == 0 {
		columns = newStream.Schema.ColumnNames()
	} else {
		previouslySelectedSet := NewSet(metadata.SelectedColumns.Columns...)
		oldSchemaCols := NewSet(oldStream.Schema.ColumnNames()...)

		// Iterate new schema: retain previously selected columns, add new ones if sync_new_columns enabled.
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
func getDestDBPrefix(catalog *Catalog) (constantValue bool, prefix string) {
	if catalog == nil {
		return false, ""
	}

	streamDestDB := make(map[string]string, len(catalog.Streams))
	for _, s := range catalog.Streams {
		streamDestDB[s.Stream.ID()] = s.Stream.DestinationDatabase
	}

	var destDBs []string
	for namespace, metadataList := range catalog.SelectedStreams {
		for _, metadata := range metadataList {
			streamID := fmt.Sprintf("%s.%s", namespace, metadata.StreamName)
			destDBs = append(destDBs, resolveConfigurableField(metadata.DestinationDatabase, streamDestDB[streamID]))
		}
	}

	if len(destDBs) == 0 {
		return false, ""
	}

	prefixOrConstValue := strings.Split(destDBs[0], ":")
	for _, db := range destDBs[1:] {
		parts := strings.Split(db, ":")
		if parts[0] != prefixOrConstValue[0] {
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
			// TODO: log the differences for user reference
			isDifferent := func() bool {
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

				return (oldMetadata.Normalization != newMetadata.Normalization) ||
					(oldMetadata.PartitionRegex != newMetadata.PartitionRegex) ||
					(oldMetadata.Filter != newMetadata.Filter) ||
					(oldMetadata.UseSourceColumnNames != newMetadata.UseSourceColumnNames) ||
					!reflect.DeepEqual(oldMetadata.FilterConfig, newMetadata.FilterConfig) ||
					(oldMetadata.AppendMode != newMetadata.AppendMode) ||
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
