package types

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/goccy/go-json"
	"github.com/spf13/viper"
)

const (
	codeAvailableStreamsEmpty = "catalog.available_streams_empty"
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

// MarshalJSON writes columns in sorted order so the file is stable across runs.
// called implicitly by json.Marshal when writing SelectedColumns to file
func (s SelectedColumns) MarshalJSON() ([]byte, error) {
	sorted := slices.Sorted(slices.Values(s.Columns))
	type alias SelectedColumns // alias to avoid recursive call
	return json.Marshal(alias{Columns: sorted, SyncNewColumns: s.SyncNewColumns})
}

type StreamMetadata struct {
	ChunkColumn    string  `json:"chunk_column,omitempty"`
	PartitionRegex string  `json:"partition_regex"`
	StreamName     string  `json:"stream_name"`
	AppendMode     *bool   `json:"append_mode,omitempty"`
	Normalization  *bool   `json:"normalization,omitempty"`
	UpdateType     *string `json:"update_type,omitempty"`
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
	return cmp.Or(values...)
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

// ResolveCatalog loads the runtime catalog from disk in new or legacy format:
//   - New format: availableStreamsFilePath + selectedStreamsFilePath
//   - Legacy format: streamsFilePath
//
// Callers validate the flags first, so the new-format paths are either both set or both empty.
func ResolveCatalog(streamsFilePath, availableStreamsFilePath, selectedStreamsFilePath string) (*Catalog, error) {
	if availableStreamsFilePath != "" && selectedStreamsFilePath != "" {
		catalog := &Catalog{}
		if err := utils.UnmarshalFile(availableStreamsFilePath, catalog, false); err != nil {
			return nil, fmt.Errorf("failed to read streams from %s: %w", availableStreamsFilePath, err)
		}
		if len(catalog.Streams) == 0 {
			return nil, errs.Precondition(errs.CatalogError, codeAvailableStreamsEmpty,
				fmt.Errorf("available_streams file %s has no streams[]", availableStreamsFilePath))
		}

		selectedCatalog := &Catalog{}
		if err := utils.UnmarshalFile(selectedStreamsFilePath, selectedCatalog, false); err != nil {
			return nil, fmt.Errorf("failed to read selected_streams from %s: %w", selectedStreamsFilePath, err)
		}
		// An empty selected_streams file selects nothing, like an explicit {} in streams.json.
		// Keep that non-nil: a nil selection reads as "no prior selection" and discover re-selects every stream.
		catalog.SelectedStreams = selectedCatalog.SelectedStreams
		if catalog.SelectedStreams == nil {
			catalog.SelectedStreams = map[string][]StreamMetadata{}
		}

		return catalog, nil
	}

	legacy, err := ResolveLegacyCatalog(streamsFilePath)
	if err != nil {
		return nil, err
	}
	return legacyToCanonical(legacy), nil
}

// sortByNamespaceStreamName sorts the catalog so it is written in the same order on every run.
// It sorts streams[] by namespace, then stream name, and the streams under each
// selected_streams namespace by stream_name. The namespace keys need no sorting:
// encoding/json writes map keys in sorted order.
func (c *Catalog) sortByNamespaceStreamName() {
	// sort streams[] by namespace, then stream name
	slices.SortFunc(c.Streams, func(left, right *ConfiguredStream) int {
		return cmp.Or(
			strings.Compare(left.Stream.Namespace, right.Stream.Namespace),
			strings.Compare(left.Stream.Name, right.Stream.Name))
	})

	// sort the streams under each selected_streams namespace by stream_name
	for namespace := range c.SelectedStreams {
		slices.SortFunc(c.SelectedStreams[namespace], func(a, b StreamMetadata) int {
			return strings.Compare(a.StreamName, b.StreamName)
		})
	}
}

// writeSplitFiles writes the catalog as available_streams.json and selected_streams.json.
func (c *Catalog) writeSplitFiles() error {
	return c.WriteSplitToFiles(viper.GetString(constants.AvailableStreamsPath), viper.GetString(constants.SelectedStreamsPath))
}

// WriteSplitToFiles writes streams[] to availablePath and selected_streams to selectedPath.
func (c *Catalog) WriteSplitToFiles(availablePath, selectedPath string) error {
	if err := (&Catalog{Streams: c.Streams}).WriteToFile(availablePath); err != nil {
		return fmt.Errorf("failed to create available_streams file: %w", err)
	}
	if err := (&Catalog{SelectedStreams: c.SelectedStreams}).WriteToFile(selectedPath); err != nil {
		return fmt.Errorf("failed to create selected_streams file: %w", err)
	}
	return nil
}

func (c *Catalog) WriteToFile(path string) error {
	c.sortByNamespaceStreamName()
	return logger.FileLoggerWithPath(c, path)
}

func GetWrappedCatalog(streams []*Stream, engines []QueryEngine) *Catalog {
	catalog := &Catalog{
		Streams:         []*ConfiguredStream{},
		SelectedStreams: make(map[string][]StreamMetadata),
	}
	// The default delete format is the cheapest one every target engine can read.
	available := AvailableUpdateTypes(engines)
	updateType := PreferredUpdateType(available)

	for _, stream := range streams {
		stream.RefreshSelectableColumns()
		stream.AvailableUpdateTypes = available
		if stream.DefaultStreamProperties != nil {
			stream.DefaultStreamProperties.UpdateType = updateType
		}

		catalog.Streams = append(catalog.Streams, &ConfiguredStream{
			Stream: stream,
		})

		metadata := StreamMetadata{
			StreamName:     stream.Name,
			PartitionRegex: "",
			SyncMode:       stream.SyncMode,
			CursorField:    utils.Ternary(stream.SyncMode == INCREMENTAL, stream.CursorField, "").(string),
		}
		catalog.SelectedStreams[stream.Namespace] = append(catalog.SelectedStreams[stream.Namespace], metadata)
	}

	return catalog
}

func streamMapByID(streams []*ConfiguredStream) map[string]*ConfiguredStream {
	streamMap := make(map[string]*ConfiguredStream, len(streams))
	for _, stream := range streams {
		streamMap[stream.Stream.ID()] = stream
	}
	return streamMap
}

// MergeCatalogs merges old catalog with new catalog based on the following rules:
// 1. SelectedStreams: Retain only streams present in both oldCatalog.SelectedStreams and newStreamMap
// 2. SelectedColumns: Retain columns present in both old and new schemas, add NEW columns if sync_new_columns is true
// 3. SyncMode: Use from oldCatalog if the stream exists in old catalog
// 4. Everything else: Keep as new catalog
func mergeCatalogs(oldCatalog, newCatalog *Catalog, engines []QueryEngine) *Catalog {
	if oldCatalog == nil {
		return newCatalog
	}

	oldStreams := streamMapByID(oldCatalog.Streams)

	// merge selected streams
	if oldCatalog.SelectedStreams != nil {
		newStreams := streamMapByID(newCatalog.Streams)
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
					mergeUpdateType(&metadata, streamID, engines)

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

// mergeUpdateType keeps a previously configured delete format only while every current
// target query engine can still read it. An unreadable choice is cleared rather than
// replaced: switching delete formats can force a table recreate, so the user must pick the
// new one explicitly, and a blank update_type fails validation until they do.
func mergeUpdateType(metadata *StreamMetadata, streamID string, engines []QueryEngine) {
	if metadata.UpdateType == nil {
		return
	}

	// A blank value predates update_type and always meant equality (see
	// ConfiguredStream.GetUpdateType). Record it, so blank is left to mean "needs a choice".
	if *metadata.UpdateType == "" {
		equality := string(UpdateTypeEquality)
		metadata.UpdateType = &equality
	}

	// Without target engines nothing constrains the choice.
	if len(engines) == 0 {
		return
	}

	available := AvailableUpdateTypes(engines)
	if slices.Contains(available, UpdateType(*metadata.UpdateType)) {
		return
	}

	logger.Warnf("Stream %s update mode %s is not readable by the selected query engines; cleared, choose one of %v",
		streamID, *metadata.UpdateType, available)
	metadata.UpdateType = new(string)
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
	slices.Sort(columns)

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
// 1. Compares properties from selected_streams: normalization, partition_regex, filter, append_mode, use_source_column_names, update_type (dv -> other, pos -> dv)
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
				logger.Warnf("Skipping; selected stream %s has no matching entry in the new available streams", streamID)
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

			// NOTE: delete mode changes keep the table, except dv -> other and pos -> dv (see dvDelta)
			// TODO: log the differences for user reference
			oldDestinationDatabase := resolveConfigurableField(oldMetadata.DestinationDatabase, oldStream.Stream.DestinationDatabase)
			oldDestinationTable := resolveConfigurableField(oldMetadata.DestinationTable, oldStream.Stream.DestinationTable)
			isDifferent := func() bool {
				oldConfigured := &ConfiguredStream{Stream: oldStream.Stream, StreamMetadata: oldMetadata}
				newConfigured := &ConfiguredStream{Stream: newStream.Stream, StreamMetadata: newMetadata}

				// leaving dv: v3 forbids the Parquet positional deletes eq/pos write; pos -> dv: not supported yet (only eq -> dv is migrated)
				oldUpdateType, newUpdateType := oldConfigured.GetUpdateType(), newConfigured.GetUpdateType()
				dvDelta := (oldUpdateType == UpdateTypeDeletionVector && newUpdateType != UpdateTypeDeletionVector) ||
					(oldUpdateType == UpdateTypePosition && newUpdateType == UpdateTypeDeletionVector)

				oldSyncMode, newSyncMode := oldConfigured.GetSyncMode(), newConfigured.GetSyncMode()
				oldCursorField := resolveConfigurableField(oldMetadata.CursorField, oldStream.Stream.CursorField)
				newCursorField := resolveConfigurableField(newMetadata.CursorField, newStream.Stream.CursorField)
				newDestinationDatabase := resolveConfigurableField(newMetadata.DestinationDatabase, newStream.Stream.DestinationDatabase)
				newDestinationTable := resolveConfigurableField(newMetadata.DestinationTable, newStream.Stream.DestinationTable)

				// check cursor field if SyncMode is incremental
				cursorDelta := newSyncMode == INCREMENTAL && oldCursorField != newCursorField

				return (oldConfigured.NormalizationEnabled() != newConfigured.NormalizationEnabled()) ||
					(oldConfigured.AppendModeEnabled() != newConfigured.AppendModeEnabled()) ||
					(oldMetadata.PartitionRegex != newMetadata.PartitionRegex) ||
					(oldMetadata.Filter != newMetadata.Filter) ||
					(oldMetadata.UseSourceColumnNames != newMetadata.UseSourceColumnNames) ||
					!reflect.DeepEqual(oldMetadata.FilterConfig, newMetadata.FilterConfig) ||
					(oldSyncMode != newSyncMode) ||
					(oldDestinationDatabase != newDestinationDatabase) ||
					(oldDestinationTable != newDestinationTable) ||
					cursorDelta ||
					dvDelta
			}()

			// if any difference, add stream to diff streams
			if isDifferent {
				// copy of the new stream to modify it for the difference
				newStreamCopy := *newStream.Stream
				deltaStream := &ConfiguredStream{
					Stream: &newStreamCopy,
				}

				// keep the user's existing destination mapping in the diff output even when discover produced new values
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
