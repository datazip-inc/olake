package types

import (
	"fmt"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

const (
	codeLegacyStreamsMissing       = "catalog.streams_missing"
	codeLegacySelectedStreamsEmpty = "catalog.legacy_selected_streams_empty"
)

// LegacyStreamMetadata is the streams.json selected_streams entry shape, frozen to match the pre-split behavior
// Deprecated: Use StreamMetadata instead
type LegacyStreamMetadata struct {
	ChunkColumn          string           `json:"chunk_column,omitempty"`
	PartitionRegex       string           `json:"partition_regex"`
	StreamName           string           `json:"stream_name"`
	AppendMode           bool             `json:"append_mode,omitempty"`
	Normalization        bool             `json:"normalization"`
	UpdateType           string           `json:"update_type,omitempty"`
	UseSourceColumnNames bool             `json:"use_source_column_names"`
	Filter               string           `json:"filter,omitempty"`
	FilterConfig         *FilterConfig    `json:"filter_config,omitempty"`
	SelectedColumns      *SelectedColumns `json:"selected_columns"`
}

// LegacyCatalog is the streams.json ([]streams + selected_streams)
// Deprecated: Use Catalog instead
type LegacyCatalog struct {
	SelectedStreams map[string][]LegacyStreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream               `json:"streams,omitempty"`
}

// ResolveLegacyCatalog loads a self-contained streams.json (streams[] + selected_streams[]) from disk.
// Deprecated: Use ResolveCatalog instead
func ResolveLegacyCatalog(streamsFilePath string) (*LegacyCatalog, error) {
	catalog := &LegacyCatalog{}
	if err := utils.UnmarshalFile(streamsFilePath, catalog, false); err != nil {
		return nil, fmt.Errorf("failed to read streams from %s: %w", streamsFilePath, err)
	}

	if len(catalog.Streams) > 0 && len(catalog.SelectedStreams) == 0 {
		return nil, errs.Precondition(errs.CatalogError, codeLegacySelectedStreamsEmpty,
			fmt.Errorf("streams file %s has streams[] but no selected_streams", streamsFilePath))
	}

	if len(catalog.Streams) == 0 && len(catalog.SelectedStreams) > 0 {
		return nil, errs.Precondition(errs.CatalogError, codeLegacyStreamsMissing,
			fmt.Errorf("streams file %s has selected_streams but no streams[]", streamsFilePath))
	}

	return catalog, nil
}

// legacyToStreamMetadata converts a legacy selected_streams entry to the runtime StreamMetadata
// shape. SyncMode/CursorField/DestinationDatabase/DestinationTable stay zero here - those live
// on Stream, not metadata.
func legacyToStreamMetadata(streamMetadata LegacyStreamMetadata) StreamMetadata {
	return StreamMetadata{
		ChunkColumn:          streamMetadata.ChunkColumn,
		PartitionRegex:       streamMetadata.PartitionRegex,
		StreamName:           streamMetadata.StreamName,
		AppendMode:           &streamMetadata.AppendMode,
		Normalization:        &streamMetadata.Normalization,
		UpdateType:           streamMetadata.UpdateType,
		UseSourceColumnNames: streamMetadata.UseSourceColumnNames,
		Filter:               streamMetadata.Filter,
		FilterConfig:         streamMetadata.FilterConfig,
		SelectedColumns:      streamMetadata.SelectedColumns,
	}
}

// legacyToCanonical converts a LegacyCatalog (streams.json) into a Catalog (available_streams.json and selected_streams.json).
func legacyToCanonical(legacy *LegacyCatalog) *Catalog {
	selectedStreams := make(map[string][]StreamMetadata, len(legacy.SelectedStreams))
	for namespace, metadataList := range legacy.SelectedStreams {
		converted := make([]StreamMetadata, len(metadataList))
		for i, metadata := range metadataList {
			converted[i] = legacyToStreamMetadata(metadata)
		}
		selectedStreams[namespace] = converted
	}

	return &Catalog{
		Streams:         legacy.Streams,
		SelectedStreams: selectedStreams,
	}
}

// WriteToFile writes the legacy catalog as-is; toLegacyCatalog already emits it sorted.
func (c *LegacyCatalog) WriteToFile(path string) error {
	return logger.FileLoggerWithPath(c, path)
}

// toLegacyCatalog produces streams.json's on-disk shape from the single canonical merged
// Catalog — the same merge result available_streams.json/selected_streams.json are written
// from. Two things the legacy shape always spells out explicitly, that the canonical shape
// leaves implicit:
//   - normalization/append_mode/update_type: nil on StreamMetadata falls back to the stream's
//     own DefaultStreamProperties (set once at discover time by the driver layer).
//   - selected_columns: nil/empty means "all columns"; streams.json writes the current column
//     list instead of omitting the field, matching pre-split behavior.
func toLegacyCatalog(canonical *Catalog) *LegacyCatalog {
	canonical.sortByNamespaceStreamName()
	legacy := &LegacyCatalog{
		Streams:         canonical.Streams,
		SelectedStreams: make(map[string][]LegacyStreamMetadata, len(canonical.SelectedStreams)),
	}

	configuredByID := streamMapByID(canonical.Streams)
	for namespace, metadataList := range canonical.SelectedStreams {
		converted := make([]LegacyStreamMetadata, 0, len(metadataList))
		for _, metadata := range metadataList {
			configured, ok := configuredByID[fmt.Sprintf("%s.%s", namespace, metadata.StreamName)]
			if !ok {
				continue
			}
			converted = append(converted, toLegacyStreamMetadata(metadata, configured.Stream))
		}
		legacy.SelectedStreams[namespace] = converted
	}

	return legacy
}

// toLegacyStreamMetadata converts one canonical StreamMetadata into its streams.json shape.
// stream supplies the current schema for the "all columns" fallback, and its
// DefaultStreamProperties for the normalization/append_mode/update_type fallback.
func toLegacyStreamMetadata(metadata StreamMetadata, stream *Stream) LegacyStreamMetadata {
	defaults := DefaultStreamProperties{}
	if stream.DefaultStreamProperties != nil {
		defaults = *stream.DefaultStreamProperties
	}

	normalization := defaults.Normalization
	if metadata.Normalization != nil {
		normalization = *metadata.Normalization
	}

	appendMode := defaults.AppendMode
	if metadata.AppendMode != nil {
		appendMode = *metadata.AppendMode
	}

	updateType := string(defaults.UpdateType)
	if metadata.UpdateType != "" {
		updateType = metadata.UpdateType
	}

	selectedColumns := metadata.SelectedColumns
	if selectedColumns == nil || len(selectedColumns.Columns) == 0 {
		syncNewColumns := true
		if selectedColumns != nil {
			syncNewColumns = selectedColumns.SyncNewColumns
		}
		selectedColumns = &SelectedColumns{Columns: stream.Schema.ColumnNames(), SyncNewColumns: syncNewColumns}
	}

	return LegacyStreamMetadata{
		ChunkColumn:          metadata.ChunkColumn,
		PartitionRegex:       metadata.PartitionRegex,
		StreamName:           metadata.StreamName,
		AppendMode:           appendMode,
		Normalization:        normalization,
		UpdateType:           updateType,
		UseSourceColumnNames: metadata.UseSourceColumnNames,
		Filter:               metadata.Filter,
		FilterConfig:         metadata.FilterConfig,
		SelectedColumns:      selectedColumns,
	}
}
