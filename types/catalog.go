package types

import (
	"fmt"
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

type StreamMetadata struct {
	ChunkColumn     string           `json:"chunk_column,omitempty"`
	PartitionRegex  string           `json:"partition_regex"`
	StreamName      string           `json:"stream_name"`
	AppendMode      bool             `json:"append_mode,omitempty"`
	Normalization   bool             `json:"normalization"`
	Filter          string           `json:"filter,omitempty"`
	SelectedColumns *SelectedColumns `json:"selected_columns"`
	SyncNewColumns  bool             `json:"sync_new_columns"`
}
type Catalog struct {
	SelectedStreams map[string][]StreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream         `json:"streams,omitempty"`
}

type SelectedColumns struct {
	Columns     []string            `json:"columns"`
	Map         map[string]struct{} `json:"-"`
	AllSelected bool                `json:"-"`
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

		// Collect selected columns from schema
		var selectedColumns []string
		stream.Schema.Properties.Range(func(key, _ interface{}) bool {
			if columnName, ok := key.(string); ok {
				selectedColumns = append(selectedColumns, columnName)
			}
			return true
		})

		catalog.SelectedStreams[stream.Namespace] = append(catalog.SelectedStreams[stream.Namespace], StreamMetadata{
			StreamName:    stream.Name,
			AppendMode:    utils.Ternary(driver == string(constants.Kafka), true, false).(bool),
			Normalization: IsDriverRelational(driver),
			SelectedColumns: &SelectedColumns{
				Columns: selectedColumns,
			},
			SyncNewColumns: false,
		})
	}

	return catalog
}

// MergeCatalogs merges old catalog with new catalog based on the following rules:
// 1. SelectedStreams: Retain only streams present in both oldCatalog.SelectedStreams and newStreamMap
// 2. SelectedColumns: Retain columns present in both old and new schemas, add NEW columns if sync_new_columns is true, add mandatory columns if not present
// 3. SyncMode: Use from oldCatalog if the stream exists in old catalog
// 4. Everything else: Keep as new catalog
func mergeCatalogs(oldCatalog, newCatalog *Catalog) *Catalog {
	if oldCatalog == nil {
		return newCatalog
	}

	createStreamAndMetadataMaps := func(catalog *Catalog) (map[string]*ConfiguredStream, map[string]*StreamMetadata) {
		streamMap := make(map[string]*ConfiguredStream)
		metadataMap := make(map[string]*StreamMetadata)

		for _, st := range catalog.Streams {
			streamID := st.Stream.ID()
			streamMap[streamID] = st

			if metadataList, exists := catalog.SelectedStreams[st.Stream.Namespace]; exists {
				for i := range metadataList {
					if metadataList[i].StreamName == st.Stream.Name {
						metadataMap[streamID] = &metadataList[i]
						break
					}
				}
			}
		}
		return streamMap, metadataMap
	}

	oldStreams, _ := createStreamAndMetadataMaps(oldCatalog)

	// merge selected streams
	if oldCatalog.SelectedStreams != nil {
		newStreams, newMetadataMap := createStreamAndMetadataMaps(newCatalog)
		selectedStreams := make(map[string][]StreamMetadata)

		for namespace, metadataList := range oldCatalog.SelectedStreams {
			_ = utils.ForEach(metadataList, func(metadata StreamMetadata) error {
				streamID := fmt.Sprintf("%s.%s", namespace, metadata.StreamName)
				_, exists := newStreams[streamID]

				if exists {
					oldSchema := oldStreams[streamID].Stream.Schema
					newSchema := newStreams[streamID].Stream.Schema

					// when selectedColumns property is not present or empty, use all columns from new schema or only columns that existed in old schema
					if metadata.SelectedColumns == nil || len(metadata.SelectedColumns.Columns) == 0 {
						if metadata.SyncNewColumns {
							// syn all columns
							columns := newMetadataMap[streamID].SelectedColumns.Columns
							metadata.SelectedColumns = &SelectedColumns{
								Columns: columns,
							}
						} else {
							// we select only columns that existed in old schema
							var preservedColumns []string
							oldSchema.Properties.Range(func(key, _ interface{}) bool {
								colName := key.(string)
								if _, exists := newSchema.Properties.Load(colName); exists {
									preservedColumns = append(preservedColumns, colName)
								}
								return true
							})
							metadata.SelectedColumns = &SelectedColumns{
								Columns: preservedColumns,
							}

							// no need to call ensureMandatoryColumns here as all the columns are already present in the old schema
						}
					} else {
						// when selectedColumns property is present and not empty, use only columns that existed in both old and new schemas
						var preservedSelectedColumns []string
						for _, previouslySelectedCol := range metadata.SelectedColumns.Columns {
							// Check if column exists in both old and new schemas
							_, existsInOld := oldSchema.Properties.Load(previouslySelectedCol)
							_, existsInNew := newSchema.Properties.Load(previouslySelectedCol)
							if existsInOld && existsInNew {
								preservedSelectedColumns = append(preservedSelectedColumns, previouslySelectedCol)
							}
						}

						metadata.SelectedColumns.Columns = preservedSelectedColumns

						// add new discovered columns if sync_new_columns is true
						if metadata.SyncNewColumns {
							_, newAddedColumns := getColumnsDelta(oldSchema, newSchema)

							if len(newAddedColumns) > 0 {
								metadata.SelectedColumns.Columns = append(metadata.SelectedColumns.Columns, newAddedColumns...)
							}
						}

						ensureMandatoryColumns(&metadata, newStreams[streamID].Stream)
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
			// preserve metadata from old
			newStream.Stream.SyncMode = oldStream.Stream.SyncMode
			newStream.Stream.CursorField = oldStream.Stream.CursorField
			newStream.Stream.DestinationDatabase = oldStream.Stream.DestinationDatabase
			newStream.Stream.DestinationTable = oldStream.Stream.DestinationTable
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
// 1. Compares properties from selected_streams: normalization, partition_regex, filter, append_mode, sync_new_columns
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
			// selected columns change
			// destination database change
			// cursor field change , Format: "primary_cursor:secondary_cursor"
			// sync mode change
			// destination table change
			// TODO: log the differences for user reference
			isDifferent := func() bool {
				// check cursor field if SyncMode is incremental
				cursorDelta := utils.Ternary(newStream.Stream.SyncMode == INCREMENTAL, oldStream.Stream.CursorField != newStream.Stream.CursorField, false).(bool)

				return (oldMetadata.Normalization != newMetadata.Normalization) ||
					(oldMetadata.PartitionRegex != newMetadata.PartitionRegex) ||
					(oldMetadata.Filter != newMetadata.Filter) ||
					(oldMetadata.AppendMode != newMetadata.AppendMode) ||
					(oldMetadata.SyncNewColumns != newMetadata.SyncNewColumns) ||
					(oldStream.Stream.SyncMode != newStream.Stream.SyncMode) ||
					(oldStream.Stream.DestinationDatabase != newStream.Stream.DestinationDatabase) ||
					(oldStream.Stream.DestinationTable != newStream.Stream.DestinationTable) ||
					cursorDelta
			}()

			// if any difference, add stream to diff streams
			if isDifferent {
				// copy of the new stream to modify it for the difference
				newStreamCopy := *newStream.Stream
				deltaStream := &ConfiguredStream{
					Stream: &newStreamCopy,
				}

				// safely change for destination database and table if difference present
				deltaStream.Stream.DestinationDatabase = oldStream.Stream.DestinationDatabase
				deltaStream.Stream.DestinationTable = oldStream.Stream.DestinationTable

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
