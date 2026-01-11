package types

import (
	"fmt"
	"strings"
	"reflect"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
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
	ChunkColumn    string `json:"chunk_column,omitempty"`
	PartitionRegex string `json:"partition_regex"`
	StreamName     string `json:"stream_name"`
	AppendMode     bool   `json:"append_mode,omitempty"`
	Normalization  bool   `json:"normalization"`
	Filter         string `json:"filter,omitempty"`
}
type Catalog struct {
	SelectedStreams map[string][]StreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream         `json:"streams,omitempty"`
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

		catalog.SelectedStreams[stream.Namespace] = append(catalog.SelectedStreams[stream.Namespace], StreamMetadata{
			StreamName:    stream.Name,
			AppendMode:    utils.Ternary(driver == string(constants.Kafka), true, false).(bool),
			Normalization: IsDriverRelational(driver),
		})
	}

	return catalog
}

// MergeCatalogs merges old catalog with new catalog based on the following rules:
// 1. SelectedStreams: Retain only streams present in both oldCatalog.SelectedStreams and newStreamMap
// 2. SyncMode: Use from oldCatalog if the stream exists in old catalog
// 3. Everything else: Keep as new catalog
func mergeCatalogs(oldCatalog, newCatalog *Catalog) *Catalog {
	if oldCatalog == nil {
		return newCatalog
	}

	createStreamMap := func(catalog *Catalog) map[string]*ConfiguredStream {
		sm := make(map[string]*ConfiguredStream)
		for _, st := range catalog.Streams {
			sm[st.Stream.ID()] = st
		}
		return sm
	}

	// merge selected streams
	if oldCatalog.SelectedStreams != nil {
		newStreams := createStreamMap(newCatalog)
		selectedStreams := make(map[string][]StreamMetadata)
		for namespace, metadataList := range oldCatalog.SelectedStreams {
			_ = utils.ForEach(metadataList, func(metadata StreamMetadata) error {
				_, exists := newStreams[fmt.Sprintf("%s.%s", namespace, metadata.StreamName)]
				if exists {
					selectedStreams[namespace] = append(selectedStreams[namespace], metadata)
				}
				return nil
			})
		}
		newCatalog.SelectedStreams = selectedStreams
	}

	constantValue, prefix := getDestDBPrefix(oldCatalog.Streams)

	// merge streams metadata
	oldStreams := createStreamMap(oldCatalog)
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
// 1. Compares properties from selected_streams: normalization, partition_regex, filter, append_mode
// 2. Compares properties from streams: destination_database, destination_table, cursor_field, sync_mode
// 3. For now, any new stream present in new catalog is added to the difference. Later collision detection will happen.
//
// Parameters:
//   - oldStreams: The previous catalog to compare against
//   - newStreams: The current catalog with potential changes
//
// Returns:
//   - A catalog containing only the streams that have differences
// GetStreamsDelta compares two catalogs and returns a new catalog with streams that have differences.
func GetStreamsDelta(oldStreams, newStreams *Catalog) *Catalog {
	diffStreams := &Catalog{
		Streams:         []*ConfiguredStream{},
		SelectedStreams: make(map[string][]StreamMetadata),
	}

	if oldStreams == nil || newStreams == nil {
        return diffStreams
    }

	oldStreamsMap := make(map[string]*ConfiguredStream)
    for _, stream := range oldStreams.Streams {
        oldStreamsMap[stream.Stream.ID()] = stream
    }

    newStreamsMap := make(map[string]*ConfiguredStream)
    for _, stream := range newStreams.Streams {
        newStreamsMap[stream.Stream.ID()] = stream
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

			newStream, newExists := newStreamsMap[streamID]
			if !newExists {
				continue
			}

			oldMetadata, oldMetaExists := oldSelectedMap[streamID]
			oldStream, oldStreamExists := oldStreamsMap[streamID]

			// If new stream or previously unselected, treat as a difference
			if !oldMetaExists || !oldStreamExists {
				diffStreams.Streams = append(diffStreams.Streams, newStream)
				diffStreams.SelectedStreams[namespace] = append(diffStreams.SelectedStreams[namespace], newMetadata)
				continue
			}

			isDifferent := false

			// Helper to check for difference and log it
			check := func(field string, oldVal, newVal interface{}) {
                if !reflect.DeepEqual(oldVal, newVal) {
                    logger.Infof("Stream [%s] %s changed: %v -> %v", streamID, field, oldVal, newVal)
                    isDifferent = true
                }
            }

			// Compare metadata fields
            check("Normalization", oldMetadata.Normalization, newMetadata.Normalization)
            check("PartitionRegex", oldMetadata.PartitionRegex, newMetadata.PartitionRegex)
            check("Filter", oldMetadata.Filter, newMetadata.Filter)
            check("AppendMode", oldMetadata.AppendMode, newMetadata.AppendMode)

            // Compare stream fields
            check("SyncMode", oldStream.Stream.SyncMode, newStream.Stream.SyncMode)
            check("DestinationDatabase", oldStream.Stream.DestinationDatabase, newStream.Stream.DestinationDatabase)
            check("DestinationTable", oldStream.Stream.DestinationTable, newStream.Stream.DestinationTable)

            // Conditional check for CursorField
            if newStream.Stream.SyncMode == INCREMENTAL {
                check("CursorField", oldStream.Stream.CursorField, newStream.Stream.CursorField)
            }

            if isDifferent {
				// Copy stream to safely modify destination info
				newStreamCopy := *newStream.Stream
				deltaStream := &ConfiguredStream{Stream: &newStreamCopy}

				deltaStream.Stream.DestinationDatabase = oldStream.Stream.DestinationDatabase
				deltaStream.Stream.DestinationTable = oldStream.Stream.DestinationTable

				diffStreams.Streams = append(diffStreams.Streams, deltaStream)
				diffStreams.SelectedStreams[namespace] = append(diffStreams.SelectedStreams[namespace], newMetadata)
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
