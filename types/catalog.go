package types

import "fmt"

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

// Log is a dto for airbyte logs serialization
type Log struct {
	Level   string `json:"level,omitempty"`
	Message string `json:"message,omitempty"`
}

// StatusRow is a dto for airbyte result status serialization
type StatusRow struct {
	Status  ConnectionStatus `json:"status,omitempty"`
	Message string           `json:"message,omitempty"`
}

type StreamMetadata struct {
	ChunkColumn    string `json:"chunk_column,omitempty"`
	PartitionRegex string `json:"partition_regex"`
	StreamName     string `json:"stream_name"`
	AppendMode     bool   `json:"append_mode,omitempty"`
	Normalization  bool   `json:"normalization" default:"false"`
}

// ConfiguredCatalog is a dto for formatted airbyte catalog serialization
type Catalog struct {
	SelectedStreams map[string][]StreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream         `json:"streams,omitempty"`
}

func GetWrappedCatalog(streams []*Stream) *Catalog {
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
			StreamName:     stream.Name,
			PartitionRegex: "",
			AppendMode:     false,
		})
	}

	return catalog
}

// createStreamMap creates a map of streams for quick lookup by stream ID
func createStreamMap(catalog *Catalog) map[string]*ConfiguredStream {
	streamMap := make(map[string]*ConfiguredStream)
	for _, stream := range catalog.Streams {
		streamMap[stream.Stream.ID()] = stream
	}
	return streamMap
}

// MergeCatalogs merges old catalog with new catalog based on the following rules:
// 1. SelectedStreams: Retain only streams present in both oldCatalog.SelectedStreams and newStreamMap
// 2. SyncMode: Use from oldCatalog if the stream exists in old catalog
// 3. Everything else: Keep as new catalog
func mergeCatalogs(oldCatalog, newCatalog *Catalog) *Catalog {

	oldStreamMap := createStreamMap(oldCatalog)
	// Filter selected streams to only include those in new catalog
	if oldCatalog.SelectedStreams != nil {
		newStreamMap := createStreamMap(newCatalog)
		filtered := make(map[string][]StreamMetadata)
		for namespace, stream := range oldCatalog.SelectedStreams {
			for _, metadata := range stream {
				if _, exists := newStreamMap[fmt.Sprintf("%s.%s", namespace, metadata.StreamName)]; exists {
					filtered[namespace] = append(filtered[namespace], metadata)
				}
			}
		}
		newCatalog.SelectedStreams = filtered
	}
	// Preserve sync modes from old catalog
	for _, newStream := range newCatalog.Streams {
		if oldStream, exists := oldStreamMap[newStream.Stream.ID()]; exists {
			newStream.Stream.SyncMode = oldStream.Stream.SyncMode
		}
	}
	return newCatalog
}
