package types

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
	SplitColumn    string `json:"split_column"`
	PartitionRegex string `json:"partition_regex"`
	StreamName     string `json:"stream_name"`
}

// ConfiguredCatalog is a dto for formatted airbyte catalog serialization
type Catalog struct {
	SelectedStreams map[string][]StreamMetadata `json:"selected_streams,omitempty"`
	Streams         []*ConfiguredStream         `json:"streams,omitempty"`
	DefaultMode       SyncMode                  `json:"default_mode,omitempty"`
}

func GetWrappedCatalog(catalog *Catalog) *Catalog {
	if catalog == nil {
		return nil
	}

	wrappedCatalog := &Catalog{
		DefaultMode: catalog.DefaultMode,
	}

	for _, stream := range catalog.Streams {
		wrappedCatalog.Streams = append(wrappedCatalog.Streams, stream)
	}

	return wrappedCatalog
}
