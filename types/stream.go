package types

import (
	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/jsonschema/schema"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
)

// Output Stream Object for dsynk
type Stream struct {
	// Name of the Stream
	Name string `json:"name,omitempty"`
	// Namespace of the Stream, or Database it belongs to
	// helps in identifying collections with same name in different database
	Namespace string `json:"namespace,omitempty"`
	// Possible Schema of the Stream
	Schema *TypeSchema `json:"type_schema,omitempty"`
	// Supported sync modes from driver for the respective Stream
	SupportedSyncModes *Set[SyncMode] `json:"supported_sync_modes,omitempty"`
	// Primary key if available
	SourceDefinedPrimaryKey *Set[string] `json:"source_defined_primary_key,omitempty"`
	// Available cursor fields supported by driver
	AvailableCursorFields *Set[string] `json:"available_cursor_fields,omitempty"`
	// Input of JSON Schema from Client to be parsed by driver
	AdditionalProperties string `json:"additional_properties,omitempty"`
	// Renderable JSON Schema for additional properties supported by respective driver for individual stream
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
	SyncMode                   SyncMode          `json:"sync_mode,omitempty"` // Mode being used for syncing data
}

func NewStream(name, namespace string) *Stream {
	return &Stream{
		Name:                    name,
		Namespace:               namespace,
		SupportedSyncModes:      NewSet[SyncMode](),
		SourceDefinedPrimaryKey: NewSet[string](),
		AvailableCursorFields:   NewSet[string](),
		Schema:                  NewTypeSchema(),
	}
}

func (s *Stream) ID() string {
	return utils.StreamIdentifier(s.Name, s.Namespace)
}

func (s *Stream) WithSyncMode(modes ...SyncMode) *Stream {
	for _, mode := range modes {
		s.SupportedSyncModes.Insert(mode)
	}

	return s
}

func (s *Stream) WithPrimaryKey(keys ...string) *Stream {
	for _, key := range keys {
		s.SourceDefinedPrimaryKey.Insert(key)
	}

	return s
}

func (s *Stream) WithCursorField(columns ...string) *Stream {
	for _, column := range columns {
		s.AvailableCursorFields.Insert(column)
	}

	return s
}

func (s *Stream) WithSchema(schema *TypeSchema) *Stream {
	s.Schema = schema
	return s
}

// Add or Update Column in Stream Type Schema
func (s *Stream) UpsertField(column string, typ DataType, nullable bool) {
	types := []DataType{typ}
	if nullable {
		types = append(types, Null)
	}

	s.Schema.AddTypes(column, types...)
}

func (s *Stream) Wrap(_ int) *ConfiguredStream {
	return &ConfiguredStream{
		Stream: s,
	}
}

func (s *Stream) UnmarshalJSON(data []byte) error {
	// Define a type alias to avoid recursion
	type Alias Stream

	// Create a temporary alias value to unmarshal into
	var temp Alias

	temp.AvailableCursorFields = NewSet[string]()
	temp.SourceDefinedPrimaryKey = NewSet[string]()
	temp.SupportedSyncModes = NewSet[SyncMode]()

	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	*s = Stream(temp)
	return nil
}

func StreamsToMap(streams ...*Stream) map[string]*Stream {
	output := make(map[string]*Stream)
	for _, stream := range streams {
		output[stream.ID()] = stream
	}

	return output
}

func LogCatalog(streams []*Stream) {
	message := Message{
		Type:    CatalogMessage,
		Catalog: GetWrappedCatalog(streams),
	}
	logger.Info(message)
	// write catalog to the specified file
	err := logger.FileLogger(message.Catalog, "streams", ".json")
	if err != nil {
		logger.Fatalf("failed to create streams file: %s", err)
	}
}
