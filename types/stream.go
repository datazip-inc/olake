package types

import (
	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/jsonschema/schema"
	"github.com/datazip-inc/olake/utils"
)

// Output Stream Object for dsynk
type Stream struct {
	Name                       string            `json:"name,omitempty"`
	Namespace                  string            `json:"namespace,omitempty"`
	Schema                     *TypeSchema       `json:"json_schema,omitempty"`
	SupportedSyncModes         *Set[SyncMode]    `json:"supported_sync_modes,omitempty"`
	SourceDefinedPrimaryKey    *Set[string]      `json:"source_defined_primary_key,omitempty"`
	SourceDefinedCursor        bool              `json:"source_defined_cursor"`
	DefaultCursorFields        *Set[string]      `json:"default_cursor_fields,omitempty"`
	AdditionalProperties       string            `json:"additional_properties,omitempty"`
	AdditionalPropertiesSchema schema.JSONSchema `json:"additional_properties_schema,omitempty"`
}

func NewStream(name, namespace string) *Stream {
	return &Stream{
		Name:                    name,
		Namespace:               namespace,
		SupportedSyncModes:      NewSet[SyncMode](),
		SourceDefinedPrimaryKey: NewSet[string](),
		DefaultCursorFields:     NewSet[string](),
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
		s.DefaultCursorFields.Insert(column)
	}

	s.SourceDefinedCursor = true
	return s
}

// Add or Update Column in Stream Type Schema
func (s *Stream) UpsertField(column string, typ DataType, nullable bool) {
	if s.Schema == nil {
		s.Schema = &TypeSchema{
			Properties: map[string]*Property{},
		}
	}

	property := &Property{
		Type: []DataType{typ},
	}

	if nullable {
		property.Type = append(property.Type, NULL)
	}

	s.Schema.Properties[column] = property
}

func (s *Stream) WithSchema(schema TypeSchema) *Stream {
	s.Schema = &schema
	return s
}

func (s *Stream) Wrap(batchSize int) *ConfiguredStream {
	return &ConfiguredStream{
		Stream:    s,
		SyncMode:  FULLREFRESH,
		batchSize: batchSize,
	}
}

func (s *Stream) UnmarshalJSON(data []byte) error {
	// Define a type alias to avoid recursion
	type Alias Stream

	// Create a temporary alias value to unmarshal into
	var temp Alias

	temp.DefaultCursorFields = NewSet[string]()
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
