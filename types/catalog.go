package types

import (
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/utils"
	"github.com/goccy/go-json"
	"github.com/xitongsys/parquet-go/parquet"
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

// ConfiguredCatalog is a dto for formatted airbyte catalog serialization
type Catalog struct {
	Streams []*ConfiguredStream `json:"streams,omitempty"`
}

// TypeSchema is a DTO for Airbyte catalog schema object serialization
type TypeSchema struct {
	Properties sync.Map
}

func NewTypeSchema() *TypeSchema {
	return &TypeSchema{
		Properties: sync.Map{},
	}
}

// MarshalJSON custom marshaller to handle sync.Map encoding
func (t *TypeSchema) MarshalJSON() ([]byte, error) {
	// Create a map to temporarily store data for JSON marshalling
	propertiesMap := make(map[string]*Property)
	t.Properties.Range(func(key, value interface{}) bool {
		strKey, ok := key.(string)
		if !ok {
			return false
		}
		prop, ok := value.(*Property)
		if !ok {
			return false
		}
		propertiesMap[strKey] = prop
		return true
	})

	// Create an alias to avoid infinite recursion
	type Alias TypeSchema
	return json.Marshal(&struct {
		*Alias
		Properties map[string]*Property `json:"properties,omitempty"`
	}{
		Alias:      (*Alias)(t),
		Properties: propertiesMap,
	})
}

// UnmarshalJSON custom unmarshaller to handle sync.Map decoding
func (t *TypeSchema) UnmarshalJSON(data []byte) error {
	// Create a temporary structure to unmarshal JSON into
	type Alias TypeSchema
	aux := &struct {
		*Alias
		Properties map[string]*Property `json:"properties,omitempty"`
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Populate sync.Map with the data from temporary map
	for key, value := range aux.Properties {
		t.Properties.Store(key, value)
	}

	return nil
}

func (t *TypeSchema) GetType(column string) (DataType, error) {
	p, found := t.Properties.Load(column)
	if !found {
		return "", fmt.Errorf("column [%s] missing from type schema", column)
	}

	return p.(*Property).DataType(), nil
}

func (t *TypeSchema) AddTypes(column string, types ...DataType) {
	p, found := t.Properties.Load(column)
	if !found {
		p = &Property{
			Type: types,
		}
		t.Properties.Store(column, p)
		return
	}

	property := p.(*Property)
	property.Type = append(property.Type, types...)
}

func (t *TypeSchema) GetProperty(column string) (*Property, error) {
	p, found := t.Properties.Load(column)
	if !found {
		return nil, fmt.Errorf("column [%s] missing from type schema", column)
	}

	return p.(*Property), nil
}

func (t *TypeSchema) GetLevelZeroFlattenedParquetSchemaForJsonTypes() []string {

	sc := []string{}
	t.Properties.Range(func(key, value interface{}) bool {
		col := key.(string)
		minType, convType := value.(*Property).DataType().getParquetEquivalent()
		ftag := fmt.Sprintf("name=%s, type=%s, repetitiontype=OPTIONAL", col, minType)
		if convType != -1 {
			if value.(*Property).DataType().stringificationNeededForJsonTypes() {
				convType = parquet.ConvertedType_UTF8
			}
			ftag = fmt.Sprintf("name=%s, type=%s, convertedtype=%s, repetitiontype=OPTIONAL", col, minType, convType)
		}
		sc = append(sc, ftag)
		return true
	})
	return sc
}

// Property is a dto for catalog properties representation
type Property struct {
	Type []DataType `json:"type,omitempty"`
	// TODO: Decide to keep in the Protocol Or Not
	// Format string     `json:"format,omitempty"`
}

func (p *Property) DataType() DataType {
	i, found := utils.ArrayContains(p.Type, func(elem DataType) bool {
		return elem != NULL
	})
	if !found {
		return NULL
	}

	return p.Type[i]
}

func (p *Property) Nullable() bool {
	_, found := utils.ArrayContains(p.Type, func(elem DataType) bool {
		return elem == NULL
	})

	return found
}

func GetWrappedCatalog(streams []*Stream) *Catalog {
	catalog := &Catalog{
		Streams: []*ConfiguredStream{},
	}

	for _, stream := range streams {
		catalog.Streams = append(catalog.Streams, &ConfiguredStream{
			Stream: stream,
		})
	}

	return catalog
}
