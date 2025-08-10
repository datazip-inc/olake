package types

import (
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/datazip-inc/olake/utils"
	"github.com/goccy/go-json"
	"github.com/parquet-go/parquet-go"
)

type TypeSchema struct {
	mu         sync.Mutex
	Properties sync.Map `json:"-"`
}

func NewTypeSchema() *TypeSchema {
	return &TypeSchema{
		mu:         sync.Mutex{},
		Properties: sync.Map{},
	}
}

func (t *TypeSchema) Override(fields map[string]*Property) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for key, value := range fields {
		stored, loaded := t.Properties.LoadAndDelete(key)
		if loaded && stored.(*Property).Nullable() {
			value.Type.Insert(Null)
		}
		t.Properties.Store(key, value)
	}
}

// MarshalJSON custom marshaller to handle sync.Map encoding
func (t *TypeSchema) MarshalJSON() ([]byte, error) {
	// Create a map to temporarily store data for JSON marshaling
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
		t.Properties.Store(column, &Property{
			Type: NewSet(types...),
		})
		return
	}

	property := p.(*Property)
	property.Type.Insert(types...)
}

func (t *TypeSchema) GetProperty(column string) (bool, *Property) {
	p, found := t.Properties.Load(column)
	if !found {
		return false, nil
	}

	return true, p.(*Property)
}

func (t *TypeSchema) ToParquet() *parquet.Schema {
	groupNode := parquet.Group{}
	t.Properties.Range(func(key, value interface{}) bool {
		groupNode[key.(string)] = value.(*Property).DataType().ToNewParquet()
		return true
	})

	return parquet.NewSchema("olake_schema", groupNode)
}

func (t *TypeSchema) ToIceberg() []*proto.IcebergPayload_SchemaField {
	var icebergFields []*proto.IcebergPayload_SchemaField
	t.Properties.Range(func(key, value interface{}) bool {
		icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
			IceType: value.(*Property).DataType().ToIceberg(),
			Key:     key.(string),
		})
		return true
	})

	return icebergFields
}

// returns raw schema in iceberg format
func RawSchema() []*proto.IcebergPayload_SchemaField {
	var icebergFields []*proto.IcebergPayload_SchemaField
	icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
		IceType: String.ToIceberg(),
		Key:     constants.OlakeID,
	})
	icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
		IceType: Timestamp.ToIceberg(),
		Key:     constants.OlakeTimestamp,
	})
	icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
		IceType: Timestamp.ToIceberg(),
		Key:     constants.CdcTimestamp,
	})
	icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
		IceType: String.ToIceberg(),
		Key:     constants.OpType,
	})
	icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
		IceType: String.ToIceberg(),
		Key:     constants.StringifiedData,
	})
	return icebergFields
}

// Property is a dto for catalog properties representation
type Property struct {
	Type *Set[DataType] `json:"type,omitempty"`
	// TODO: Decide to keep in the Protocol Or Not
	// Format string     `json:"format,omitempty"`
}

func (p *Property) DataType() DataType {
	types := p.Type.Array()
	i, found := utils.ArrayContains(types, func(elem DataType) bool {
		return elem != Null
	})
	if !found {
		return Null
	}

	return types[i]
}

func (p *Property) Nullable() bool {
	_, found := utils.ArrayContains(p.Type.Array(), func(elem DataType) bool {
		return elem == Null
	})

	return found
}
