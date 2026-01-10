package types

import (
	"fmt"
	"sync"

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
	column = utils.Ternary(t.HasDestinationColumnName(), column, utils.Reformat(column)).(string)
	p, found := t.Properties.Load(column)
	if !found {
		return "", fmt.Errorf("column [%s] missing from type schema", column)
	}
	return p.(*Property).DataType(), nil
}

func (t *TypeSchema) AddTypes(column string, types ...DataType) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, found := t.Properties.Load(column)
	if !found {
		t.Properties.Store(column, &Property{
			Type:                  NewSet(types...),
			DestinationColumnName: utils.Reformat(column),
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
		prop := value.(*Property)
		groupNode[prop.getDestinationColumnName(key.(string))] = prop.DataType().ToNewParquet()
		return true
	})

	return parquet.NewSchema("olake_schema", groupNode)
}

func (t *TypeSchema) ToIceberg() []*proto.IcebergPayload_SchemaField {
	var icebergFields []*proto.IcebergPayload_SchemaField
	t.Properties.Range(func(key, value interface{}) bool {
		prop := value.(*Property)
		icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
			IceType: prop.DataType().ToIceberg(),
			Key:     prop.getDestinationColumnName(key.(string)),
		})
		return true
	})

	return icebergFields
}

// FilterSchemaBySelectedColumns filters a schema to only include selected columns.
// Used for filtering destination table schemas during table creation.
// Returns filtered schema if some columns are selected, otherwise returns the original schema if all columns are selected or no selection is provided.
func FilterSchemaBySelectedColumns(schema *TypeSchema, selectedMap map[string]struct{}, allSelected bool) *TypeSchema {
	if len(selectedMap) == 0 {
		return schema
	}
	if allSelected {
		return schema
	}

	filtered := NewTypeSchema()
	schema.Properties.Range(func(key, value interface{}) bool {
		colName, ok := key.(string)
		if !ok {
			return true
		}
		if _, exists := selectedMap[colName]; exists {
			filtered.Properties.Store(colName, value)
		}
		return true
	})
	return filtered
}

func (t *TypeSchema) HasDestinationColumnName() bool {
	found := false
	t.Properties.Range(func(_, value interface{}) bool {
		found = value.(*Property).DestinationColumnName != ""
		return true
	})
	return found
}

// Property is a dto for catalog properties representation
type Property struct {
	Type                  *Set[DataType] `json:"type,omitempty"`
	DestinationColumnName string         `json:"destination_column_name,omitempty"`
}

// returns datatype according to typecast tree if multiple type present
func (p *Property) DataType() DataType {
	types := p.Type.Array()
	// remove null, to not mess up with tree
	i, found := utils.ArrayContains(types, func(elem DataType) bool {
		return elem == Null
	})
	if found {
		types = append(types[:i], types[i+1:]...)
	}

	// if only null was present
	if len(types) == 0 {
		return Null
	}

	// get Common Ancestor
	commonType := types[0]
	for idx := 1; idx < len(types); idx++ {
		commonType = GetCommonAncestorType(commonType, types[idx])
	}
	return commonType
}

func (p *Property) Nullable() bool {
	_, found := utils.ArrayContains(p.Type.Array(), func(elem DataType) bool {
		return elem == Null
	})

	return found
}

func (p *Property) getDestinationColumnName(key string) string {
	return utils.Ternary(p.DestinationColumnName != "", p.DestinationColumnName, key).(string)
}

// Tree that is being used for typecasting

type typeNode struct {
	t     DataType
	left  *typeNode
	right *typeNode
}

var typecastTree = &typeNode{
	t: String,
	left: &typeNode{
		t: Float64,
		left: &typeNode{
			t: Int64,
			left: &typeNode{
				t: Int32,
				left: &typeNode{
					t: Bool,
				},
			},
		},
		right: &typeNode{
			t: Float32,
		},
	},
	right: &typeNode{
		t: TimestampNano,
		left: &typeNode{
			t: TimestampMicro,
			left: &typeNode{
				t: TimestampMilli,
				left: &typeNode{
					t: Timestamp,
				},
			},
		},
	},
}

// GetCommonAncestorType returns lowest common ancestor type
func GetCommonAncestorType(t1, t2 DataType) DataType {
	return lowestCommonAncestor(typecastTree, t1, t2)
}

func lowestCommonAncestor(
	root *typeNode,
	t1, t2 DataType,
) DataType {
	node := root

	for node != nil {
		wt1, t1Exist := TypeWeights[t1]
		wt2, t2Exist := TypeWeights[t2]
		rootW, rootExist := TypeWeights[node.t]

		if !rootExist {
			return Unknown
		}

		// If any type is not found in weights map, return Unknown
		if !t1Exist || !t2Exist {
			return node.t
		}

		if wt1 > rootW && wt2 > rootW {
			// If both t1 and t2 have greater weights than parent
			node = node.right
		} else if wt1 < rootW && wt2 < rootW {
			// If both t1 and t2 have lesser weights than parent
			node = node.left
		} else {
			// We have found the split point, i.e. the LCA node.
			return node.t
		}
	}
	return Unknown
}
