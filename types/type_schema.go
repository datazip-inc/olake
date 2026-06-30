package types

import (
	"fmt"
	"sort"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
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

// ColumnNames returns the list of column names currently present in the schema.
// Note: ordering is not guaranteed because sync.Map iteration order is not defined.
func (t *TypeSchema) ColumnNames() []string {
	var columns []string
	t.Properties.Range(func(col, _ interface{}) bool {
		if colName, ok := col.(string); ok {
			columns = append(columns, colName)
		}
		return true
	})
	return columns
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

func (t *TypeSchema) AddTypes(column string, isOlakeColumn bool, types ...DataType) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p, found := t.Properties.Load(column)
	if !found {
		t.Properties.Store(column, &Property{
			Type:                  NewSet(types...),
			DestinationColumnName: utils.Reformat(column), // Used to render UI column names when source column names are disabled.
			OlakeColumn:           isOlakeColumn,
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

func (t *TypeSchema) ToParquet(defaultColumns bool, stream StreamInterface) *parquet.Schema {
	// keeping default columns parquet schema for backward compatibility for olake columns
	groupNode := parquet.Group{
		constants.OlakeID:        parquet.String(),
		constants.OlakeTimestamp: parquet.Timestamp(parquet.Microsecond),
		constants.OpType:         parquet.String(),
		constants.CdcTimestamp:   parquet.Optional(parquet.Timestamp(parquet.Microsecond)),
	}
	isSelected := stream.IsSelectedColumn()

	t.Properties.Range(func(key, value interface{}) bool {
		prop := value.(*Property)
		colName := key.(string)
		outName := stream.ResolveColumnName(colName)
		if !isSelected(outName) || (defaultColumns && !prop.OlakeColumn) {
			return true
		}
		groupNode[outName] = prop.DataType().ToNewParquet()
		return true
	})

	if defaultColumns {
		groupNode[constants.StringifiedData] = parquet.JSON()
	}

	return parquet.NewSchema("olake_schema", groupNode)
}

// ToArrow builds the arrow schema written by the parquet destination. It mirrors
// ToParquet column-for-column — same OLake system columns, same selection and
// defaultColumns rules, same stringified "data" column when defaultColumns is set —
// so the arrow pipeline reproduces the physical schema the legacy writer produced.
// Fields are emitted in a stable alphabetical order.
func (t *TypeSchema) ToArrow(defaultColumns bool, stream StreamInterface) *arrow.Schema {
	// Default OLake system columns, mirroring ToParquet's base group. A schema
	// property of the same name overrides these below (and becomes nullable),
	// exactly as the parquet.Group map overwrite does.
	colTypes := map[string]arrow.DataType{
		constants.OlakeID:        arrow.BinaryTypes.String,
		constants.OlakeTimestamp: arrowTimestampUTC,
		constants.OpType:         arrow.BinaryTypes.String,
		constants.CdcTimestamp:   arrowTimestampUTC,
	}
	nullable := map[string]bool{
		constants.OlakeID:        false,
		constants.OlakeTimestamp: false,
		constants.OpType:         false,
		constants.CdcTimestamp:   true,
	}

	isSelected := stream.IsSelectedColumn()
	t.Properties.Range(func(key, value any) bool {
		prop := value.(*Property)
		outName := stream.ResolveColumnName(key.(string))
		if !isSelected(outName) || (defaultColumns && !prop.OlakeColumn) {
			return true
		}
		colTypes[outName] = prop.DataType().ToArrow()
		nullable[outName] = true // ToNewParquet makes every data column optional
		return true
	})

	if defaultColumns {
		colTypes[constants.StringifiedData] = arrow.BinaryTypes.String
		nullable[constants.StringifiedData] = false // parquet.JSON() is required
	}

	names := make([]string, 0, len(colTypes))
	for name := range colTypes {
		names = append(names, name)
	}
	sort.Strings(names)

	fields := make([]arrow.Field, 0, len(names))
	for _, name := range names {
		field := arrow.Field{Name: name, Type: colTypes[name], Nullable: nullable[name]}
		// Preserve the JSON logical type for the denormalized "data" column, matching
		// ToParquet's parquet.JSON(). Only set in defaultColumns mode, where "data" is
		// unambiguously the stringified column we add below (never a real source column).
		if defaultColumns && name == constants.StringifiedData {
			field.Metadata = arrow.MetadataFrom(map[string]string{"PARQUET:logical": "JSON"})
		}
		fields = append(fields, field)
	}
	return arrow.NewSchema(fields, nil)
}

func (t *TypeSchema) ToIceberg(defaultColumns bool, stream StreamInterface, includeColumns ...string) []*proto.IcebergPayload_SchemaField {
	var icebergFields []*proto.IcebergPayload_SchemaField
	isSelected := stream.IsSelectedColumn()

	// build a lookup set so partition columns can be included even in defaultColumns mode
	includeSet := NewSet[string](includeColumns...)

	t.Properties.Range(func(key, value interface{}) bool {
		prop := value.(*Property)
		colName := key.(string)
		outName := stream.ResolveColumnName(colName)
		// skip non-olake columns in defaultColumns mode unless explicitly included (e.g. partition columns)
		if !isSelected(outName) || (defaultColumns && !prop.OlakeColumn && !includeSet.Exists(colName)) {
			return true
		}
		icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
			IceType: prop.DataType().ToIceberg(),
			Key:     outName,
		})
		return true
	})

	if defaultColumns {
		icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
			IceType: "string",
			Key:     constants.StringifiedData,
		})
	}

	return icebergFields
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
	OlakeColumn           bool           `json:"olake_column,omitempty"`
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
