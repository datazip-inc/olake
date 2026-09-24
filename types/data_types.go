package types

import (
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination/iceberg/proto"
	"github.com/parquet-go/parquet-go"
)

type DataType string

const (
	Null           DataType = "null"
	Int32          DataType = "integer_small"
	Int64          DataType = "integer"
	Float32        DataType = "number_small"
	Float64        DataType = "number"
	String         DataType = "string"
	Bool           DataType = "boolean"
	Object         DataType = "object"
	Array          DataType = "array"
	Unknown        DataType = "unknown"
	Timestamp      DataType = "timestamp"
	TimestampMilli DataType = "timestamp_milli" // storing datetime up to 3 precisions
	TimestampMicro DataType = "timestamp_micro" // storing datetime up to 6 precisions
	TimestampNano  DataType = "timestamp_nano"  // storing datetime up to 9 precisions
	Binary         DataType = "binary"
	FixedBinary    DataType = "fixed_binary(%d)"
)

var RawSchema = map[string]DataType{
	constants.StringifiedData: String,
	constants.CdcTimestamp:    Timestamp,
	constants.OlakeTimestamp:  Timestamp,
	constants.OpType:          String,
	constants.OlakeID:         String,
}

type destinationTypesMap map[DataType]destinationType

// get returns the destination type a DataType maps to, along with any parameters it carries.
func (m destinationTypesMap) get(d DataType) (destinationType, []any, bool) {
	if mapping, ok := m[d]; ok {
		return mapping, nil, true
	}

	family, params := instanceOf(d)
	if family == nil {
		return destinationType{}, nil, false
	}
	mapping, ok := m[family.pattern]
	if !ok {
		return destinationType{}, nil, false
	}
	return mapping, asAny(params), true
}

// destinationTypes is the canonical DataType -> destination type mapping. Every declared DataType
// must have an entry here (enforced by unit tests)
var destinationTypes = destinationTypesMap{
	Bool:           {"boolean", leafNode(parquet.BooleanType)},
	Int32:          {"int", leafNode(parquet.Int32Type)},
	Int64:          {"long", leafNode(parquet.Int64Type)},
	Float32:        {"float", leafNode(parquet.FloatType)},
	Float64:        {"double", leafNode(parquet.DoubleType)},
	String:         {"string", plainNode(parquet.String)},
	Timestamp:      {"timestamptz", timestampNode}, // timestamptz as we use default utc
	TimestampMilli: {"timestamptz", timestampNode},
	TimestampMicro: {"timestamptz", timestampNode},
	TimestampNano:  {"timestamptz", timestampNode},
	Object:         {"string", plainNode(parquet.String)}, // nested structures are serialized as strings
	Array:          {"string", plainNode(parquet.String)},
	Binary:         {"binary", leafNode(parquet.ByteArrayType)},
	FixedBinary:    {"fixed[%d]", fixedBinaryNode}, // the pattern is a registry key; only an instance renders
}

type icebergToDataTypeMap map[string]DataType

// get returns the DataType an iceberg type denotes.
func (m icebergToDataTypeMap) get(icebergType string) (DataType, bool) {
	if dataType, declared := m[icebergType]; declared {
		return dataType, true
	}
	family, params := icebergInstanceOf(icebergType)
	if family == nil {
		return "", false
	}
	pattern, registered := m[family.icebergPattern()]
	if !registered {
		return "", false
	}
	return pattern.Of(asAny(params)...), true
}

// icebergToDataType maps each iceberg type back to one canonical DataType — several DataTypes
// share the same iceberg type. A family is registered under its pattern, the same key its
// destination mapping produces. IcebergTypeToDatatype's fallback is String.
var icebergToDataType = icebergToDataTypeMap{
	"boolean":     Bool,
	"int":         Int32,
	"long":        Int64,
	"float":       Float32,
	"double":      Float64,
	"timestamptz": TimestampMilli,
	"string":      String,
	"binary":      Binary,
	"fixed[%d]":   FixedBinary,
}

type Record map[string]any

type RawRecord struct {
	Data         map[string]any `json:"data"`
	OlakeColumns map[string]any `json:"olake_columns"`
}

func CreateRawRecord(data map[string]any, olakeColumns map[string]any) RawRecord {
	return RawRecord{
		Data:         data,
		OlakeColumns: olakeColumns,
	}
}

// returns raw schema in iceberg format
func GetIcebergRawSchema() []*proto.IcebergPayload_SchemaField {
	var icebergFields []*proto.IcebergPayload_SchemaField
	for key, typ := range RawSchema {
		icebergFields = append(icebergFields, &proto.IcebergPayload_SchemaField{
			IceType: typ.ToIceberg(),
			Key:     key,
		})
	}
	return icebergFields
}

// ToNewParquet returns the parquet node for d, always optional so the field is nullable.
func (d DataType) ToNewParquet() parquet.Node {
	mapping, params, registered := destinationTypes.get(d)
	if !registered {
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)) // unregistered types travel as bytes
	}
	return parquet.Optional(mapping.parquetNodeConstructor(params...))
}

func (d DataType) ToIceberg() string {
	mapping, params, registered := destinationTypes.get(d)
	switch {
	case !registered:
		return "string" // fallback for unregistered types
	case len(params) == 0:
		return mapping.icebergType
	default:
		return fmt.Sprintf(mapping.icebergType, params...)
	}
}

func IcebergTypeToDatatype(d string) DataType {
	if dataType, registered := icebergToDataType.get(d); registered {
		return dataType
	}
	return String // fallback for unregistered types
}
