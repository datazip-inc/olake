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

// destinationTypes is the canonical DataType -> destination type mapping. Every declared DataType
// must have an entry here (enforced by TestDeclaredTypesHaveExplicitIcebergMapping and
// TestDeclaredTypesHaveExplicitParquetMapping); the ToIceberg/ToNewParquet fallbacks are reserved
// for types that are not declared constants.
var destinationTypes = map[DataType]destinationType{
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

// icebergToDataType maps each iceberg type back to one canonical DataType — several DataTypes
// share the same iceberg type. IcebergTypeToDatatype's fallback is String.
var icebergToDataType = map[string]DataType{
	"boolean":     Bool,
	"int":         Int32,
	"long":        Int64,
	"float":       Float32,
	"double":      Float64,
	"timestamptz": TimestampMilli,
	"string":      String,
	"binary":      Binary,
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
	base, params := d.resolve()
	if mapping, ok := destinationTypes[base]; ok {
		return parquet.Optional(mapping.parquetNodeConstructor(params...))
	}
	return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)) // unregistered types travel as bytes
}

func (d DataType) ToIceberg() string {
	base, params := d.resolve()
	if mapping, ok := destinationTypes[base]; ok {
		return fmt.Sprintf(mapping.icebergType, params...)
	}
	return "string" // fallback for unregistered types
}

// ForLoadedState returns the type a column carries for the state version this sync is pinned at.
// Binary columns were carried as text before state version 8, so state written by such a build
// keeps them as String and an existing destination column does not change type on upgrade.
func ForLoadedState(d DataType) DataType {
	switch {
	case constants.LoadedStateVersion < 8 && (d == Binary || BaseOf(d) == FixedBinary):
		return String
	default:
		return d
	}
}

func IcebergTypeToDatatype(d string) DataType {
	if instance, ok := icebergInstance(d); ok {
		return instance
	}
	if dataType, ok := icebergToDataType[d]; ok {
		return dataType
	}
	return String // fallback for unregistered types
}
