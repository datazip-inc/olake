package types

import (
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
)

// Tree Representation of TypeWeights
// 								 10 (Binary)
// 									 /
//									/
//                              5 (String)
//                            /       	   \
//             3 (Float64)   /              \ 9 (TimestampNano)
//                         /  \             /
//             2 (Int64)  /    \4(Float32) / 8 (TimestampMicro)
//                       /                /
//            1 (Int32) /                / 7 (TimestampMilli)
//                     /                /
//           0 (Bool) /                / 6 (Timestamp)
//

var TypeWeights = map[DataType]int{
	Bool:           0,
	Int32:          1,
	Int64:          2,
	Float64:        3,
	Float32:        4,
	String:         5,
	TimestampNano:  9,
	TimestampMicro: 8,
	TimestampMilli: 7,
	Timestamp:      6,
	Binary:         10,
}

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
	String:         {"string", parquet.String},
	Timestamp:      {"timestamptz", timestampNode}, // timestamptz as we use default utc
	TimestampMilli: {"timestamptz", timestampNode},
	TimestampMicro: {"timestamptz", timestampNode},
	TimestampNano:  {"timestamptz", timestampNode},
	Object:         {"string", parquet.String}, // nested structures are serialized as strings
	Array:          {"string", parquet.String},
	Binary:         {"binary", leafNode(parquet.ByteArrayType)},
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

func (d DataType) ToNewParquet() parquet.Node {
	construct := func() parquet.Node { return parquet.Leaf(parquet.ByteArrayType) } // fallback for unregistered types
	if mapping, ok := destinationTypes[d]; ok && mapping.parquetNode != nil {
		construct = mapping.parquetNode
	}
	return parquet.Optional(construct()) // Ensure the field is nullable
}

func (d DataType) ToIceberg() string {
	if mapping, ok := destinationTypes[d]; ok && mapping.icebergType != "" {
		return mapping.icebergType
	}
	return "string" // fallback for unregistered types
}

func IcebergTypeToDatatype(d string) DataType {
	if dataType, ok := icebergToDataType[d]; ok {
		return dataType
	}
	return String // fallback for unregistered types
}
