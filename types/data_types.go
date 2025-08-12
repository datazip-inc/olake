package types

import (
	"time"

	"github.com/datazip-inc/olake/constants"
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
)

// Tree Representation of TypeWeights
//
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
}

var RawSchema = map[string]DataType{
	constants.StringifiedData: String,
	constants.CdcTimestamp:    Timestamp,
	constants.OlakeTimestamp:  Timestamp,
	constants.OpType:          String,
}

type Record map[string]any

type RawRecord struct {
	Data           map[string]any `parquet:"data,json"`
	OlakeID        string         `parquet:"_olake_id"`
	OlakeTimestamp time.Time      `parquet:"_olake_timestamp"`
	OperationType  string         `parquet:"_op_type"` // "r" for read/backfill, "c" for create, "u" for update, "d" for delete
	CdcTimestamp   time.Time      `parquet:"_cdc_timestamp"`
}

func CreateRawRecord(olakeID string, data map[string]any, operationType string, cdcTimestamp time.Time) RawRecord {
	return RawRecord{
		OlakeID:       olakeID,
		Data:          data,
		OperationType: operationType,
		CdcTimestamp:  cdcTimestamp,
	}
}

type Metadata struct {
	DestTableName string `json:"dest_table_name"`
	ThreadID      string `json:"thread_id"`
	PrimaryKey    string `json:"primary_key,omitempty"` // omitempty because it's not in commit type
}

type IceColumn struct {
	IceType string `json:"ice_type"`
	Key     string `json:"key"`
	Value   any    `json:"value"`
}

type IceRecord struct {
	Record     []IceColumn `json:"record"`
	RecordType string      `json:"record_type"` // u/c/r
}

type IcebergWriterPayload struct {
	Type     string      `json:"type"` // "records" or "commit"
	Metadata Metadata    `json:"metadata"`
	Records  []IceRecord `json:"records,omitempty"` // omitempty because it's not in commit type
}

func (d DataType) ToNewParquet() parquet.Node {
	var n parquet.Node

	switch d {
	case Int32:
		n = parquet.Leaf(parquet.Int32Type)
	case Float32:
		n = parquet.Leaf(parquet.FloatType)
	case Int64:
		n = parquet.Leaf(parquet.Int64Type)
	case Float64:
		n = parquet.Leaf(parquet.DoubleType)
	case String:
		n = parquet.String()
	case Bool:
		n = parquet.Leaf(parquet.BooleanType)
	case Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		n = parquet.Timestamp(parquet.Microsecond)
	case Object, Array:
		// Ensure proper handling of nested structures
		n = parquet.String()
	default:
		n = parquet.Leaf(parquet.ByteArrayType)
	}

	n = parquet.Optional(n) // Ensure the field is nullable
	return n
}

func (d DataType) ToIceberg() string {
	switch d {
	case Bool:
		return "boolean"
	case Int32:
		return "int"
	case Int64:
		return "long"
	case Float32:
		return "float"
	case Float64:
		return "double"
	case Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		return "timestamptz" // use with timezone as we use default utc
	default:
		return "string"
	}
}
