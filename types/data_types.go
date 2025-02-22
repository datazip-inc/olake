package types

import (
	"github.com/parquet-go/parquet-go"
)

type DataType string

const (
	Null           DataType = "null"
	Int64          DataType = "integer"
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

type Record map[string]any

type RawRecord struct {
	OlakeID        string         `parquet:"olake_id"`
	Data           map[string]any `parquet:"data,json"`
	DeleteTime     int64          `parquet:"cdc_deleted_at"`
	OlakeTimestamp int64          `parquet:"olake_insert_time"`
}

func CreateRawRecord(olakeID string, data map[string]any, deleteAt int64) RawRecord {
	return RawRecord{
		OlakeID:    olakeID,
		Data:       data,
		DeleteTime: deleteAt,
	}
}
func (d DataType) ToNewParquet() parquet.Node {
	var n parquet.Node

	switch d {
	case Int64:
		n = parquet.Leaf(parquet.Int64Type)
	case Float64:
		n = parquet.Leaf(parquet.DoubleType)
	case String:
		n = parquet.String()
	case Bool:
		n = parquet.Leaf(parquet.BooleanType)
	case Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		n = parquet.Leaf(parquet.Int64Type)
		n = parquet.Encoded(n, &parquet.Plain)
	case Object, Array:
		// Ensure proper handling of nested structures
		n = parquet.String()
	default:
		n = parquet.Leaf(parquet.ByteArrayType)
	}

	n = parquet.Optional(n) // Ensure the field is nullable
	return n
}
