package types

import (
	"time"

	"github.com/fraugster/parquet-go/parquet"
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

type DriverRecord struct {
	OlakeID    string     `json:"olake_id"`
	Data       Record     `json:"data"`
	DeleteTime *time.Time `json:"cdc_deleted_at,omitempty"`
}

func CreateRecord(olakeID string, data map[string]any, deleteAt *time.Time) DriverRecord {
	return DriverRecord{
		OlakeID:    olakeID,
		Data:       data,
		DeleteTime: deleteAt,
	}
}

// returns parquet equivalent type & convertedType for the datatype
func (d DataType) ToParquet() *parquet.SchemaElement {
	switch d {
	case Int64:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case Float64:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_DOUBLE),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case String:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_UTF8),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case Bool:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BOOLEAN),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	//TODO: Not able to generate correctly in parquet, handle later
	case Timestamp:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case TimestampMilli:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	//TODO: Not able to generate correctly in parquet, handle later
	case TimestampNano:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case Object, Array: // Objects/Arrays are turned into String in parquet
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_UTF8),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	default:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_JSON),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	}
}
