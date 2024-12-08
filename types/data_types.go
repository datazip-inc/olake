package types

import (
	"github.com/xitongsys/parquet-go/parquet"
)

type DataType string

const (
	NULL            DataType = "null"
	INT64           DataType = "integer"
	FLOAT64         DataType = "number"
	STRING          DataType = "string"
	BOOL            DataType = "boolean"
	OBJECT          DataType = "object"
	ARRAY           DataType = "array"
	UNKNOWN         DataType = "unknown"
	TIMESTAMP       DataType = "timestamp"
	TIMESTAMP_MILLI DataType = "timestamp_milli" // storing datetime upto 3 precisions
	TIMESTAMP_MICRO DataType = "timestamp_micro" // storing datetime upto 6 precisions
	TIMESTAMP_NANO  DataType = "timestamp_nano"  // storing datetime upto 9 precisions
)

type Record map[string]any

// returns parquet equivalent type & convertedType for the datatype
func (d DataType) ToParquet() *parquet.SchemaElement {
	switch d {
	case INT64:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case FLOAT64:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_FLOAT),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case STRING:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BYTE_ARRAY),
			ConvertedType:  ToPointer(parquet.ConvertedType_UTF8),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case BOOL:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_BOOLEAN),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	//TODO: Not able to generate correctly in parquet, handle later
	case TIMESTAMP:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case TIMESTAMP_MILLI:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	//TODO: Not able to generate correctly in parquet, handle later
	case TIMESTAMP_NANO:
		return &parquet.SchemaElement{
			Type:           ToPointer(parquet.Type_INT64),
			ConvertedType:  ToPointer(parquet.ConvertedType_TIMESTAMP_MILLIS),
			RepetitionType: ToPointer(parquet.FieldRepetitionType_OPTIONAL),
		}
	case OBJECT, ARRAY: // Objects/Arrays are turned into String in parquet
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
