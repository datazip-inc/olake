package types

import (
	"encoding/json"
	"fmt"

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

func (r Record) GetStringifiedJSONValue(key string) (string, error) {
	value := r[key]
	switch value.(type) {
	case struct{}, map[string]interface{}, []interface{}:
		s, err := json.Marshal(value)
		return string(s), err
	default:
		return fmt.Sprintf("%v", r[key]), nil
	}
}

// returns parquet equivalent type & convertedType for the datatype
func (d DataType) getParquetEquivalent() (parquet.Type, parquet.ConvertedType) {
	switch d {
	case INT64:
		return parquet.Type_INT64, parquet.ConvertedType_INT_64
	case FLOAT64:
		return parquet.Type_DOUBLE, -1
	case STRING:
		return parquet.Type_BYTE_ARRAY, parquet.ConvertedType_UTF8
	case BOOL:
		return parquet.Type_BOOLEAN, -1
	//TODO: Not able to generate correctly in parquet, handle later
	case TIMESTAMP:
		return parquet.Type_INT64, parquet.ConvertedType_TIMESTAMP_MILLIS
	case TIMESTAMP_MILLI:
		return parquet.Type_INT64, parquet.ConvertedType_TIMESTAMP_MILLIS
	case TIMESTAMP_MICRO:
		return parquet.Type_INT64, parquet.ConvertedType_TIMESTAMP_MICROS
	//TODO: Not able to generate correctly in parquet, handle later
	case TIMESTAMP_NANO:
		return parquet.Type_INT64, parquet.ConvertedType_TIMESTAMP_MICROS
	case OBJECT:
		return parquet.Type_BYTE_ARRAY, parquet.ConvertedType_JSON
	default:
		return parquet.Type_BYTE_ARRAY, parquet.ConvertedType_JSON
	}
}

func (d DataType) stringificationNeededForJsonTypes() bool {
	switch d {
	case INT64:
		return false
	case FLOAT64:
		return false
	case BOOL:
		return false
	case TIMESTAMP:
		return false
	case TIMESTAMP_MILLI:
		return false
	case TIMESTAMP_MICRO:
		return false
	case TIMESTAMP_NANO:
		return false
	default:
		return true
	}
}
