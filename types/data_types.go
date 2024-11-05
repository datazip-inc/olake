package types

type DataType string

const (
	NULL      DataType = "null"
	INT64     DataType = "integer"
	FLOAT64   DataType = "number"
	STRING    DataType = "string"
	BOOL      DataType = "boolean"
	OBJECT    DataType = "object"
	ARRAY     DataType = "array"
	UNKNOWN   DataType = "unknown"
	TIMESTAMP DataType = "timestamp"
)

type Record = map[string]any
