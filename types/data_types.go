package types

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

type Record = map[string]any
