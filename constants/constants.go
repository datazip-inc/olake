package constants

const (
	ParquetFileExt       = "parquet"
	MongoPrimaryID       = "_id"
	MongoPrimaryIDPrefix = `ObjectID("`
	MongoPrimaryIDSuffix = `")`
	OlakeID              = "_olake_id"
	OlakeTimestamp       = "_olake_timestamp"
	OpType               = "_op_type"
	CdcTimestamp         = "_cdc_timestamp"
	DBName               = "_db"
)

// OlakeInternalFieldsMap provides O(1) lookup for checking if a field is internal
var OlakeInternalFieldsMap = map[string]bool{
	OlakeID:        true,
	OlakeTimestamp: true,
	OpType:         true,
	CdcTimestamp:   true,
	DBName:         true,
}
