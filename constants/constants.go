package constants

const (
	ParquetFileExt       = "parquet"
	MongoPrimaryID       = "_id"
	MongoPrimaryIDPrefix = `ObjectID("`
	MongoPrimaryIDSuffix = `")`
	OlakeID              = "_olake_id"
	OlakeTimestamp       = "_olake_insert_time"
	CDCDeletedAt         = "_cdc_deleted_at"
	OpType               = "_op_type"
)
