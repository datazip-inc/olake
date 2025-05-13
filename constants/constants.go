package constants

const (
	ParquetFileExt = "parquet"
	PartitionRegex = `\{([^}]+)\}`
	MongoPrimaryID = "_id"
	OlakeID        = "_olake_id"
	OlakeTimestamp = "_olake_timestamp"
	OpType         = "_op_type"
	CdcTimestamp   = "_cdc_timestamp"
	DBName         = "_db"
)
