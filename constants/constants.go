package constants

import "time"

const (
	DefaultRetryCount      = 3
	DefaultThreadCount     = 3
	DefaultDiscoverTimeout = 5 * time.Minute
	DefaultRetryTimeout    = 60 * time.Second
	ParquetFileExt         = "parquet"
	PartitionRegexIceberg  = `\{([^,]+),\s*([^}]+)\}`
	PartitionRegexParquet  = `\{([^}]+)\}`
	MongoPrimaryID         = "_id"
	OlakeID                = "_olake_id"
	OlakeTimestamp         = "_olake_timestamp"
	OpType                 = "_op_type"
	CdcTimestamp           = "_cdc_timestamp"
	DBName                 = "_db"
	DefaultReadPreference  = "secondaryPreferred"
	EncryptionKey          = "OLAKE_ENCRYPTION_KEY"
	ConfigFolder           = "CONFIG_FOLDER"
	// EffectiveParquetSize is the effective size in bytes considering 512MB targeted parquet size, compression ratio as 8 and chunk inflation factor as 2
	EffectiveParquetSize = int64(512) * 1024 * 1024 * int64(8) * int64(2)
)

type DriverType string

const (
	MongoDB  DriverType = "mongodb"
	Postgres DriverType = "postgres"
	MySQL    DriverType = "mysql"
	Oracle   DriverType = "oracle"
)
