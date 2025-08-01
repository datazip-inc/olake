package constants

import (
	"time"
)

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
	NormalizationProperty  = "normalization"
	DefaultReadPreference  = "secondaryPreferred"
	EncryptionKey          = "OLAKE_ENCRYPTION_KEY"
	ConfigFolder           = "CONFIG_FOLDER"
	// EffectiveParquetSize is the effective size in bytes considering 512MB targeted parquet size and compression ratio as 8
	EffectiveParquetSize = int64(512) * 1024 * 1024 * int64(8)
)

type DriverType string

const (
	MongoDB  DriverType = "mongodb"
	Postgres DriverType = "postgres"
	MySQL    DriverType = "mysql"
	Oracle   DriverType = "oracle"
)

var (
	// RelationalDrivers contains driver types that are relational databases
	RelationalDrivers = []DriverType{
		Postgres,
		MySQL,
	}
)
