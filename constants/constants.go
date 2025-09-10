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
	StringifiedData        = "data"
	DefaultReadPreference  = "secondaryPreferred"
	EncryptionKey          = "OLAKE_ENCRYPTION_KEY"
	ConfigFolder           = "CONFIG_FOLDER"
	NoSave                 = "NO_SAVE"
	StatePath              = "STATE_PATH"
	StreamsPath            = "STREAMS_PATH"
	// DestinationDatabasePrefix is used as prefix for destination database name
	DestinationDatabasePrefix = "DESTINATION_DATABASE_PREFIX"
	// EffectiveParquetSize is the effective size in bytes considering 256mb targeted parquet size, compression ratio as 8
	EffectiveParquetSize = int64(256) * 1024 * 1024 * int64(8)
)

type DriverType string

const (
	MongoDB  DriverType = "mongodb"
	Postgres DriverType = "postgres"
	MySQL    DriverType = "mysql"
	Oracle   DriverType = "oracle"
)

var RelationalDrivers = []DriverType{Postgres, MySQL, Oracle}
