package constants

import (
	"time"
)

const (
	DefaultRetryCount      = 3
	DefaultThreadCount     = 3
	DefaultDiscoverTimeout = 5 * time.Minute
	DefaultRetryTimeout    = 60 * time.Second
	DestError              = "destination error"
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
	StatePath              = "STATE_PATH"
	StreamsPath            = "STREAMS_PATH"
	DifferencePath         = "DIFFERENCE_STREAMS_PATH"
	LSNNotUpdatedError     = "LSN not updated after 5 minutes"
	NoRecordsFoundError    = "no records found in given initial wait time"
	// DestinationDatabasePrefix is used as prefix for destination database name
	DestinationDatabasePrefix = "DESTINATION_DATABASE_PREFIX"
	// EffectiveParquetSize is the effective size in bytes considering 256mb targeted parquet size, compression ratio as 8
	EffectiveParquetSize = int64(256) * 1024 * 1024 * int64(8)
	LatestStateVersion   = 1 // version of state file to be used for backward compatibility
)

type DriverType string

const (
	MongoDB  DriverType = "mongodb"
	Postgres DriverType = "postgres"
	MySQL    DriverType = "mysql"
	Oracle   DriverType = "oracle"
	Kafka    DriverType = "kafka"
)

var RelationalDrivers = []DriverType{Postgres, MySQL, Oracle}

var NonRetryableErrors = []string{DestError, "context canceled", NoRecordsFoundError, LSNNotUpdatedError, "lsn mismatch"}

// LoadedStateVersion stores the version of the state file being used
var LoadedStateVersion = 1
