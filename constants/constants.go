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
	EffectiveParquetSize        = int64(256) * 1024 * 1024 * int64(8)
	DB2StateTimestampFormat     = "2006-01-02 15:04:05.000000"
	DefaultStateTimestampFormat = "2006-01-02T15:04:05.000000000Z"
)

type DriverType string

const (
	MongoDB  DriverType = "mongodb"
	Postgres DriverType = "postgres"
	MySQL    DriverType = "mysql"
	Oracle   DriverType = "oracle"
	DB2      DriverType = "db2"
	S3       DriverType = "s3"
	Kafka    DriverType = "kafka"
	MSSQL    DriverType = "mssql"
)

var RelationalDrivers = []DriverType{Postgres, MySQL, Oracle, DB2, MSSQL}

var ParallelCDCDrivers = []DriverType{MongoDB, MSSQL}

var NonRetryableErrors = []string{DestError, "context canceled", NoRecordsFoundError, LSNNotUpdatedError, "lsn mismatch"}

var SkipCDCDrivers = []DriverType{Oracle, DB2}

// DriversRequiringIncrementalFormatter are drivers that require special formatting for incremental value
var DriversRequiringIncrementalFormatter = []DriverType{Oracle, DB2, MSSQL}
