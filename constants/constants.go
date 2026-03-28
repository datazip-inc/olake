package constants

import (
	"fmt"
	"os"
	"time"
)

// Build-time version information, injected via ldflags.
// Defaults are used when running locally without build flags.
var (
	version        = "dev"
	commitSHA      = "unknown"
	releaseChannel = "dev"
)

// GetVersion returns the OLake build version. It first checks the build-time
// injected variable, then falls back to the DRIVER_VERSION environment variable
// (set in Docker images), and finally returns "dev" if neither is available.
func GetVersion() string {
	if version != "" && version != "dev" {
		return version
	}
	if v := os.Getenv("DRIVER_VERSION"); v != "" {
		return v
	}
	return "dev"
}

// GetCommitSHA returns the git commit SHA used for the build.
func GetCommitSHA() string {
	return commitSHA
}

// GetReleaseChannel returns the release channel (e.g., stable, beta, dev).
func GetReleaseChannel() string {
	return releaseChannel
}

const (
	DefaultRetryCount      = 3
	DefaultThreadCount     = 3
	DefaultDiscoverTimeout = 5 * time.Minute
	DefaultRetryTimeout    = 60 * time.Second
	GRPCRequestTimeout     = 3600 * time.Second
	DestError              = "destination error"
	ParquetFileExt         = "parquet"
	PartitionRegexIceberg  = `\{([^,]+),\s*([^}]+)\}`
	PartitionRegexParquet  = `\{([^}]+)\}`
	MongoPrimaryID         = "_id"
	OlakeID                = "_olake_id"
	OlakeTimestamp         = "_olake_timestamp"
	OpType                 = "_op_type"
	CdcTimestamp           = "_cdc_timestamp"
	StringifiedData        = "data"
	DefaultReadPreference  = "secondaryPreferred"
	EncryptionKey          = "OLAKE_ENCRYPTION_KEY"
	ConfigFolder           = "CONFIG_FOLDER"
	StatePath              = "STATE_PATH"
	StreamsPath            = "STREAMS_PATH"
	DifferencePath         = "DIFFERENCE_STREAMS_PATH"
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

// Drivers where filters are applied in memory after full refresh data is read.
var FullRefreshPostReadFilterDrivers = []DriverType{S3, Kafka}
var RelationalDrivers = []DriverType{Postgres, MySQL, Oracle, DB2, MSSQL}

var ParallelCDCDrivers = []DriverType{MongoDB, MSSQL}
var ErrNonRetryable = fmt.Errorf("failed with non retryable error")
var ErrGlobalContextGroup = fmt.Errorf("global context group error")
var SkipCDCDrivers = []DriverType{Oracle, DB2}

// DriversRequiringIncrementalFormatter are drivers that require special formatting for incremental value
var DriversRequiringIncrementalFormatter = []DriverType{Oracle, DB2, MSSQL}
