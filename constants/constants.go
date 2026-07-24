package constants

import (
	"fmt"
	"time"
)

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
	// DistributionLower and DistributionUpper define the acceptable range
	// of the distribution factor for validating evenly distributed numeric PKs.
	DistributionLower = 0.05
	DistributionUpper = 1000.0
	// MysqlChunkAcceptanceRatio defines the minimum ratio of expected chunks that must be generated
	// for the split to be considered valid.
	MysqlChunkAcceptanceRatio = float64(0.8)
	// SamplePercentMin / SamplePercentMax define the clamped range for TABLESAMPLE /
	// SAMPLE BLOCK percentage used by physloc and ROWID chunk boundary estimation.
	// 0.01 is the practical floor below which page-level sampling may return zero
	// rows; 50 caps worst-case I/O so a bad row-count estimate cannot escalate to a
	// near-full scan.
	SamplePercentMin = float64(0.01)
	SamplePercentMax = float64(50.0)
	// SampleRowsPerChunkMultiplier controls sample density: each target chunk gets
	// ~10 sample points to pick a boundary from, producing even spacing even when
	// blocks/pages are clustered (e.g. freshly inserted rows land on adjacent pages).
	SampleRowsPerChunkMultiplier = int64(10)
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
