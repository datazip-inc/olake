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

	// CdcTimestamp is the column name olake writes the CDC event timestamp into.
	CdcTimestamp = "_cdc_timestamp"

	// MinCDCInitialWaitTime is the floor (in seconds) for a CDC driver's initial_wait_time;
	// values below this are clamped up to it rather than failing setup, matching the UI's minimum.
	MinCDCInitialWaitTime = 120
	// DefaultCDCInitialWaitTime is used when initial_wait_time is not provided.
	DefaultCDCInitialWaitTime = 1200

	// MaxDestinationBatchBytes is the maximum source bytes held in a writer thread buffer before flush.
	MaxDestinationBatchBytes = int64(1) * 1024 * 1024 * 1024 // 1 GB

	// Index store constants
	IndexDBDir                = "OLAKE_INDEX_DB_DIR"
	IndexDBCacheSizePerStream = "OLAKE_INDEX_DB_CACHE_SIZE"
	MaxOpenFilesPerStream     = "OLAKE_INDEX_DB_MAX_OPEN_FILES"
	DefaultDirName            = "olake-table-index"
	DefaultCacheSize          = 128 * 1024 * 1024 // 128 MB default block cache
	DefaultMemTableSize       = 64 * 1024 * 1024  // 64 MB default memtable
	DefaultMaxOpenFiles       = 1000
)

// DriverType identifies a source/destination driver.
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

// DriversRequiringIncrementalFormatter are drivers that require special formatting for incremental value
var DriversRequiringIncrementalFormatter = []DriverType{Oracle, DB2, MSSQL}

var RESTCatalogs = []string{
	"rest",
	"lakekeeper",
	"nessie",
	"s3tables",
	"unity",
	"polaris",
	"biglake",
}
