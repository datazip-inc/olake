package constants

// DriverType identifies a source/destination driver. It lives in lib so both olake and the
// decoupled test modules under tests/ can reference the exact same driver identifiers.
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

// CdcTimestamp is the column name olake writes the CDC event timestamp into.
const CdcTimestamp = "_cdc_timestamp"

// SkipCDCDrivers are drivers that do not run a CDC-based sync.
var SkipCDCDrivers = []DriverType{Oracle, DB2, S3}

// UppercaseStreamDrivers are drivers that fold unquoted identifiers to uppercase, so their
// discovered stream names are uppercase in streams.json.
var UppercaseStreamDrivers = []DriverType{Oracle, DB2}
