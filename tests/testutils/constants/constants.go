// Package constants holds the pieces of olake's external contract that the integration tests need
// to name: the driver identifiers it accepts on the command line, and the column it writes CDC
// event times into.
//
// These are restated here rather than imported from olake on purpose. A black-box suite that shared
// the product's own constant could not detect a contract break -- rename the CDC column and every
// test using the shared symbol recompiles clean and passes, sailing past the exact regression the
// suite exists to catch. The duplication is the mechanism, not the cost.
//
// It is a package of its own, rather than more surface on testutils, so the contract stays legible:
// what is here is what olake promises, and everything else in tests/ is harness.
package constants

// DriverType identifies a source driver.
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

// LatestStateVersion is the state file format olake writes today. A state file without it reads as
// version 0, which puts the sync on legacy type mapping -- so the harness has to name the version.
const LatestStateVersion = 6

// SkipCDCDrivers are drivers that do not run a CDC-based sync. Unlike the identifiers above this is
// test-side policy, not an olake contract -- it decides which suites skip their CDC subtests.
var SkipCDCDrivers = []DriverType{Oracle, DB2, S3}

// UppercaseStreamDrivers are drivers whose sources name objects in uppercase, so discover writes
// uppercase stream names and a test patching streams.json by name must match that casing.
var UppercaseStreamDrivers = []DriverType{Oracle, DB2}
