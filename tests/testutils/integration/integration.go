package integration

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

const (
	icebergDestinationFile      = "iceberg_destination.json"
	icebergArrowDestinationFile = "iceberg_destination_arrow.json"
	parquetDestinationFile      = "parquet_destination.json"
)

type Test struct {
	*testutils.TestConfig

	// icebergDestination is the destination config the next iceberg sync runs against, which is how
	// IcebergWriter selects between the legacy and the arrow writer.
	icebergDestination               string
	ExpectedData                     map[string]interface{}
	ExpectedUpdatedData              map[string]interface{}
	DestinationDataTypeSchema        map[string]string
	UpdatedDestinationDataTypeSchema map[string]string
	DefaultCDCColumnsSchema          map[string]string

	// The fields below exist for the backward-compatibility suite (compatibility.go) and are zero for
	// every other suite, which keeps their behavior identical to before they existed.

}

// reset table and add back data to the table
func (cfg *Test) resetTable(ctx context.Context, t *testing.T) error {
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "add")
	if cfg.TestConfig.Driver == string(constants.DB2) {
		// to populate stats for DB2
		cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "populate-stats")
	}
	return nil
}

// runSyncAndVerify executes a sync command and verifies the results in Iceberg
func (cfg *Test) runSyncAndVerify(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useState bool,
	destinationType string,
	operation string,
	opSymbol string,
	schema map[string]interface{},
	isCDC bool,
) error {
	cmd := testutils.SyncArgs(useState, cfg.destinationFile(destinationType), "--destination-database-prefix", cfg.UniqueID())

	// Execute operation before sync if needed
	if useState && operation != "" {
		cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, operation)
		// SQL Server CDC is asynchronous: the capture job only picks up the DML above on its next
		// transaction-log scan, and the sync's change window ends at the job's processed max LSN
		// (sys.fn_cdc_get_max_lsn), so syncing too early would see no changes. Wait for the capture
		// job to advance past the DML. Incremental runs read the table directly and need no wait.
		if isCDC && cfg.TestConfig.Driver == "mssql" {
			cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "wait-cdc-catchup")
		}
	}

	// Run sync against the driver image
	code, out, err := testutils.RunOlake(ctx, cfg.TestConfig, cmd...)
	if err != nil || code != 0 {
		return testutils.RenderOlakeFailure(code, err, out)
	}

	t.Logf("Sync successful for %s driver", cfg.TestConfig.Driver)

	// Use evolved schema only for CDC "update" operation (where schema evolution is expected)
	// Incremental "insert" uses opSymbol "u" but doesn't have schema evolution
	evolvedSchema := operation == "update"

	// Verification reads the destination back through Spark Connect (with retries), a real slice of
	// sync wall-clock; time it as its own phase.
	defer testutils.TrackPhaseTiming(t, cfg.TestConfig.Driver, destinationType+" verify")()

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			} else {
				VerifyIcebergSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			}
		}
	case "parquet":
		{
			if evolvedSchema {
				VerifyParquetSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			} else {
				VerifyParquetSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			}
		}
	}

	return nil
}

// destinationFile names the destination config a sync of this kind runs against. The iceberg one
// is whichever writer variant IcebergWriter selected, defaulting to the committed base config.
func (cfg *Test) destinationFile(destinationType string) string {
	if destinationType == "parquet" {
		return parquetDestinationFile
	}
	if cfg.icebergDestination == "" {
		return icebergDestinationFile
	}
	return cfg.icebergDestination
}

// IcebergDestinationFile names the iceberg destination config the next sync runs against, for
// suites whose expectations depend on which writer that config selects.
func (cfg *Test) IcebergDestinationFile() string {
	return cfg.destinationFile("iceberg")
}

func (cfg *Test) IcebergWriter(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useArrowWriter bool,
	testFunc func(context.Context, *testing.T, string) error,
) error {
	// Writer variants are separate config files, so no suite ever edits one in place; SyncArgs
	// hands whichever is named here to --destination.
	cfg.icebergDestination = icebergDestinationFile
	if useArrowWriter {
		cfg.icebergDestination = icebergArrowDestinationFile
	}

	return testFunc(ctx, t, testTable)
}

type syncTestCase struct {
	name                     string
	operation                string
	useState                 bool
	opSymbol                 string
	expected                 map[string]interface{}
	preSetup                 []func(*testutils.TestConfig) error // host-side actions executed before the sync
	verifyNoDuplicates       bool                                // if true, assert COUNT(*) == COUNT(DISTINCT _olake_id) after sync
	expectedRowCountByOpType int64                               // when > 0, assert COUNT(DISTINCT _olake_id) == this value (catches over-sync and under-sync)
}

// updateStreamConfig sets sync_mode and cursor_field on the stream identified by
// namespace+name in streams[].
func updateStreamConfig(config *testutils.TestConfig, namespace, streamName, syncMode, cursorField string) error {
	// in case of Oracle, the stream names are in uppercase in streams.json
	streamName = testutils.NormalizeStreamName(config.Driver, streamName)
	return testutils.EditJSONFile(config.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
		streams, _ := doc["streams"].([]interface{})
		for _, raw := range streams {
			wrapper, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			if stream["namespace"] == namespace && stream["name"] == streamName {
				stream["sync_mode"] = syncMode
				stream["cursor_field"] = cursorField
			}
		}
		return nil
	})
}
