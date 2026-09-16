package integration

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

const (
	icebergDestinationFile      = "iceberg_destination.json"
	icebergArrowDestinationFile = "iceberg_destination_arrow.json"
	parquetDestinationFile      = "parquet_destination.json"
)

type TestHandler struct {
	*testutils.TestConfig

	// icebergDestination is the destination config the next iceberg sync runs against, which is how
	// IcebergWriter selects between the legacy and the arrow writer.
	icebergDestination               string
	ExpectedData                     map[string]interface{}
	ExpectedUpdatedData              map[string]interface{}
	DestinationDataTypeSchema        map[string]string
	UpdatedDestinationDataTypeSchema map[string]string
	DefaultCDCColumnsSchema          map[string]string
	TypeMapping                      map[string]string
}

// reset table and add back data to the table
func (th *TestHandler) resetTable(ctx context.Context, t *testing.T) {
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "drop")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "create")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "add")
}

// runSyncAndVerify executes a sync command and verifies the results in Iceberg
func (th *TestHandler) runSyncAndVerify(
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
	// Execute operation before sync if needed
	if useState && operation != "" {
		th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, operation)
	}

	// Run sync against the driver image
	if err := testutils.RunSync(ctx, t, th.TestConfig, th.destinationFile(destinationType), useState); err != nil {
		return err
	}

	t.Logf("Sync successful for %s driver", th.TestConfig.Driver)

	// Use evolved schema only for CDC "update" operation (where schema evolution is expected)
	// Incremental "insert" uses opSymbol "u" but doesn't have schema evolution
	evolvedSchema := operation == "update"

	// Verification reads the destination back through Spark Connect (with retries), a real slice of
	// sync wall-clock; time it as its own phase.
	defer testutils.TrackPhaseTiming(t, th.TestConfig.Driver, destinationType+" verify")()

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, testTable, th.TestConfig.DestinationDB, th.UpdatedDestinationDataTypeSchema, th.TypeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.PartitionRegex, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude)
			} else {
				VerifyIcebergSync(t, testTable, th.TestConfig.DestinationDB, th.DestinationDataTypeSchema, th.TypeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.PartitionRegex, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude)
			}
		}
	case "parquet":
		{
			if evolvedSchema {
				VerifyParquetSync(t, testTable, th.TestConfig.DestinationDB, th.UpdatedDestinationDataTypeSchema, th.TypeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude)
			} else {
				VerifyParquetSync(t, testTable, th.TestConfig.DestinationDB, th.DestinationDataTypeSchema, th.TypeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude)
			}
		}
	}

	return nil
}

// destinationFile names the destination config a sync of this kind runs against. The iceberg one
// is whichever writer variant IcebergWriter selected, defaulting to the committed base config.
func (th *TestHandler) destinationFile(destinationType string) string {
	if destinationType == "parquet" {
		return parquetDestinationFile
	}
	if th.icebergDestination == "" {
		return icebergDestinationFile
	}
	return th.icebergDestination
}

// IcebergDestinationFile names the iceberg destination config the next sync runs against, for
// suites whose expectations depend on which writer that config selects.
func (th *TestHandler) IcebergDestinationFile() string {
	return th.destinationFile("iceberg")
}

func (th *TestHandler) IcebergWriter(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useArrowWriter bool,
	testFunc func(context.Context, *testing.T, string) error,
) error {
	// Writer variants are separate config files, so no suite ever edits one in place; SyncArgs
	// hands whichever is named here to --destination.
	th.icebergDestination = testutils.Ternary(useArrowWriter, icebergArrowDestinationFile, icebergDestinationFile).(string)

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
