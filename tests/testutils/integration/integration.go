package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const (
	icebergDestinationFile      = "iceberg_destination.json"
	icebergArrowDestinationFile = "iceberg_destination_arrow.json"
	parquetDestinationFile      = "parquet_destination.json"
)

// RowCountContract defines the expected number of rows seeded and verified for each phase of a sync test.
type RowCountContract struct {
	FullLoad int
	Insert   int
	Update   int
}

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
	ParquetTypeMapping               map[string]string
	DedupKeys                        []string
	UpsertAddOp                      string
	UpsertUpdateOp                   string
	SkipTombstone                    bool
	RowCounts                        RowCountContract
}

// ExpectedRowCount resolves the expected row count for a given operation and op_type symbol.
// Returns 0 for delete operations, and falls back to standard defaults (FullLoad: 5, Insert: 1, Update: 1)
// if the handler's RowCounts contract fields are unset. Returns an error if the operation is unhandled.
func (th *TestHandler) ExpectedRowCount(opSymbol, operation string) (int, error) {
	if opSymbol == "d" {
		return 0, nil
	}
	fullLoad := testutils.Ternary(th.RowCounts.FullLoad > 0, th.RowCounts.FullLoad, 5).(int)
	insertCount := testutils.Ternary(th.RowCounts.Insert > 0, th.RowCounts.Insert, 1).(int)
	updateCount := testutils.Ternary(th.RowCounts.Update > 0, th.RowCounts.Update, 1).(int)

	switch operation {
	case "insert", "upsert_add", "add":
		return insertCount, nil
	case "update", "upsert_update", "upsert_column_update", "upsert_update_repartition", "upsert_update_partition_1":
		return updateCount, nil
	case "":
		return fullLoad, nil
	default:
		driver := ""
		if th.TestConfig != nil {
			driver = th.TestConfig.Driver
		}
		return 0, fmt.Errorf("ExpectedRowCount: unhandled operation %q for symbol %q (driver %s); declare expectedRowCount explicitly in test case or register in ExpectedRowCount", operation, opSymbol, driver)
	}
}

// reset table and add back data to the table, then reset the catalog so the scenario starts fresh
func (th *TestHandler) resetTable(ctx context.Context, t *testing.T) {
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "drop")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "create")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, testutils.Ternary(len(th.DedupKeys) > 0, testutils.Ternary(th.UpsertAddOp != "", th.UpsertAddOp, "upsert_add"), "add").(string))
	th.resetCatalog(t)
}

// resetCatalog renders the catalog from its templates with the suite's settings and empties the table
// index, so a scenario never inherits another scenario's catalog edits or index entries.
func (th *TestHandler) resetCatalog(t *testing.T) {
	t.Helper()
	testTable := th.GetTableName()
	_, err := th.RenderCatalog("")
	require.NoError(t, err, "failed to render the catalog")
	require.NoError(t, th.IsolateDestinationDB(), "failed to isolate the destination database in the catalog")
	if th.PrimaryKey != "" {
		require.NoError(t, updateStreamPrimaryKey(th.TestConfig, th.Namespace, testTable, th.PrimaryKey), "failed to key the stream on %s in the catalog", th.PrimaryKey)
	}
	if len(th.DedupKeys) > 0 {
		require.NoError(t, setStreamUpsert(th.TestConfig, th.Namespace, testTable, th.DedupKeys), "failed to set stream upsert")
	}
	require.NoError(t, testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, th.PartitionRegex, th.FilterConfig, []string{testTable}, []string{th.ColumnToExclude}), "failed to enable normalization and partition regex in the catalog")
	require.NoError(t, os.RemoveAll(th.tableIndexHostDir()), "failed to clear table index")
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
	expectedRowCount ...int,
) error {
	// Resolve expected row count upfront so invalid operations fail fast before wasting a sync
	var count int
	if len(expectedRowCount) > 0 && expectedRowCount[0] != 0 {
		count = expectedRowCount[0]
	} else {
		var err error
		count, err = th.ExpectedRowCount(opSymbol, operation)
		if err != nil {
			t.Fatalf("failed to resolve expected row count: %v", err)
		}
	}

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
	evolvedSchema := operation == "update" || operation == "upsert_update" || operation == "upsert_column_update" || operation == "upsert_update_repartition" || operation == "upsert_update_partition_1"

	// Verification reads the destination back through Spark Connect (with retries), a real slice of
	// sync wall-clock; time it as its own phase.
	defer testutils.TrackPhaseTiming(t, th.TestConfig.Driver, destinationType+" verify")()

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, testTable, th.TestConfig.DestinationDB, th.UpdatedDestinationDataTypeSchema, th.TypeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.PartitionRegex, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude, count)
			} else {
				VerifyIcebergSync(t, testTable, th.TestConfig.DestinationDB, th.DestinationDataTypeSchema, th.TypeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.PartitionRegex, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude, count)
			}
		}
	case "parquet":
		{
			typeMapping := th.TypeMapping
			if th.ParquetTypeMapping != nil {
				typeMapping = th.ParquetTypeMapping
			}
			if evolvedSchema {
				VerifyParquetSync(t, testTable, th.TestConfig.DestinationDB, th.UpdatedDestinationDataTypeSchema, typeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude, count)
			} else {
				VerifyParquetSync(t, testTable, th.TestConfig.DestinationDB, th.DestinationDataTypeSchema, typeMapping, th.DefaultCDCColumnsSchema, schema, opSymbol, th.TestConfig.Driver, isCDC, th.TestConfig.ColumnToExclude, count)
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
	previous := th.icebergDestination
	th.icebergDestination = testutils.Ternary(useArrowWriter, icebergArrowDestinationFile, icebergDestinationFile).(string)
	defer func() { th.icebergDestination = previous }()

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
	expectedRowCount         int                                 // optional override for Verify*Sync: 0 means use contract (ExpectedRowCount); < 0 (e.g. SkipRowCount) skips exact count and asserts non-empty; > 0 asserts exact count
}

// updateStreamConfig sets sync_mode and cursor_field for the stream identified by namespace+name:
// on its selected_streams entry, which takes priority over streams[], or on streams[] under
// streams v1, whose selected_streams has no such fields.
func updateStreamConfig(config *testutils.TestConfig, namespace, streamName, syncMode, cursorField string) error {
	edit := func(entry map[string]interface{}) {
		entry["sync_mode"] = syncMode
		entry["cursor_field"] = cursorField
	}
	if config.StreamsV1 {
		return editStream(config, namespace, streamName, edit)
	}
	return editSelectedStream(config, namespace, streamName, edit)
}

// updateStreamPrimaryKey sets source_defined_primary_key on the stream identified by
// namespace+name in streams[], for a suite whose table is keyed on a column other than the one
// the committed catalog template declares.
func updateStreamPrimaryKey(config *testutils.TestConfig, namespace, streamName, primaryKey string) error {
	return editStream(config, namespace, streamName, func(stream map[string]interface{}) {
		stream["source_defined_primary_key"] = []interface{}{primaryKey}
	})
}

// editStream applies edit to the stream identified by namespace+name in streams[].
func editStream(config *testutils.TestConfig, namespace, streamName string, edit func(stream map[string]interface{})) error {
	// in case of Oracle, the stream names are in uppercase in the catalog
	streamName = testutils.NormalizeStreamName(config.Driver, streamName)
	catalog := config.Catalog()
	return catalog.Edit(func(doc map[string]interface{}) error {
		stream := testutils.AvailableStreamEntry(doc, namespace, streamName)
		if stream == nil {
			return fmt.Errorf("stream %q not found in namespace %q of %s", streamName, namespace, catalog)
		}
		edit(stream)
		return nil
	})
}

// editSelectedStream applies edit to the selected_streams entry identified by namespace+name.
func editSelectedStream(config *testutils.TestConfig, namespace, streamName string, edit func(entry map[string]interface{})) error {
	streamName = testutils.NormalizeStreamName(config.Driver, streamName)
	catalog := config.Catalog()
	return catalog.Edit(func(doc map[string]interface{}) error {
		entry := testutils.SelectedStreamEntry(doc, namespace, streamName)
		if entry == nil {
			return fmt.Errorf("stream %q not selected in namespace %q of %s", streamName, namespace, catalog)
		}
		edit(entry)
		return nil
	})
}
