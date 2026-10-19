package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// icebergTableIndexTestDrivers lists drivers that run Iceberg table-index / delete-mode tests.
// One CDC-capable driver is enough; these exercise destination behavior, not source specifics.
var icebergTableIndexTestDrivers = []constants.DriverType{constants.Postgres}

// seedRowCount is the number of filter-passing rows inserted by ExecuteQuery("add").
const seedRowCount int64 = 5

// ToastValueLength is the length of the col_toast value ExecuteQuery("add") seeds in Postgres.
// Stored out of line, an UPDATE that leaves it unchanged does not resend it (unchanged TOAST).
const ToastValueLength = 4000

// hasIcebergTableIndexTest reports whether the driver participates in Iceberg row-index tests.
func hasIcebergTableIndexTest(driver string) bool {
	return slices.Contains(icebergTableIndexTestDrivers, constants.DriverType(driver))
}

// setUpdateType sets update_type in selected_streams for the stream identified by namespace+streamName.
func setUpdateType(config *testutils.TestConfig, namespace, streamName, updateType string) error {
	streamName = testutils.NormalizeStreamName(config.Driver, streamName)
	return testutils.EditJSONFile(config.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
		selected, _ := doc["selected_streams"].(map[string]interface{})
		nsStreams, _ := selected[namespace].([]interface{})
		matched := false
		for _, raw := range nsStreams {
			stream, ok := raw.(map[string]interface{})
			if !ok || fmt.Sprint(stream["stream_name"]) != streamName {
				continue
			}
			stream["update_type"] = updateType
			matched = true
		}
		if !matched {
			return fmt.Errorf("stream %q not found in namespace %q", streamName, namespace)
		}
		return nil
	})
}

func getSparkSession(ctx context.Context, t *testing.T) sql.SparkSession {
	t.Helper()
	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	return spark
}

func refreshTable(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) {
	t.Helper()
	if err := testutils.RefreshTable(ctx, spark, fullTableName); err != nil {
		t.Logf("REFRESH TABLE %s failed (non-fatal): %v", fullTableName, err)
	}
}

func countSpark(ctx context.Context, t *testing.T, spark sql.SparkSession, query string) int64 {
	t.Helper()
	df, err := spark.Sql(ctx, query)
	require.NoError(t, err, "spark sql failed: %s", query)
	rows, err := df.Collect(ctx)
	require.NoError(t, err, "spark collect failed: %s", query)
	require.Len(t, rows, 1)
	cnt, ok := rows[0].Value("cnt").(int64)
	require.True(t, ok, "cnt is not int64: %T", rows[0].Value("cnt"))
	return cnt
}

func countDeleteFiles(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, content int) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	return countSpark(ctx, t, spark, fmt.Sprintf(
		"SELECT count(*) as cnt FROM %s.delete_files WHERE content = %d", fullTableName, content,
	))
}

func countByOpType(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName, opType string) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	return countSpark(ctx, t, spark, fmt.Sprintf(
		"SELECT count(*) as cnt FROM %s WHERE _op_type = '%s'", fullTableName, opType,
	))
}

func countLiveRecords(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	return countSpark(ctx, t, spark, fmt.Sprintf("SELECT count(*) as cnt FROM %s", fullTableName))
}

func queryLiveOpTypes(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) map[string]string {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT col_bigserial, _op_type FROM %s", fullTableName))
	require.NoError(t, err)
	rows, err := df.Collect(ctx)
	require.NoError(t, err)

	result := make(map[string]string, len(rows))
	for _, r := range rows {
		result[fmt.Sprintf("%v", r.Value("col_bigserial"))] = fmt.Sprintf("%v", r.Value("_op_type"))
	}
	return result
}

// tableIndexHostDir is the host-side pebble index directory, the mount behind containerTableIndexDir.
func (th *TestHandler) tableIndexHostDir() string {
	return filepath.Join(th.TestConfig.TestWorkingDir, "olake-table-index")
}

// runIcebergSync runs one sync against the selected iceberg destination config.
func (th *TestHandler) runIcebergSync(ctx context.Context, t *testing.T, step string, useState bool) error {
	if err := testutils.RunSync(ctx, t, th.TestConfig, th.destinationFile("iceberg"), useState); err != nil {
		return fmt.Errorf("%s failed: %w", step, err)
	}
	return nil
}

// prepareTableIndexSync resets source data and configures the catalog for CDC with the given table.
func (th *TestHandler) prepareTableIndexSync(ctx context.Context, t *testing.T, testTable string) error {
	t.Helper()
	th.resetTable(ctx, t)

	if err := updateStreamConfig(th.TestConfig, th.Namespace, testTable, "cdc", ""); err != nil {
		return fmt.Errorf("failed setting stream mode: %w", err)
	}
	return nil
}

// applyCDCUpdate runs evolve-schema (adds includedColumn) then update, matching the main CDC suite.
func (th *TestHandler) applyCDCUpdate(ctx context.Context, t *testing.T) {
	t.Helper()
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "evolve-schema")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "update")
}

// testIcebergEqToPosConversion tests conversion of equality delete files to positional delete files
// when the Iceberg table already contains equality deletes from a previous sync.
func (th *TestHandler) testIcebergEqToPosConversion(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}

	// Step 1: full load + CDC update with equality deletes
	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "eq"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := th.runIcebergSync(ctx, t, "initial full load sync", false); err != nil {
		return err
	}

	th.applyCDCUpdate(ctx, t)

	if err := th.runIcebergSync(ctx, t, "cdc eq sync", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 2), "expected 1 equality delete file before conversion")
	opTypesBefore := queryLiveOpTypes(ctx, t, spark, fullTableName)
	require.Equal(t, seedRowCount, int64(len(opTypesBefore)), "live row count before conversion should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row before conversion")

	// Step 2: CDC insert with positional deletes (triggers eq -> pos conversion)
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "insert")

	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := th.runIcebergSync(ctx, t, "cdc pos sync", true); err != nil {
		return err
	}

	require.Equal(t, int64(0), countDeleteFiles(ctx, t, spark, fullTableName, 2), "expected 0 equality delete files after conversion")
	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected 1 positional delete file after conversion")

	opTypesAfter := queryLiveOpTypes(ctx, t, spark, fullTableName)
	for idStr, opBefore := range opTypesBefore {
		require.Equal(t, opBefore, opTypesAfter[idStr], "optype mismatch for id %s after conversion", idStr)
	}

	return nil
}

// testIcebergCleanTablePositionalWithPebbleIndex tests full load then positional CDC updates on a clean table.
func (th *TestHandler) testIcebergCleanTablePositionalWithPebbleIndex(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}

	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := th.runIcebergSync(ctx, t, "initial full load", false); err != nil {
		return err
	}

	th.applyCDCUpdate(ctx, t)

	if err := th.runIcebergSync(ctx, t, "cdc pos sync", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)

	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected positional delete file to exist")
	require.Equal(t, seedRowCount, countLiveRecords(ctx, t, spark, fullTableName), "live record count should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row")
	require.Equal(t, seedRowCount-1, countByOpType(ctx, t, spark, fullTableName, "r"), "expected remaining backfill rows")

	if _, err := os.Stat(th.tableIndexHostDir()); os.IsNotExist(err) {
		return fmt.Errorf("expected pebble index directory to exist at %s", th.tableIndexHostDir())
	}

	return nil
}

// testIcebergRebuildIndexFromScratch tests that a missing/corrupted stream index is rebuilt on the next pos sync.
func (th *TestHandler) testIcebergRebuildIndexFromScratch(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := th.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}

	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := th.runIcebergSync(ctx, t, "initial full load", false); err != nil {
		return err
	}

	if err := os.RemoveAll(th.tableIndexHostDir()); err != nil {
		return fmt.Errorf("failed to delete the table index: %w", err)
	}

	th.applyCDCUpdate(ctx, t)

	if err := th.runIcebergSync(ctx, t, "cdc sync after index delete", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)

	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected positional delete file after index rebuild")
	require.Equal(t, seedRowCount, countLiveRecords(ctx, t, spark, fullTableName), "live record count should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row after rebuild")

	if _, err := os.Stat(th.tableIndexHostDir()); os.IsNotExist(err) {
		return fmt.Errorf("pebble index should be rebuilt and present at %s", th.tableIndexHostDir())
	}

	return nil
}

// TestUnchangedToastRecovery checks, with preserve_toast_values on, that pos and dv refill a col_toast the CDC
// update left out. TestDiscover checks what discover offers with the option on.
func (th *TestHandler) TestUnchangedToastRecovery(t *testing.T) {
	ctx := t.Context()
	testTable := th.GetTableName()
	require.NoError(t, enablePreserveToastValues(th.TestConfig), "failed enabling preserve_toast_values")

	toastValue := strings.Repeat("t", ToastValueLength)
	testCases := []struct {
		updateType   string
		deleteFormat string // what the update's delete is written as, proving the sync ran under updateType
	}{
		{updateType: "pos", deleteFormat: "PARQUET"},
		{updateType: "dv", deleteFormat: "PUFFIN"},
	}

	for _, tc := range testCases {
		t.Run(tc.updateType, func(t *testing.T) {
			require.NoError(t, th.prepareTableIndexSync(ctx, t, testTable))
			require.NoError(t, setUpdateType(th.TestConfig, th.Namespace, testTable, tc.updateType))
			defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)
			// DestinationDB is set once the catalog is rendered for this suite
			fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)

			require.NoError(t, th.runSyncAndVerify(ctx, t, testTable, false, "iceberg", "", "r",
				map[string]interface{}{"col_toast": toastValue}, false))

			// the update does not touch col_toast, so Postgres leaves it out of the change
			th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "evolve-schema")
			require.NoError(t, th.runSyncAndVerify(ctx, t, testTable, true, "iceberg", "update", "u",
				map[string]interface{}{"col_toast": toastValue}, true))

			require.Greaterf(t, countDeleteFilesByFormat(ctx, t, getSparkSession(ctx, t), fullTableName, 1, tc.deleteFormat), int64(0),
				"expected a %s positional delete from the %s update", tc.deleteFormat, tc.updateType)
		})
	}

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data for UnchangedToastRecovery as (%s) is set", th.Driver, testutils.KeepTestDataEnvVar)
		return
	}
	th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
}

// enablePreserveToastValues turns preserve_toast_values on in the suite's own copy of source.json.
func enablePreserveToastValues(config *testutils.TestConfig) error {
	return testutils.EditJSONFile(config.GetFilePath("source.json"), func(doc map[string]interface{}) error {
		updateMethod, ok := doc["update_method"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("no update_method object in source config")
		}
		updateMethod["preserve_toast_values"] = true
		return nil
	})
}

// TestIcebergPosIncremental syncs incrementally into a pos-mode stream, which keeps a table index. Rows must keep
// the op type the source emitted: only ResolveUpsertOp streams have create vs update resolved from the index.
func (th *TestHandler) TestIcebergPosIncremental(t *testing.T) {
	ctx := t.Context()
	testTable := th.GetTableName()

	writerTypes := []struct {
		name     string
		useArrow bool
	}{
		{"Legacy", false},
		{"Arrow", true},
	}
	for _, wt := range writerTypes {
		t.Run(wt.name, func(t *testing.T) {
			require.NoError(t, th.IcebergWriter(ctx, t, testTable, wt.useArrow, th.testIcebergPosIncrementalInsert))
		})
	}

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data for PosIncremental as (%s) is set", th.Driver, testutils.KeepTestDataEnvVar)
		return
	}
	th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
}

// testIcebergPosIncrementalInsert full-loads the seed rows in pos mode, then syncs one inserted row incrementally.
// Incremental reads emit "u" for every row, so the inserted row must land as "u", not be relabelled "c".
func (th *TestHandler) testIcebergPosIncrementalInsert(ctx context.Context, t *testing.T, testTable string) error {
	th.resetTable(ctx, t)
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)

	if err := updateStreamConfig(th.TestConfig, th.Namespace, testTable, "incremental", th.CursorField); err != nil {
		return fmt.Errorf("failed setting stream mode: %w", err)
	}
	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}

	if err := th.runIcebergSync(ctx, t, "full load", false); err != nil {
		return err
	}

	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "insert")
	if err := th.runIcebergSync(ctx, t, "incremental sync", true); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)

	require.Equal(t, seedRowCount+1, countLiveRecords(ctx, t, spark, fullTableName), "live rows after the incremental insert")
	require.Equal(t, seedRowCount, countByOpType(ctx, t, spark, fullTableName, "r"), "full load rows")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "incremental insert must keep the op type the source emitted")

	return nil
}
