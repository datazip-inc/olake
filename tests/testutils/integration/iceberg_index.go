package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
		for _, raw := range nsStreams {
			stream, ok := raw.(map[string]interface{})
			if !ok || fmt.Sprint(stream["stream_name"]) != streamName {
				continue
			}
			stream["update_type"] = updateType
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
	if _, err := spark.Sql(ctx, fmt.Sprintf("REFRESH TABLE %s", fullTableName)); err != nil {
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
func (cfg *Test) tableIndexHostDir() string {
	return filepath.Join(cfg.TestConfig.TestWorkingDir, "olake-table-index")
}

// runIcebergSync runs one sync with state against the selected iceberg destination config.
func (cfg *Test) runIcebergSync(ctx context.Context, step string) error {
	cmd := testutils.SyncArgs(true, cfg.destinationFile("iceberg"), "--destination-database-prefix", cfg.UniqueID())
	if code, out, err := testutils.RunOlake(ctx, cfg.TestConfig, cmd...); err != nil || code != 0 {
		return fmt.Errorf("%s failed: %w", step, testutils.RenderOlakeFailure(code, err, out))
	}
	return nil
}

// prepareTableIndexSync resets source data and configures catalog/state for CDC with the given table.
func (cfg *Test) prepareTableIndexSync(ctx context.Context, t *testing.T, testTable string) error {
	t.Helper()
	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed resetting table: %w", err)
	}

	if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, cfg.ColumnToExclude); err != nil {
		return fmt.Errorf("failed updating selected streams: %w", err)
	}

	if err := updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "cdc", ""); err != nil {
		return fmt.Errorf("failed setting stream mode: %w", err)
	}

	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed resetting state file: %w", err)
	}

	_ = os.RemoveAll(cfg.tableIndexHostDir())
	return nil
}

// applyCDCUpdate runs evolve-schema (adds includedColumn) then update, matching the main CDC suite.
func (cfg *Test) applyCDCUpdate(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "update")
}

// testIcebergEqToPosConversion tests conversion of equality delete files to positional delete files
// when the Iceberg table already contains equality deletes from a previous sync.
func (cfg *Test) testIcebergEqToPosConversion(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, cfg.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)

	if err := cfg.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}

	// Step 1: full load + CDC update with equality deletes
	if err := setUpdateType(cfg.TestConfig, cfg.Namespace, testTable, "eq"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := cfg.runIcebergSync(ctx, "initial full load sync"); err != nil {
		return err
	}

	cfg.applyCDCUpdate(ctx, t)

	if err := cfg.runIcebergSync(ctx, "cdc eq sync"); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 2), "expected 1 equality delete file before conversion")
	opTypesBefore := queryLiveOpTypes(ctx, t, spark, fullTableName)
	require.Equal(t, seedRowCount, int64(len(opTypesBefore)), "live row count before conversion should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row before conversion")

	// Step 2: CDC insert with positional deletes (triggers eq -> pos conversion)
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "insert")

	if err := setUpdateType(cfg.TestConfig, cfg.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := cfg.runIcebergSync(ctx, "cdc pos sync"); err != nil {
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
func (cfg *Test) testIcebergCleanTablePositionalWithPebbleIndex(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, cfg.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)

	if err := cfg.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}

	if err := setUpdateType(cfg.TestConfig, cfg.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := cfg.runIcebergSync(ctx, "initial full load"); err != nil {
		return err
	}

	cfg.applyCDCUpdate(ctx, t)

	if err := cfg.runIcebergSync(ctx, "cdc pos sync"); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)

	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected positional delete file to exist")
	require.Equal(t, seedRowCount, countLiveRecords(ctx, t, spark, fullTableName), "live record count should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row")
	require.Equal(t, seedRowCount-1, countByOpType(ctx, t, spark, fullTableName, "r"), "expected remaining backfill rows")

	if _, err := os.Stat(cfg.tableIndexHostDir()); os.IsNotExist(err) {
		return fmt.Errorf("expected pebble index directory to exist at %s", cfg.tableIndexHostDir())
	}

	return nil
}

// testIcebergRebuildIndexFromScratch tests that a missing/corrupted stream index is rebuilt on the next pos sync.
func (cfg *Test) testIcebergRebuildIndexFromScratch(ctx context.Context, t *testing.T, testTable string) error {
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, cfg.TestConfig.DestinationDB, testTable)

	defer testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)

	if err := cfg.prepareTableIndexSync(ctx, t, testTable); err != nil {
		return err
	}

	if err := setUpdateType(cfg.TestConfig, cfg.Namespace, testTable, "pos"); err != nil {
		return fmt.Errorf("failed setting delete type: %w", err)
	}
	if err := cfg.runIcebergSync(ctx, "initial full load"); err != nil {
		return err
	}

	_ = os.RemoveAll(cfg.tableIndexHostDir())

	cfg.applyCDCUpdate(ctx, t)

	if err := cfg.runIcebergSync(ctx, "cdc sync after index delete"); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)

	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected positional delete file after index rebuild")
	require.Equal(t, seedRowCount, countLiveRecords(ctx, t, spark, fullTableName), "live record count should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row after rebuild")

	if _, err := os.Stat(cfg.tableIndexHostDir()); os.IsNotExist(err) {
		return fmt.Errorf("pebble index should be rebuilt and present at %s", cfg.tableIndexHostDir())
	}

	return nil
}
