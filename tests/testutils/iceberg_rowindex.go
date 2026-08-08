package testutils

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// icebergRowIndexTestDrivers lists drivers that run Iceberg row-index / delete-mode tests.
// One CDC-capable driver is enough; these exercise destination behavior, not source specifics.
var icebergRowIndexTestDrivers = []constants.DriverType{constants.Postgres}

// seedRowCount is the number of filter-passing rows inserted by ExecuteQuery("add").
const seedRowCount int64 = 5

// hasIcebergRowIndexTest reports whether the driver participates in Iceberg row-index tests.
func hasIcebergRowIndexTest(driver string) bool {
	return slices.Contains(icebergRowIndexTestDrivers, constants.DriverType(driver))
}

func getSparkSession(ctx context.Context, t *testing.T) sql.SparkSession {
	t.Helper()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
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

// prepareRowIndexSync resets source data and configures catalog/state for CDC with the given table.
func (cfg *IntegrationTest) prepareRowIndexSync(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	t.Helper()
	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed resetting table: %w", err)
	}

	streamUpdateCmd := UpdateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, true, cfg.ColumnToExclude)
	if code, out, err := ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
		return fmt.Errorf("failed updating selected streams (%d): %s\n%s", code, err, out)
	}

	modeCmd := UpdateStreamConfigCommand(*cfg.TestConfig, cfg.Namespace, testTable, "cdc", "")
	if code, out, err := ExecCommand(ctx, c, modeCmd); err != nil || code != 0 {
		return fmt.Errorf("failed setting stream mode (%d): %s\n%s", code, err, out)
	}

	resetCmd := ResetStateFileCommand(*cfg.TestConfig)
	if code, out, err := ExecCommand(ctx, c, resetCmd); err != nil || code != 0 {
		return fmt.Errorf("failed resetting state file (%d): %s\n%s", code, err, out)
	}
	return nil
}

// applyCDCUpdate runs evolve-schema (adds includedColumn) then update, matching the main CDC suite.
func (cfg *IntegrationTest) applyCDCUpdate(ctx context.Context, t *testing.T, testTable string) {
	t.Helper()
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "evolve-schema", false)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "update", false)
}

// testIcebergEqToPosConversion tests conversion of equality delete files to positional delete files
// when the Iceberg table already contains equality deletes from a previous sync.
func (cfg *IntegrationTest) testIcebergEqToPosConversion(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	destDBPrefix := Ternary(cfg.TestConfig.DataFormat != "", fmt.Sprintf("integration_%s_%s", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat), fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)).(string)
	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, cfg.DestinationDB, testTable)

	defer DropIcebergTable(t, testTable, cfg.DestinationDB)

	if err := cfg.prepareRowIndexSync(ctx, t, c, testTable); err != nil {
		return err
	}

	// Step 1: full load + CDC update with equality deletes
	syncEqCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "eq")
	if code, out, err := ExecCommand(ctx, c, syncEqCmd); err != nil || code != 0 {
		return fmt.Errorf("initial full load sync failed (%d): %s\n%s", code, err, out)
	}

	cfg.applyCDCUpdate(ctx, t, testTable)

	cdcEqCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "eq")
	if code, out, err := ExecCommand(ctx, c, cdcEqCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc eq sync failed (%d): %s\n%s", code, err, out)
	}

	spark := getSparkSession(ctx, t)
	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 2), "expected 1 equality delete file before conversion")
	opTypesBefore := queryLiveOpTypes(ctx, t, spark, fullTableName)
	require.Equal(t, seedRowCount, int64(len(opTypesBefore)), "live row count before conversion should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row before conversion")
	defer func() {
		if err := spark.Stop(); err != nil {
			t.Logf("Failed to stop Spark session: %v", err)
		}
	}()

	// Step 2: CDC insert with positional deletes (triggers eq -> pos conversion)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "insert", false)

	cdcPosCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, cdcPosCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc pos sync failed (%d): %s\n%s", code, err, out)
	}

	spark = getSparkSession(ctx, t)
	defer func() {
		if err := spark.Stop(); err != nil {
			t.Logf("Failed to stop Spark session: %v", err)
		}
	}()

	require.Equal(t, int64(0), countDeleteFiles(ctx, t, spark, fullTableName, 2), "expected 0 equality delete files after conversion")
	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected 1 positional delete file after conversion")

	opTypesAfter := queryLiveOpTypes(ctx, t, spark, fullTableName)
	for idStr, opBefore := range opTypesBefore {
		require.Equal(t, opBefore, opTypesAfter[idStr], "optype mismatch for id %s after conversion", idStr)
	}

	return nil
}

// testIcebergCleanTablePositionalWithPebbleIndex tests full load then positional CDC updates on a clean table.
func (cfg *IntegrationTest) testIcebergCleanTablePositionalWithPebbleIndex(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	destDBPrefix := Ternary(cfg.TestConfig.DataFormat != "", fmt.Sprintf("integration_%s_%s", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat), fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)).(string)
	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, cfg.DestinationDB, testTable)

	defer DropIcebergTable(t, testTable, cfg.DestinationDB)

	if err := cfg.prepareRowIndexSync(ctx, t, c, testTable); err != nil {
		return err
	}

	syncFullCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, syncFullCmd); err != nil || code != 0 {
		return fmt.Errorf("initial full load failed (%d): %s\n%s", code, err, out)
	}

	cfg.applyCDCUpdate(ctx, t, testTable)

	cdcPosCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, cdcPosCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc pos sync failed (%d): %s\n%s", code, err, out)
	}

	spark := getSparkSession(ctx, t)
	defer func() {
		if err := spark.Stop(); err != nil {
			t.Logf("Failed to stop Spark session: %v", err)
		}
	}()

	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected positional delete file to exist")
	require.Equal(t, seedRowCount, countLiveRecords(ctx, t, spark, fullTableName), "live record count should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row")
	require.Equal(t, seedRowCount-1, countByOpType(ctx, t, spark, fullTableName, "r"), "expected remaining backfill rows")

	checkIdxCmd := "find /test-olake -name olake-row-index -type d | grep ."
	if code, out, err := ExecCommand(ctx, c, checkIdxCmd); err != nil || code != 0 {
		return fmt.Errorf("expected pebble index directory to exist (%d): %s\n%s", code, err, out)
	}

	return nil
}

// testIcebergRebuildIndexFromScratch tests that a missing/corrupted row index is rebuilt on the next pos sync.
func (cfg *IntegrationTest) testIcebergRebuildIndexFromScratch(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	destDBPrefix := Ternary(cfg.TestConfig.DataFormat != "", fmt.Sprintf("integration_%s_%s", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat), fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)).(string)
	fullTableName := fmt.Sprintf("%s.%s.%s", icebergCatalog, cfg.DestinationDB, testTable)

	defer DropIcebergTable(t, testTable, cfg.DestinationDB)

	if err := cfg.prepareRowIndexSync(ctx, t, c, testTable); err != nil {
		return err
	}

	syncFullCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, syncFullCmd); err != nil || code != 0 {
		return fmt.Errorf("initial full load failed (%d): %s\n%s", code, err, out)
	}

	if code, out, err := ExecCommand(ctx, c, "rm -rf /test-olake/olake-row-index /test-olake/drivers/*/olake-row-index"); err != nil || code != 0 {
		return fmt.Errorf("failed deleting row index (%d): %s\n%s", code, err, out)
	}

	cfg.applyCDCUpdate(ctx, t, testTable)

	cdcPosCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, cdcPosCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc sync after index delete failed (%d): %s\n%s", code, err, out)
	}

	spark := getSparkSession(ctx, t)
	defer func() {
		if err := spark.Stop(); err != nil {
			t.Logf("Failed to stop Spark session: %v", err)
		}
	}()

	require.Equal(t, int64(1), countDeleteFiles(ctx, t, spark, fullTableName, 1), "expected positional delete file after index rebuild")
	require.Equal(t, seedRowCount, countLiveRecords(ctx, t, spark, fullTableName), "live record count should match seed")
	require.Equal(t, int64(1), countByOpType(ctx, t, spark, fullTableName, "u"), "expected 1 updated row after rebuild")

	if code, out, err := ExecCommand(ctx, c, "find /test-olake -name olake-row-index -type d | grep ."); err != nil || code != 0 {
		return fmt.Errorf("pebble index should be rebuilt and present (%d): %s\n%s", code, err, out)
	}

	return nil
}
