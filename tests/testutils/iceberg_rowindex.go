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

var icebergRowIndexTestDrivers = []constants.DriverType{constants.Postgres}

// hasIcebergRowIndexTest reports whether the driver participates in the Iceberg RowIndex integration tests.
func hasIcebergRowIndexTest(driver string) bool {
	return slices.Contains(icebergRowIndexTestDrivers, constants.DriverType(driver))
}

// getSparkSession returns a connected SparkSession for querying Iceberg metadata/tables
func getSparkSession(ctx context.Context, t *testing.T) sql.SparkSession {
	t.Helper()
	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	return spark
}

// testIcebergEqToPosConversion tests conversion of equality delete files to positional delete files
// when the Iceberg table already contains equality deletes from a previous sync.
func (cfg *IntegrationTest) testIcebergEqToPosConversion(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	destDBPrefix := fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)
	fullTableName := fmt.Sprintf("olake_iceberg.%s.%s", cfg.DestinationDB, testTable)

	defer DropIcebergTable(t, testTable, cfg.DestinationDB)

	// 1. Reset database table and discover catalog
	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed resetting table: %w", err)
	}

	discoverCmd := DiscoverCommand(*cfg.TestConfig)
	code, out, err := ExecCommand(ctx, c, discoverCmd)
	if err != nil || code != 0 {
		return fmt.Errorf("discover failed (%d): %s\n%s", code, err, out)
	}

	// Update selected streams and set cdc mode
	streamUpdateCmd := UpdateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, true, cfg.ColumnToExclude)
	if code, out, err := ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
		return fmt.Errorf("failed updating selected streams (%d): %s\n%s", code, err, out)
	}

	modeCmd := UpdateStreamConfigCommand(*cfg.TestConfig, cfg.Namespace, testTable, "full_refresh, cdc", "col_cursor")
	if code, out, err := ExecCommand(ctx, c, modeCmd); err != nil || code != 0 {
		return fmt.Errorf("failed setting stream mode (%d): %s\n%s", code, err, out)
	}

	resetCmd := ResetStateFileCommand(*cfg.TestConfig)
	if code, out, err := ExecCommand(ctx, c, resetCmd); err != nil || code != 0 {
		return fmt.Errorf("failed resetting state file (%d): %s\n%s", code, err, out)
	}

	// Step 1: Initial full load + CDC updates using --delete-type eq
	syncEqCmd := SyncCommand(*cfg.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "eq")
	if code, out, err := ExecCommand(ctx, c, syncEqCmd); err != nil || code != 0 {
		return fmt.Errorf("initial full load sync failed (%d): %s\n%s", code, err, out)
	}

	// Perform updates on source DB to generate equality deletes in CDC mode
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "update", false)

	cdcEqCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "eq")
	if code, out, err := ExecCommand(ctx, c, cdcEqCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc eq sync failed (%d): %s\n%s", code, err, out)
	}

	// Verify equality delete files exist before conversion
	spark := getSparkSession(ctx, t)
	eqDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s.delete_files WHERE content = 2", fullTableName))
	require.NoError(t, err)
	eqRows, err := eqDf.Collect(ctx)
	require.NoError(t, err)
	eqCountBefore := eqRows[0].Value("cnt").(int64)
	require.Greater(t, eqCountBefore, int64(0), "expected equality delete files to exist before conversion")

	// Get live records and optypes before conversion
	liveDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT col_bigserial, _op_type FROM %s", fullTableName))
	require.NoError(t, err)
	liveRowsBefore, err := liveDf.Collect(ctx)
	require.NoError(t, err)
	opTypesBefore := make(map[string]string)
	for _, r := range liveRowsBefore {
		idStr := fmt.Sprintf("%v", r.Value("col_bigserial"))
		opStr := fmt.Sprintf("%v", r.Value("_op_type"))
		opTypesBefore[idStr] = opStr
	}
	spark.Stop()

	// Step 2: Next CDC sync using --delete-type pos (triggers equality to positional conversion)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "insert", false)

	cdcPosCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, cdcPosCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc pos sync failed (%d): %s\n%s", code, err, out)
	}

	// Step 3: Verifications after conversion
	spark = getSparkSession(ctx, t)
	defer spark.Stop()

	// Verify NO equality delete files exist
	eqDfAfter, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s.delete_files WHERE content = 2", fullTableName))
	require.NoError(t, err)
	eqRowsAfter, err := eqDfAfter.Collect(ctx)
	require.NoError(t, err)
	eqCountAfter := eqRowsAfter[0].Value("cnt").(int64)
	require.Equal(t, int64(0), eqCountAfter, "expected 0 equality delete files after conversion")

	// Verify positional delete files exist (content = 1)
	posDfAfter, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s.delete_files WHERE content = 1", fullTableName))
	require.NoError(t, err)
	posRowsAfter, err := posDfAfter.Collect(ctx)
	require.NoError(t, err)
	posCountAfter := posRowsAfter[0].Value("cnt").(int64)
	require.Greater(t, posCountAfter, int64(0), "expected positional delete files to exist after conversion")

	// Verify live record optypes match what they were before conversion
	liveDfAfter, err := spark.Sql(ctx, fmt.Sprintf("SELECT col_bigserial, _op_type FROM %s", fullTableName))
	require.NoError(t, err)
	liveRowsAfter, err := liveDfAfter.Collect(ctx)
	require.NoError(t, err)
	opTypesAfter := make(map[string]string)
	for _, r := range liveRowsAfter {
		idStr := fmt.Sprintf("%v", r.Value("col_bigserial"))
		opStr := fmt.Sprintf("%v", r.Value("_op_type"))
		opTypesAfter[idStr] = opStr
	}

	for idStr, opBefore := range opTypesBefore {
		require.Equal(t, opBefore, opTypesAfter[idStr], "optype mismatch for id %s after conversion", idStr)
	}

	return nil
}

// testIcebergCleanTablePositionalWithPebbleIndex tests starting full load on a clean table,
// followed by updates using positional deletes. It verifies positional delete files exist,
// record count matches, and Pebble DB index is created with records matching ingested count.
func (cfg *IntegrationTest) testIcebergCleanTablePositionalWithPebbleIndex(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	destDBPrefix := fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)
	fullTableName := fmt.Sprintf("olake_iceberg.%s.%s", cfg.DestinationDB, testTable)

	defer DropIcebergTable(t, testTable, cfg.DestinationDB)

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed resetting table: %w", err)
	}

	discoverCmd := DiscoverCommand(*cfg.TestConfig)
	if code, out, err := ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
		return fmt.Errorf("discover failed (%d): %s\n%s", code, err, out)
	}

	streamUpdateCmd := UpdateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, true, cfg.ColumnToExclude)
	if code, out, err := ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
		return fmt.Errorf("failed updating selected streams (%d): %s\n%s", code, err, out)
	}

	modeCmd := UpdateStreamConfigCommand(*cfg.TestConfig, cfg.Namespace, testTable, "full_refresh, cdc", "col_cursor")
	if code, out, err := ExecCommand(ctx, c, modeCmd); err != nil || code != 0 {
		return fmt.Errorf("failed setting stream mode (%d): %s\n%s", code, err, out)
	}

	resetCmd := ResetStateFileCommand(*cfg.TestConfig)
	if code, out, err := ExecCommand(ctx, c, resetCmd); err != nil || code != 0 {
		return fmt.Errorf("failed resetting state file (%d): %s\n%s", code, err, out)
	}

	// Step 1: Initial full load with positional deletes
	syncFullCmd := SyncCommand(*cfg.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, syncFullCmd); err != nil || code != 0 {
		return fmt.Errorf("initial full load failed (%d): %s\n%s", code, err, out)
	}

	// Step 2: Run CDC sync with positional deletes after updates
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "update", false)

	cdcPosCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, cdcPosCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc pos sync failed (%d): %s\n%s", code, err, out)
	}

	// Step 3: Verifications
	spark := getSparkSession(ctx, t)
	defer spark.Stop()

	// Verify positional delete files exist (content = 1)
	posDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s.delete_files WHERE content = 1", fullTableName))
	require.NoError(t, err)
	posRows, err := posDf.Collect(ctx)
	require.NoError(t, err)
	posCount := posRows[0].Value("cnt").(int64)
	require.Greater(t, posCount, int64(0), "expected positional delete files to exist")

	// Verify total live record count matches expected seed data count
	countDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s", fullTableName))
	require.NoError(t, err)
	countRows, err := countDf.Collect(ctx)
	require.NoError(t, err)
	liveCount := countRows[0].Value("cnt").(int64)
	expectedCount := int64(len(cfg.ExpectedData))
	if expectedCount > 0 {
		require.Equal(t, expectedCount, liveCount, "live record count should match seed data count")
	} else {
		require.Greater(t, liveCount, int64(0), "expected live records to exist")
	}

	// Verify Pebble DB index directory exists inside container
	checkIdxCmd := "ls /test-olake/olake-row-index"
	code, out, err := ExecCommand(ctx, c, checkIdxCmd)
	if err != nil || code != 0 {
		return fmt.Errorf("expected pebble index directory to exist (%d): %s\n%s", code, err, out)
	}

	return nil
}

// testIcebergRebuildIndexFromScratch tests that when row index is missing/corrupted on disk,
// the sync rebuilds the Pebble DB row index from scratch by scanning the Iceberg table.
func (cfg *IntegrationTest) testIcebergRebuildIndexFromScratch(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	destDBPrefix := fmt.Sprintf("integration_%s", cfg.TestConfig.Driver)
	fullTableName := fmt.Sprintf("olake_iceberg.%s.%s", cfg.DestinationDB, testTable)

	defer DropIcebergTable(t, testTable, cfg.DestinationDB)

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed resetting table: %w", err)
	}

	discoverCmd := DiscoverCommand(*cfg.TestConfig)
	if code, out, err := ExecCommand(ctx, c, discoverCmd); err != nil || code != 0 {
		return fmt.Errorf("discover failed (%d): %s\n%s", code, err, out)
	}

	streamUpdateCmd := UpdateSelectedStreamsCommand(*cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, true, cfg.ColumnToExclude)
	if code, out, err := ExecCommand(ctx, c, streamUpdateCmd); err != nil || code != 0 {
		return fmt.Errorf("failed updating selected streams (%d): %s\n%s", code, err, out)
	}

	modeCmd := UpdateStreamConfigCommand(*cfg.TestConfig, cfg.Namespace, testTable, "full_refresh, cdc", "col_cursor")
	if code, out, err := ExecCommand(ctx, c, modeCmd); err != nil || code != 0 {
		return fmt.Errorf("failed setting stream mode (%d): %s\n%s", code, err, out)
	}

	resetCmd := ResetStateFileCommand(*cfg.TestConfig)
	if code, out, err := ExecCommand(ctx, c, resetCmd); err != nil || code != 0 {
		return fmt.Errorf("failed resetting state file (%d): %s\n%s", code, err, out)
	}

	// Step 1: Initial full load with positional deletes
	syncFullCmd := SyncCommand(*cfg.TestConfig, false, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, syncFullCmd); err != nil || code != 0 {
		return fmt.Errorf("initial full load failed (%d): %s\n%s", code, err, out)
	}

	// Step 2: Delete row index directory inside container to simulate missing/corrupted index
	delIdxCmd := "rm -rf /test-olake/olake-row-index"
	if code, out, err := ExecCommand(ctx, c, delIdxCmd); err != nil || code != 0 {
		return fmt.Errorf("failed deleting row index (%d): %s\n%s", code, err, out)
	}

	// Step 3: Run next sync with updates (triggers index rebuild from scratch)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "update", false)

	cdcPosCmd := SyncCommand(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix, "--delete-type", "pos")
	if code, out, err := ExecCommand(ctx, c, cdcPosCmd); err != nil || code != 0 {
		return fmt.Errorf("cdc sync after index delete failed (%d): %s\n%s", code, err, out)
	}

	// Step 4: Verifications
	spark := getSparkSession(ctx, t)
	defer spark.Stop()

	// Verify positional delete files exist (content = 1)
	posDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s.delete_files WHERE content = 1", fullTableName))
	require.NoError(t, err)
	posRows, err := posDf.Collect(ctx)
	require.NoError(t, err)
	posCount := posRows[0].Value("cnt").(int64)
	require.Greater(t, posCount, int64(0), "expected positional delete files to exist after index rebuild")

	// Verify total live record count matches expected seed data count
	countDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT count(*) as cnt FROM %s", fullTableName))
	require.NoError(t, err)
	countRows, err := countDf.Collect(ctx)
	require.NoError(t, err)
	liveCount := countRows[0].Value("cnt").(int64)
	expectedCount := int64(len(cfg.ExpectedData))
	if expectedCount > 0 {
		require.Equal(t, expectedCount, liveCount, "live record count should match seed data count")
	} else {
		require.Greater(t, liveCount, int64(0), "expected live records to exist")
	}

	// Verify Pebble DB index directory was recreated inside container
	checkIdxCmd := "ls /test-olake/olake-row-index"
	code, out, err := ExecCommand(ctx, c, checkIdxCmd)
	if err != nil || code != 0 {
		return fmt.Errorf("pebble index should be rebuilt and present (%d): %s\n%s", code, err, out)
	}

	return nil
}
