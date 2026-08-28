package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/stretchr/testify/require"
)

// DVUnpartTable and DVPartTable are the two source tables the deletion-vector suite drives.
// They are narrow and purpose-built (5 columns, not the ~50-column datatype-matrix table every
// other suite uses) because these tests need precise control over which DATA FILE a delete
// lands on, and over how many partitions exist - not datatype coverage.
//
// Both tables are always created, seeded and synced together: every scenario below runs one
// sync that advances both streams, then checks both. DVPartTable is not a special case with its
// own logic - it is exactly DVUnpartTable's schema, with a partition_regex set on its stream
// (see dvCatalogDoc). That is what makes it "the partitioned one."
const (
	DVUnpartTable = "dv_unpart"
	DVPartTable   = "dv_part"
)

// icebergDVTestDrivers gates the deletion-vector suite the same way icebergTableIndexTestDrivers
// gates the pos/table-index suite (see hasIcebergTableIndexTest). One CDC-capable driver is
// enough - these exercise destination behavior, not source specifics.
var icebergDVTestDrivers = []constants.DriverType{constants.MySQL}

func hasIcebergDVTest(driver string) bool {
	return slices.Contains(icebergDVTestDrivers, constants.DriverType(driver))
}

// dvColumnSchema is the type_schema every dv stream shares - both tables have the same
// columns, so this is built once and reused for both catalog entries.
func dvColumnSchema() map[string]interface{} {
	return map[string]interface{}{
		"id":                    map[string]interface{}{"type": []interface{}{"integer_small"}, "destination_column_name": "id"},
		"customer":              map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "customer"},
		"amount":                map[string]interface{}{"type": []interface{}{"number", "null"}, "destination_column_name": "amount"},
		"status":                map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "status"},
		"updated_at":            map[string]interface{}{"type": []interface{}{"timestamp", "null"}, "destination_column_name": "updated_at"},
		"_op_type":              map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_op_type", "olake_column": true},
		"_olake_id":             map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_olake_id", "olake_column": true},
		"_olake_timestamp":      map[string]interface{}{"type": []interface{}{"timestamp_micro", "null"}, "destination_column_name": "_olake_timestamp", "olake_column": true},
		"_cdc_timestamp":        map[string]interface{}{"type": []interface{}{"timestamp_micro", "null"}, "destination_column_name": "_cdc_timestamp", "olake_column": true},
		"_cdc_binlog_file_name": map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_cdc_binlog_file_name", "olake_column": true},
		"_cdc_binlog_file_pos":  map[string]interface{}{"type": []interface{}{"integer", "null"}, "destination_column_name": "_cdc_binlog_file_pos", "olake_column": true},
	}
}

// dvStreamEntry builds one `streams[]` entry, matching the shape discover would produce for a
// table this narrow.
//
// destinationDatabase is set explicitly here rather than left for `--destination-database-prefix`
// to fill in: that flag only ever gets baked into a stream's destination_database by `discover`
// itself (utils.GenerateDestinationDetails, types/catalog.go's discover-time logic) - it has no
// effect on a catalog written directly like this one. Leaving destination_database unset would
// make GetDestinationDatabase fall back to the destination config's own iceberg_db namespace
// instead of the one every Spark query in this suite expects (cfg.DestinationDB) - the sync would
// still succeed, just against the wrong namespace, which looks like the table never got created.
func dvStreamEntry(namespace, table, destinationDatabase string) map[string]interface{} {
	return map[string]interface{}{
		"stream": map[string]interface{}{
			"name":                       table,
			"namespace":                  namespace,
			"type_schema":                map[string]interface{}{"properties": dvColumnSchema()},
			"supported_sync_modes":       []interface{}{"strict_cdc", "full_refresh", "incremental", "cdc"},
			"source_defined_primary_key": []interface{}{"id"},
			"available_cursor_fields":    []interface{}{"id", "customer", "amount", "status", "updated_at"},
			"sync_mode":                  "cdc",
			"destination_database":       destinationDatabase,
			"destination_table":          table,
			"default_stream_properties":  map[string]interface{}{"normalization": true, "append_mode": false},
		},
	}
}

// dvSelectedEntry builds one `selected_streams[namespace][]` entry. partitionRegex is "" for
// the unpartitioned table.
func dvSelectedEntry(table, updateType, partitionRegex string) map[string]interface{} {
	return map[string]interface{}{
		"stream_name":     table,
		"update_type":     updateType,
		"partition_regex": partitionRegex,
		"normalization":   true,
		"append_mode":     false,
	}
}

// dvCatalogDoc builds a whole streams.json selecting both dv tables under CDC, in the given
// update_type, with DVPartTable's stream carrying a partition_regex (identity on `status`) and
// DVUnpartTable's carrying none. Written directly rather than derived from a discover run or a
// checked-in fixture, since the shape needed here (two narrow, purpose-built tables) has nothing
// in common with the wide datatype-matrix fixture every other suite seeds from.
func dvCatalogDoc(namespace, destinationDatabase, updateType string) map[string]interface{} {
	return map[string]interface{}{
		"streams": []interface{}{
			dvStreamEntry(namespace, DVUnpartTable, destinationDatabase),
			dvStreamEntry(namespace, DVPartTable, destinationDatabase),
		},
		"selected_streams": map[string]interface{}{
			namespace: []interface{}{
				dvSelectedEntry(DVUnpartTable, updateType, ""),
				dvSelectedEntry(DVPartTable, updateType, "/{status,identity}"),
			},
		},
	}
}

// prepareDVSync creates and seeds both dv tables, writes a fresh catalog selecting both under
// the given update_type, and resets sync state - the dv-suite equivalent of
// prepareTableIndexSync in iceberg_index.go.
//
// Every scenario drops and recreates dv_unpart/dv_part under the SAME table names, so each one
// gets a brand new Iceberg table with its own fresh snapshot history. The on-disk table index
// (a Pebble store bind-mounted from HostTestDataPath) does not know that - left alone, it
// replays whatever WAL a PRIOR scenario's now-deleted table left behind and hands the new
// table a bookmark snapshot ID that belongs to a completely different table's history. That
// bookmark can never be an ancestor of anything the new table produces, and the server has no
// way to tell "stale, unrelated table" apart from "genuinely diverged" - both surface as the
// same "not an ancestor, full scan required" error. iceberg_index.go's prepareTableIndexSync
// already wipes this directory per scenario for exactly this reason; missing it here is what
// caused a real, reproducible failure the first time this suite actually ran.
func (cfg *IntegrationTest) prepareDVSync(ctx context.Context, t *testing.T, updateType string) error {
	t.Helper()

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-drop")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-create")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-seed")

	doc := dvCatalogDoc(cfg.Namespace, cfg.DestinationDB, updateType)
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to build dv catalog: %w", err)
	}
	if err := writeHostFile(cfg.TestConfig.HostCatalogPath, raw); err != nil {
		return fmt.Errorf("failed to write dv catalog: %w", err)
	}

	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state file: %w", err)
	}

	if err := dvClearTableIndex(ctx, t, cfg); err != nil {
		return fmt.Errorf("failed to clear table index: %w", err)
	}
	return nil
}

// dvClearTableIndex wipes the on-disk table index between scenarios. Deliberately NOT
// os.RemoveAll from the host: the driver image has no USER directive (runs as root), so the
// index files it writes into the bind mount are root-owned, and a non-root host process (a CI
// runner, most dev machines) gets "permission denied" trying to unlink them directly - the same
// class of UID mismatch the CI workflow already chowns around for the iceberg-postgres bind
// mount. Deleting from inside a container run as the same root user sidesteps that instead of
// hoping a host-side removal happens to succeed, or silently ignoring a failure that would
// leave a stale WAL for the next scenario to wrongly replay (see prepareDVSync).
func dvClearTableIndex(ctx context.Context, t *testing.T, cfg *IntegrationTest) error {
	t.Helper()
	indexDir := filepath.Join(cfg.TestConfig.HostTestDataPath, "olake-table-index")
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		return nil
	}

	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:%s", cfg.TestConfig.HostTestDataPath, containerTestDataDir),
		"--entrypoint", "rm",
		driverImageRef(cfg.TestConfig.Driver),
		"-rf", path.Join(containerTestDataDir, "olake-table-index"),
	}
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("container rm failed: %w: %s", err, out)
	}
	return nil
}

// dvSetUpdateType flips update_type for both dv streams between syncs - the two-table
// equivalent of updateUpdateType.
func dvSetUpdateType(config *TestConfig, namespace, updateType string) error {
	for _, table := range []string{DVUnpartTable, DVPartTable} {
		if err := updateUpdateType(config, namespace, table, updateType); err != nil {
			return fmt.Errorf("failed to set update_type on %s: %w", table, err)
		}
	}
	return nil
}

// dvSelectWriter points the config at the arrow or legacy destination file, matching
// testIcebergWriter's pattern: writer variants are separate committed config files, so a
// scenario switches between them by pointing IcebergDestinationPath at one, never by editing a
// single file in place.
func dvSelectWriter(cfg *IntegrationTest, useArrow bool) {
	file := "iceberg_destination.json"
	if useArrow {
		file = "iceberg_destination_arrow.json"
	}
	cfg.TestConfig.IcebergDestinationPath = path.Join(containerTestDataDir, file)
}

// dvSyncArgs is syncArgs with the destination-database-prefix flag already applied.
//
// Deliberately its OWN prefix ("..._dv"), not the "integration_<driver>" one
// iceberg_index.go's scenarios and TestMySQLSync share: every sync, regardless of which
// stream it is actually syncing, also runs destination.Check() against a table named purely
// from this prefix (destination/writers.go's "destination check" phase, Iceberg.Check()'s
// fixed "<prefix>_test_olake" table) - a GET_OR_CREATE_TABLE that is not safe under concurrent
// callers sharing the same name (OlakeRowsIngester.loadOrCreateTable races check-then-create).
// This suite runs many more syncs, all in parallel with TestMySQLSync/TestMySQL2PC (all
// t.Parallel()); sharing their prefix means sharing that race's exact target, which is what
// surfaced it as "Table already exists" failures here. A distinct prefix gives this suite its
// own check-table, so its syncs only ever race against each other - and this suite's own
// scenarios run sequentially, not in parallel, so there is no race left to hit.
func dvSyncArgs(cfg *IntegrationTest) []string {
	destDBPrefix := Ternary(cfg.TestConfig.DataFormat != "",
		fmt.Sprintf("integration_%s_%s_dv", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat),
		fmt.Sprintf("integration_%s_dv", cfg.TestConfig.Driver)).(string)
	return syncArgs(*cfg.TestConfig, true, "iceberg", "--destination-database-prefix", destDBPrefix)
}

func dvFullTableName(cfg *IntegrationTest, table string) string {
	return fmt.Sprintf("%s.%s.%s", icebergCatalog, cfg.DestinationDB, table)
}

// dvSync runs one sync with the given writer, then fails the test immediately on a non-zero
// exit - every dv scenario is a sequence of these.
func (cfg *IntegrationTest) dvSync(ctx context.Context, t *testing.T, useArrow bool, label string) {
	t.Helper()
	dvSelectWriter(cfg, useArrow)
	code, out, err := runOlake(ctx, t, cfg.TestConfig, dvSyncArgs(cfg)...)
	require.NoError(t, err, "%s sync failed to launch: %s", label, out)
	require.Zerof(t, code, "%s sync exited %d: %s", label, code, out)
}

// dvTableState is the full picture one table's assertions are checked against after a sync.
type dvTableState struct {
	liveRows           map[string]string // id -> _op_type, from every live row
	posOrDVDeleteFiles int64             // delete_files, content=1 (positional or DV, current snapshot)
	eqDeleteFiles      int64             // delete_files, content=2 (equality, current snapshot)
	parquetPosFiles    int64             // delete_files, content=1, file_format=PARQUET
	puffinVectorFiles  int64             // delete_files, content=1, file_format=PUFFIN
	duplicateVectors   int64             // referenced_data_file appearing more than once among live PUFFIN vectors - must be 0
	formatVersion      int64
}

func dvState(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) dvTableState {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)

	return dvTableState{
		liveRows:           queryLiveOpTypesByID(ctx, t, spark, fullTableName),
		posOrDVDeleteFiles: countDeleteFiles(ctx, t, spark, fullTableName, 1),
		eqDeleteFiles:      countDeleteFiles(ctx, t, spark, fullTableName, 2),
		parquetPosFiles:    countDeleteFilesByFormat(ctx, t, spark, fullTableName, 1, "PARQUET"),
		puffinVectorFiles:  countDeleteFilesByFormat(ctx, t, spark, fullTableName, 1, "PUFFIN"),
		duplicateVectors:   duplicateVectorCount(ctx, t, spark, fullTableName),
		formatVersion:      formatVersion(ctx, t, spark, fullTableName),
	}
}

// queryLiveOpTypesByID is queryLiveOpTypes keyed on the dv tables' own primary key column - the
// existing helper is keyed on col_bigserial, which these tables do not have.
func queryLiveOpTypesByID(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) map[string]string {
	t.Helper()
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT id, _op_type FROM %s", fullTableName))
	require.NoError(t, err)
	rows, err := df.Collect(ctx)
	require.NoError(t, err)

	result := make(map[string]string, len(rows))
	for _, r := range rows {
		result[fmt.Sprintf("%v", r.Value("id"))] = fmt.Sprintf("%v", r.Value("_op_type"))
	}
	return result
}

func countDeleteFilesByFormat(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, content int, format string) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	return countSpark(ctx, t, spark, fmt.Sprintf(
		"SELECT count(*) as cnt FROM %s.delete_files WHERE content = %d AND file_format = '%s'",
		fullTableName, content, format,
	))
}

// duplicateVectorCount is "at most one live deletion vector per data file" as a count: any
// referenced_data_file appearing more than once among current-snapshot PUFFIN entries means a
// superseded vector was left behind instead of being replaced. Must be 0 on every dv table.
func duplicateVectorCount(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	return countSpark(ctx, t, spark, fmt.Sprintf(`
		SELECT count(*) as cnt FROM (
			SELECT referenced_data_file FROM %s.delete_files
			WHERE file_format = 'PUFFIN'
			GROUP BY referenced_data_file HAVING count(*) > 1
		)`, fullTableName))
}

// dvHistoryIncreasing returns the record_count of every PUFFIN vector ever written for
// fullTableName, oldest snapshot first. A merge that actually unions old and new positions
// produces a strictly increasing sequence per data file; a merge that silently reset instead of
// merging produces a flat one. Grouped by referenced_data_file since more than one file may have
// history in the same table (dv_part, once seeded across partitions, always does).
func dvHistoryIncreasing(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) map[string][]int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	df, err := spark.Sql(ctx, fmt.Sprintf(`
		SELECT referenced_data_file, record_count FROM %s.all_delete_files
		WHERE file_format = 'PUFFIN'
		ORDER BY referenced_data_file, record_count`, fullTableName))
	require.NoError(t, err)
	rows, err := df.Collect(ctx)
	require.NoError(t, err)

	out := map[string][]int64{}
	for _, r := range rows {
		file := fmt.Sprintf("%v", r.Value("referenced_data_file"))
		count, ok := r.Value("record_count").(int64)
		require.True(t, ok, "record_count is not int64: %T", r.Value("record_count"))
		out[file] = append(out[file], count)
	}
	return out
}

func removedDeleteFileCount(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	df, err := spark.Sql(ctx, fmt.Sprintf(
		"SELECT summary['removed-delete-files'] AS removed FROM %s.snapshots ORDER BY committed_at DESC LIMIT 1",
		fullTableName))
	require.NoError(t, err)
	rows, err := df.Collect(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	removed := fmt.Sprintf("%v", rows[0].Value("removed"))
	var n int64
	_, err = fmt.Sscanf(removed, "%d", &n)
	require.NoError(t, err, "removed-delete-files is not numeric: %q", removed)
	return n
}

// formatVersion reads the table's format-version property. Iceberg's Spark catalog surfaces it
// through SHOW TBLPROPERTIES like any other table property, even though it is intrinsic
// metadata rather than something OLake or a user ever sets directly.
func formatVersion(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) int64 {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	df, err := spark.Sql(ctx, fmt.Sprintf("SHOW TBLPROPERTIES %s", fullTableName))
	require.NoError(t, err)
	rows, err := df.Collect(ctx)
	require.NoError(t, err)
	for _, r := range rows {
		if fmt.Sprintf("%v", r.Value("key")) == "format-version" {
			var n int64
			_, err := fmt.Sscanf(fmt.Sprintf("%v", r.Value("value")), "%d", &n)
			require.NoError(t, err, "format-version is not numeric on %s", fullTableName)
			return n
		}
	}
	t.Fatalf("no format-version property found on %s", fullTableName)
	return 0
}

// requireLiveRowsEqual is the row-correctness check every dv scenario ends with: the live id ->
// _op_type map must be identical across both tables and must match the caller's expectation
// exactly, no more and no fewer ids.
func requireLiveRowsEqual(t *testing.T, want map[string]string, got dvTableState, tableLabel string) {
	t.Helper()
	require.Equal(t, want, got.liveRows, "%s: unexpected live row set", tableLabel)
}

// -----------------------------------------------------------------------------------------------
// Scenarios. Each runs a source-side step against both tables, syncs once, then asserts against
// both tables' state - a scenario passes only if both agree.
// -----------------------------------------------------------------------------------------------

// testIcebergDVBackfillAndCore backfills both dv tables, then runs one CDC sync that updates one
// row and deletes another. It is the smallest possible proof that "dv" actually reaches the
// server end to end: a table created under this mode should be on format version 3 from the
// very first sync (never 2, upgraded later), and every delete it produces should show up as a
// Puffin file, never a Parquet one.
func (cfg *IntegrationTest) testIcebergDVBackfillAndCore(ctx context.Context, t *testing.T, useArrow bool) error {
	defer dropIcebergTable(t, DVUnpartTable, cfg.DestinationDB)
	defer dropIcebergTable(t, DVPartTable, cfg.DestinationDB)

	if err := cfg.prepareDVSync(ctx, t, "dv"); err != nil {
		return err
	}

	cfg.dvSync(ctx, t, useArrow, "backfill")

	spark := getSparkSession(ctx, t)
	for _, table := range []string{DVUnpartTable, DVPartTable} {
		full := dvFullTableName(cfg, table)
		state := dvState(ctx, t, spark, full)
		require.Equal(t, int64(3), state.formatVersion, "%s: table created under dv must start at format version 3", table)
		require.Zero(t, state.posOrDVDeleteFiles, "%s: fresh backfill should have no deletes yet", table)
	}

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-update-id1")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-update-id1")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-delete-id5")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-delete-id5")
	cfg.dvSync(ctx, t, useArrow, "cdc update+delete")

	// A delete does not remove its id from a plain SELECT: the old row is removed via the
	// delete mechanism, but a new tombstone row recording the delete event stays, carrying
	// _op_type='d' - the same convention every other suite's delete checks rely on
	// (test_utils.go's "WHERE _op_type = 'd'" verification). id=5 is "d", not absent.
	wantRows := map[string]string{"1": "u", "2": "r", "3": "r", "4": "r", "5": "d"}
	for _, table := range []string{DVUnpartTable, DVPartTable} {
		full := dvFullTableName(cfg, table)
		state := dvState(ctx, t, spark, full)
		requireLiveRowsEqual(t, wantRows, state, table)
		require.Equal(t, int64(0), state.parquetPosFiles, "%s: dv mode must never write a Parquet positional delete", table)
		require.Greater(t, state.puffinVectorFiles, int64(0), "%s: expected at least one deletion vector after a delete", table)
		require.Equal(t, int64(3), state.formatVersion, "%s: format version must stay 3", table)
	}
	return nil
}

// testIcebergDVMergeAcrossSyncs is the single highest-value scenario in this suite. It deletes
// id=2 in one sync and id=3 in the next, both from the SAME data file (they share a status).
// A deletion vector REPLACES the previous vector for a data file rather than adding to it, so
// writing the second sync's vector without first reading the first sync's would silently
// resurrect id=2. That is exactly the bug this test exists to catch.
//
// On dv_part, id=2 and id=3 still land in one shared data file (same status, same partition),
// but the table's OTHER seed rows sit in different partitions - so this also proves the single
// shared vector writer used for the whole commit handles more than one partition's data files
// correctly, not just the one dv_unpart happens to have.
func (cfg *IntegrationTest) testIcebergDVMergeAcrossSyncs(ctx context.Context, t *testing.T, useArrow bool) error {
	defer dropIcebergTable(t, DVUnpartTable, cfg.DestinationDB)
	defer dropIcebergTable(t, DVPartTable, cfg.DestinationDB)

	if err := cfg.prepareDVSync(ctx, t, "dv"); err != nil {
		return err
	}
	cfg.dvSync(ctx, t, useArrow, "backfill")

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-delete-id2")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-delete-id2")
	cfg.dvSync(ctx, t, useArrow, "delete id=2")

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-delete-id3")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-delete-id3")
	cfg.dvSync(ctx, t, useArrow, "delete id=3")

	spark := getSparkSession(ctx, t)
	// Same tombstone convention as testIcebergDVBackfillAndCore: id=2 and id=3 stay present
	// with _op_type='d', they don't disappear.
	wantRows := map[string]string{"1": "r", "2": "d", "3": "d", "4": "r", "5": "r"}
	for _, table := range []string{DVUnpartTable, DVPartTable} {
		full := dvFullTableName(cfg, table)
		state := dvState(ctx, t, spark, full)

		requireLiveRowsEqual(t, wantRows, state, table)
		require.Equal(t, int64(0), state.duplicateVectors,
			"%s: the id=2 sync's vector must have been REPLACED by id=3's, not left alongside it - a non-zero count here means id=2 could read as un-deleted again", table)
		require.Greater(t, removedDeleteFileCount(ctx, t, spark, full), int64(0),
			"%s: the superseded vector from the id=2 sync should show up as removed in the id=3 sync's snapshot summary", table)

		for file, history := range dvHistoryIncreasing(ctx, t, spark, full) {
			for i := 1; i < len(history); i++ {
				require.Greaterf(t, history[i], history[i-1],
					"%s: vector history for %s is not increasing (%v) - a flat or shrinking sequence means a later sync overwrote instead of merging with the earlier one",
					table, file, history)
			}
		}
	}
	return nil
}

// testIcebergEqToDVMigration backfills both tables under "eq" so they accumulate real equality
// deletes, then flips update_type to "dv" mid-stream. OLake's handshake detects the leftover
// equality deletes and migrates them to deletion vectors in one commit before any dv write
// happens, rather than the table passing through positional deletes on the way. The row set
// must come out identical to what it was under eq - a migration that changes what a query
// returns is a correctness bug regardless of which representation it lands on.
func (cfg *IntegrationTest) testIcebergEqToDVMigration(ctx context.Context, t *testing.T, useArrow bool) error {
	defer dropIcebergTable(t, DVUnpartTable, cfg.DestinationDB)
	defer dropIcebergTable(t, DVPartTable, cfg.DestinationDB)

	if err := cfg.prepareDVSync(ctx, t, "eq"); err != nil {
		return err
	}
	cfg.dvSync(ctx, t, useArrow, "eq backfill")

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-update-id1")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-update-id1")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-delete-id5")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-delete-id5")
	cfg.dvSync(ctx, t, useArrow, "eq cdc")

	spark := getSparkSession(ctx, t)
	rowsBefore := map[string]dvTableState{}
	for _, table := range []string{DVUnpartTable, DVPartTable} {
		state := dvState(ctx, t, spark, dvFullTableName(cfg, table))
		require.Equal(t, int64(2), state.formatVersion, "%s: eq mode does not need v3 yet", table)
		require.Greater(t, state.eqDeleteFiles, int64(0), "%s: eq mode should have produced equality deletes to migrate", table)
		rowsBefore[table] = state
	}

	if err := dvSetUpdateType(cfg.TestConfig, cfg.Namespace, "dv"); err != nil {
		return err
	}
	// Nothing changes on the source between eq and dv - the migration alone is what this
	// sync exercises, triggered purely by the update_type flip.
	cfg.dvSync(ctx, t, useArrow, "migrate eq to dv")

	for _, table := range []string{DVUnpartTable, DVPartTable} {
		full := dvFullTableName(cfg, table)
		after := dvState(ctx, t, spark, full)
		require.Equal(t, int64(3), after.formatVersion, "%s: migrating to dv must raise format version to 3", table)
		require.Equal(t, int64(0), after.eqDeleteFiles, "%s: no equality deletes should remain after migration", table)
		requireLiveRowsEqual(t, rowsBefore[table].liveRows, after, table)
	}

	// A further sync after migration should behave exactly like a table that was native dv
	// from the start - re-run the merge-across-syncs style check once more.
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-unpart-delete-id2")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "dv-part-delete-id2")
	cfg.dvSync(ctx, t, useArrow, "post-migration delete")
	for _, table := range []string{DVUnpartTable, DVPartTable} {
		state := dvState(ctx, t, spark, dvFullTableName(cfg, table))
		require.Equal(t, int64(0), state.parquetPosFiles, "%s: post-migration deletes must still be vectors, not Parquet", table)
	}
	return nil
}

// TestIcebergDV runs the deletion-vector suite. Every sync in every scenario is a real docker
// container launch (source + destination + Spark), so the scenario count here is deliberately
// kept to what actually needs its own run - each one is minutes, not seconds, and this suite
// runs inside the same per-driver job budget as everything else in the mysql package.
//
// All three remaining scenarios run under BOTH writers, because that is the whole point of each:
// they are checking the rows writer and the arrow writer produce the same Iceberg state, or (for
// migration) that CDC syncs on either writer behave the same before and after the mode switch.
//
// Two prior scenarios were cut for cost rather than kept "for completeness": an append-mode
// check and an eq/pos-rejection check, both real behavior but both writer-independent (append
// mode never writes a delete file regardless of writer; the eq/pos rejection is enforced
// server-side at handshake, before any writer is chosen) and both fully covered in spirit by
// unit-level or Iceberg-bytecode verification already recorded in DV_IMPLEMENTATION.md, so
// spending real container-launch time on them here wasn't worth it. A prior arrow-only
// rolled-file scenario was cut earlier for the same reason: at a size that fits inside an
// integration test's time budget, it never actually reached the writer's real file-roll
// threshold, so it exercised nothing beyond what backfill/core already covers.
func (cfg *IntegrationTest) TestIcebergDV(t *testing.T) {
	ctx := t.Context()
	if !hasIcebergDVTest(cfg.TestConfig.Driver) {
		t.Skip("deletion-vector suite is gated to specific drivers")
	}

	writerTypes := []struct {
		name     string
		useArrow bool
	}{
		{"Legacy", false},
		{"Arrow", true},
	}

	for _, wt := range writerTypes {
		t.Run(wt.name, func(t *testing.T) {
			t.Run("Backfill and core", func(t *testing.T) {
				require.NoError(t, cfg.testIcebergDVBackfillAndCore(ctx, t, wt.useArrow))
			})
			t.Run("Merge across syncs", func(t *testing.T) {
				require.NoError(t, cfg.testIcebergDVMergeAcrossSyncs(ctx, t, wt.useArrow))
			})
			t.Run("Eq to DV migration", func(t *testing.T) {
				require.NoError(t, cfg.testIcebergEqToDVMigration(ctx, t, wt.useArrow))
			})
		})
	}
}
