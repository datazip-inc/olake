package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// DDLBacklogTable and DDLSwapTable are the two source tables the DDL suite drives. Without
// binlog_row_metadata=FULL (MySQL 5.7, or 8.x at MINIMAL) row events carry no column names, so
// OLake decodes them with column metadata it saved in the state and advances through each DDL in
// binlog order. A row decodes right only when every DDL before it, and none after it, reached
// that metadata. On a FULL server every row carries its own metadata, so the same checks hold
// without the cache: the suite runs on every MySQL stack and expects the same rows on each.
const (
	DDLBacklogTable = "ddl_backlog"
	DDLSwapTable    = "ddl_swap"
)

// ddlTestDrivers gates the DDL suite the way icebergDVTestDrivers gates the deletion-vector one:
// the binlog column cache it exercises is MySQL's.
var ddlTestDrivers = []constants.DriverType{constants.MySQL}

func hasDDLTest(driver string) bool {
	return slices.Contains(ddlTestDrivers, constants.DriverType(driver))
}

// ddlColumnSchema is the type_schema both DDL tables start with. Columns a DDL adds later reach
// the destination through schema evolution, as in the main suite's evolve-schema step.
func ddlColumnSchema() map[string]interface{} {
	return map[string]interface{}{
		"id":                    map[string]interface{}{"type": []interface{}{"integer_small"}, "destination_column_name": "id"},
		"a":                     map[string]interface{}{"type": []interface{}{"integer_small", "null"}, "destination_column_name": "a"},
		"b":                     map[string]interface{}{"type": []interface{}{"integer_small", "null"}, "destination_column_name": "b"},
		"status":                map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "status"},
		"_op_type":              map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_op_type", "olake_column": true},
		"_olake_id":             map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_olake_id", "olake_column": true},
		"_olake_timestamp":      map[string]interface{}{"type": []interface{}{"timestamp_micro", "null"}, "destination_column_name": "_olake_timestamp", "olake_column": true},
		"_cdc_timestamp":        map[string]interface{}{"type": []interface{}{"timestamp_micro", "null"}, "destination_column_name": "_cdc_timestamp", "olake_column": true},
		"_cdc_binlog_file_name": map[string]interface{}{"type": []interface{}{"string", "null"}, "destination_column_name": "_cdc_binlog_file_name", "olake_column": true},
		"_cdc_binlog_file_pos":  map[string]interface{}{"type": []interface{}{"integer", "null"}, "destination_column_name": "_cdc_binlog_file_pos", "olake_column": true},
	}
}

// ddlCatalogDoc builds a streams.json selecting both DDL tables under CDC. Like dvCatalogDoc it
// sets destination_database itself.
func ddlCatalogDoc(namespace, destinationDatabase string) map[string]interface{} {
	streams := []interface{}{}
	selected := []interface{}{}
	for _, table := range []string{DDLBacklogTable, DDLSwapTable} {
		streams = append(streams, map[string]interface{}{
			"stream": map[string]interface{}{
				"name":                       table,
				"namespace":                  namespace,
				"type_schema":                map[string]interface{}{"properties": ddlColumnSchema()},
				"supported_sync_modes":       []interface{}{"strict_cdc", "full_refresh", "incremental", "cdc"},
				"source_defined_primary_key": []interface{}{"id"},
				"available_cursor_fields":    []interface{}{"id", "a", "b", "status"},
				"sync_mode":                  "cdc",
				"destination_database":       destinationDatabase,
				"destination_table":          table,
				"default_stream_properties":  map[string]interface{}{"normalization": true, "append_mode": false},
			},
		})
		selected = append(selected, map[string]interface{}{
			"stream_name":     table,
			"partition_regex": "",
			"normalization":   true,
			"append_mode":     false,
		})
	}
	return map[string]interface{}{
		"streams":          streams,
		"selected_streams": map[string]interface{}{namespace: selected},
	}
}

// ddlSync runs one sync and fails the test on a failed run. A suite's first sync runs without
// state; every later one resumes it.
func (th *TestHandler) ddlSync(ctx context.Context, t *testing.T, useState bool, label string) {
	t.Helper()
	require.NoError(t, testutils.RunSync(ctx, t, th.TestConfig, icebergDestinationFile, useState), "%s sync failed", label)
}

// ddlRows reads the given columns of every row in a destination table, keyed by id, with each
// value printed as text and NULL for a missing one.
func ddlRows(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, columns ...string) map[string]map[string]string {
	t.Helper()
	refreshTable(ctx, t, spark, fullTableName)
	query := fmt.Sprintf("SELECT id, %s FROM %s", strings.Join(columns, ", "), fullTableName)
	df, err := spark.Sql(ctx, query)
	require.NoError(t, err, "spark sql failed: %s", query)
	rows, err := df.Collect(ctx)
	require.NoError(t, err, "spark collect failed: %s", query)

	out := make(map[string]map[string]string, len(rows))
	for _, r := range rows {
		row := make(map[string]string, len(columns))
		for _, column := range columns {
			value := r.Value(column)
			row[column] = testutils.Ternary(value == nil, "NULL", fmt.Sprint(value)).(string)
		}
		out[fmt.Sprint(r.Value("id"))] = row
	}
	return out
}

// TestDDL syncs both tables three times: an initial sync records the binlog position, the
// second decodes one row per table and saves its column metadata while another table takes
// writes, and the third reads a backlog of rows and DDL written while no sync ran. ExecuteQuery
// owns the SQL (ddl-create, ddl-seed, ddl-noise-start / -stop, ddl-backlog, ddl-drop).
//
// In the backlog, ddl_backlog takes a reorder, an ENUM change behind a "--" comment, an ADD
// COLUMN with no character set, DDL that must leave it alone (CREATE TABLE IF NOT EXISTS of
// itself, a trigger, DDL on other tables, one the parser cannot read) and a second reorder, with
// a row after each. ddl_swap takes a gh-ost cutover: a shadow table is altered, a row lands in
// ddl_swap during the copy, then RENAME swaps the two. Then ddl_backlog is dropped and created
// again as the only cached table, and an app's startup CREATE TABLE IF NOT EXISTS replays
// ddl_swap's layout before the cutover.
func (th *TestHandler) TestDDL(t *testing.T) {
	ctx := t.Context()
	if !hasDDLTest(th.Driver) {
		t.Skip("DDL suite is gated to specific drivers")
	}
	require.NoError(t, th.IsolateDestinationDB(), "failed to give the DDL suite its own destination namespace")

	defer testutils.DropIcebergTable(t, DDLBacklogTable, th.TestConfig.DestinationDB)
	defer testutils.DropIcebergTable(t, DDLSwapTable, th.TestConfig.DestinationDB)
	if !testutils.KeepTestData() {
		defer th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-drop")
	}

	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-create")
	raw, err := json.MarshalIndent(ddlCatalogDoc(th.Namespace, th.TestConfig.DestinationDB), "", "  ")
	require.NoError(t, err, "failed to build the DDL catalog")
	require.NoError(t, testutils.WriteHostFile(th.TestConfig.GetFilePath("streams.json"), raw), "failed to write the DDL catalog")

	th.ddlSync(ctx, t, false, "initial")

	// Writes elsewhere during the saving sync move the server past the position it stops at, so
	// the metadata it loads is anchored after that position: the busy-server case.
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-seed")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-noise-start")
	defer th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-noise-stop")
	th.ddlSync(ctx, t, true, "saving")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-noise-stop")

	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "ddl-backlog")
	th.ddlSync(ctx, t, true, "backlog")

	spark := getSparkSession(ctx, t)
	t.Run("Backlog of DDL", func(t *testing.T) {
		got := ddlRows(ctx, t, spark, fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, DDLBacklogTable), "a", "b", "status", "note")
		require.Equal(t, map[string]map[string]string{
			"100": {"a": "0", "b": "0", "status": "new", "note": "NULL"},
			"1":   {"a": "10", "b": "20", "status": "paid", "note": "NULL"},     // saved metadata, from the busy sync
			"2":   {"a": "11", "b": "21", "status": "paid", "note": "NULL"},     // after the reorder
			"3":   {"a": "12", "b": "22", "status": "refunded", "note": "NULL"}, // after the commented ENUM change
			"4":   {"a": "13", "b": "23", "status": "new", "note": "n4"},        // after ADD COLUMN with no charset
			"5":   {"a": "14", "b": "24", "status": "paid", "note": "n5"},       // after DDL that must not evict the table
			"6":   {"a": "15", "b": "25", "status": "refunded", "note": "n6"},   // after the second reorder
			"7":   {"a": "16", "b": "26", "status": "paid", "note": "n7"},       // recreated as the only cached table
			"8":   {"a": "17", "b": "27", "status": "new", "note": "n8"},        // after a reorder of the recreated table
		}, got)
	})
	t.Run("gh-ost cutover", func(t *testing.T) {
		got := ddlRows(ctx, t, spark, fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.TestConfig.DestinationDB, DDLSwapTable), "a", "b", "status")
		require.Equal(t, map[string]map[string]string{
			"100": {"a": "0", "b": "0", "status": "new"},
			"1":   {"a": "10", "b": "20", "status": "paid"}, // before the swap
			"2":   {"a": "11", "b": "21", "status": "paid"}, // during the copy, between DDLs on the shadow table
			"3":   {"a": "12", "b": "22", "status": "new"},  // after the swap, in the new layout
			"4":   {"a": "13", "b": "23", "status": "paid"}, // after a startup CREATE TABLE IF NOT EXISTS with the old layout
		}, got)
	})
}
