package compatibility

// Comparing the two runs.
//
// The suite's only assertion: the upgrade run's destination must be indistinguishable from the
// reference run's. Everything here reads the two destinations through the shared Spark session --
// row counts, the destination schema, per-_op_type counts, then every row of every value-compared
// column -- and reports the first difference it finds through the variant's diagnostics.

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// compareVariant asserts the upgrade run's destination for one scenario is indistinguishable from
// the reference run's.
func compareVariant(t *testing.T, diag *diagnostics, policies *assertionPolicies, reference, upgraded *testutils.TestConfig, group compatibilityGroup, v compatibilityVariant) {
	if reference.DriverVersion == upgraded.DriverVersion {
		// Both sides ran this case on the same image so skipping comparison
		t.Logf("both sides ran %s on %s; nothing to compare until the upgrade side switches to the candidate",
			v.name, reference.DriverVersion)
		return
	}

	ctx := t.Context()
	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "failed to connect to Spark Connect server")

	referenceDB, upgradedDB := reference.DestinationDB, upgraded.DestinationDB
	referenceTable, upgradedTable := reference.GetTableName(), upgraded.GetTableName()
	var referenceOutput, upgradedOutput string
	switch group.destination {
	case "iceberg":
		referenceOutput = icebergTable(ctx, t, spark, referenceDB, referenceTable)
		upgradedOutput = icebergTable(ctx, t, spark, upgradedDB, upgradedTable)
	case "parquet":
		referenceOutput = parquetView(ctx, t, spark, referenceDB, referenceTable, "reference")
		upgradedOutput = parquetView(ctx, t, spark, upgradedDB, upgradedTable, "upgraded")
		if referenceOutput == "" || upgradedOutput == "" {
			if (referenceOutput == "") != (upgradedOutput == "") {
				diag.fatalf(t, "only one run produced parquet files for %s (reference %q, upgrade %q): the binaries disagree about whether this case writes output", v.name, referenceDB, upgradedDB)
			}
			if !v.emptyFinalState {
				diag.fatalf(t, "neither run left parquet files for %s (reference %q, upgrade %q), but its last case writes rows: both binaries produced nothing where output is expected", v.name, referenceDB, upgradedDB)
			}
			t.Logf("verified: neither run leaves parquet files for %s -- its last case is a delete-only batch, which writes none", v.name)
			return
		}
	default:
		t.Fatalf("unknown destination %q", group.destination)
	}

	compareDestinationOutputs(ctx, t, diag, spark, referenceOutput, upgradedOutput, policies.typeOnly)
}

// icebergTable refreshes and returns the fully-qualified name of an Iceberg table: the shared
// Spark session caches snapshots, so a table written after it was built reads as empty without it.
func icebergTable(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table string) string {
	name := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, db, table)
	_, err := spark.Sql(ctx, "REFRESH TABLE "+name)
	require.NoErrorf(t, err, "failed to refresh %s -- the run may not have produced it", name)
	return name
}

// parquetView stands a temp view over one side's parquet output; "" means the side wrote no
// files, which the caller treats as a comparable state (see the emptyFinalState assertion).
// Do NOT SET spark.sql.parquet.mergeSchema on this session: it breaks every later direct file query
// (UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY), VerifyParquetSync's included.
func parquetView(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table, side string) string {
	view := fmt.Sprintf("`compatibility_%s_%s`", side, table)
	path := fmt.Sprintf("s3a://%s/%s/%s", testutils.ParquetTestBucket, db, table)
	_, err := spark.Sql(ctx, fmt.Sprintf("CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM parquet.`%s/*.parquet`", view, path))
	if err != nil {
		require.Containsf(t, err.Error(), "PATH_NOT_FOUND", "failed to read parquet at %s", path)
		return ""
	}
	t.Cleanup(func() { _, _ = spark.Sql(ctx, "DROP VIEW IF EXISTS "+view) })
	return view
}

// compareDestinationOutputs is the assertion. Order matters: a schema mismatch has to be reported before a
// row query that would fail confusingly because of it.
func compareDestinationOutputs(ctx context.Context, t *testing.T, diag *diagnostics, spark sql.SparkSession, referenceOutput, upgradedOutput string, typeOnly []string) {
	// 1. Non-vacuity FIRST. Two empty tables satisfy every diff below, and an empty reference is a
	//    plausible outcome, not a far-fetched one: a stream the baseline binary could not validate
	//    is skipped with a Warn and the sync still exits 0 (protocol/sync.go, D3 in the doc). Without
	//    this guard that scenario reports a green.
	referenceCount := scalarCount(ctx, t, spark, "SELECT COUNT(*) AS n FROM "+referenceOutput)
	if referenceCount == 0 {
		diag.fatalf(t, "the reference run produced no rows in %s; it is the source of truth, so an empty one makes the whole comparison vacuous (a silently skipped stream looks exactly like this)", referenceOutput)
	}
	upgradedCount := scalarCount(ctx, t, spark, "SELECT COUNT(*) AS n FROM "+upgradedOutput)
	if referenceCount != upgradedCount {
		diag.fatalf(t, "row count differs: reference %s has %d, upgrade %s has %d", referenceOutput, referenceCount, upgradedOutput, upgradedCount)
	}

	// 2. Schema. Compared as a map, so a column order difference (schema evolution appends in
	//    record-arrival order) is not a failure while an added, dropped or retyped column is. This
	//    is the assertion that catches a type-mapping change -- I6 in the doc.
	referenceSchema := describeOutput(ctx, t, spark, referenceOutput)
	upgradedSchema := describeOutput(ctx, t, spark, upgradedOutput)
	if !maps.Equal(referenceSchema, upgradedSchema) {
		diag.fatalf(t, "destination schema differs between the reference and upgrade runs.\n%s\n  full reference schema (%s): %v\n  full upgrade schema   (%s): %v",
			indent(require.MapDiff("column", "reference run", "post olake upgrade", referenceSchema, upgradedSchema), "  "), referenceOutput, referenceSchema, upgradedOutput, upgradedSchema)
	}

	// 3. Per-op-type counts, so a row diff reads as "5 'u' rows where the reference had 6" rather
	//    than an opaque set difference.
	referenceOps, upgradedOps := opTypeCounts(ctx, t, spark, referenceOutput), opTypeCounts(ctx, t, spark, upgradedOutput)
	if !maps.Equal(referenceOps, upgradedOps) {
		diag.fatalf(t, "per-_op_type row counts differ between the reference and upgrade runs.\n%s\n  reference: %v\n  upgrade:   %v",
			indent(require.MapDiff("op type", "reference run", "post olake upgrade", referenceOps, upgradedOps), "  "), referenceOps, upgradedOps)
	}

	// 4. Values, both directions. This is the assertion that catches a changed record: every
	//    column not in typeOnly must hold the same value on both sides.
	valueColumns := comparableColumns(referenceSchema, typeOnly)
	require.NotEmpty(t, valueColumns, "every column is type-only; there is nothing left to compare by value")
	t.Logf("comparing values of %d rows over %d columns (%d more compared by type only)", referenceCount, len(valueColumns), len(typeOnly))

	onlyInReference := rowsOnlyIn(ctx, t, spark, referenceOutput, upgradedOutput, valueColumns)
	onlyInUpgraded := rowsOnlyIn(ctx, t, spark, upgradedOutput, referenceOutput, valueColumns)
	if len(onlyInReference) == 0 && len(onlyInUpgraded) == 0 {
		t.Logf("values identical: all %d rows match on all %d compared columns", referenceCount, len(valueColumns))
		return
	}

	// Name the columns that actually differ before dumping rows -- with 30-odd columns, a row dump
	// alone leaves you diffing two long tuples by eye.
	reportColumnDiffs(ctx, t, diag, spark, referenceOutput, upgradedOutput, valueColumns)
	logSampleRows(t, diag, "only in the reference run", referenceOutput, onlyInReference)
	logSampleRows(t, diag, "only in the upgrade run", upgradedOutput, onlyInUpgraded)
	diag.fatalf(t, "row values differ between the reference and upgrade runs: %d row(s) only in %s, %d row(s) only in %s",
		len(onlyInReference), referenceOutput, len(onlyInUpgraded), upgradedOutput)
}

// reportColumnDiffs names the columns whose values differ, with a sample from each side. Runs one
// query per column, so it is called only after a diff has already been found.
func reportColumnDiffs(ctx context.Context, t *testing.T, diag *diagnostics, spark sql.SparkSession, referenceOutput, upgradedOutput string, valueColumns []string) {
	for _, col := range valueColumns {
		n := scalarCount(ctx, t, spark, fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT %s FROM %s EXCEPT ALL SELECT %s FROM %s)", col, referenceOutput, col, upgradedOutput))
		if n == 0 {
			continue
		}
		diag.logf(t, "column %s differs in %d row(s)\n  reference: %v\n  upgrade:   %v",
			col, n, sampleColumn(ctx, spark, referenceOutput, col), sampleColumn(ctx, spark, upgradedOutput, col))
	}
}

// sampleColumn returns up to three values of one column, for a failure message.
func sampleColumn(ctx context.Context, spark sql.SparkSession, output, col string) []any {
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT %s AS v FROM %s LIMIT 3", col, output))
	if err != nil {
		return nil
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return nil
	}
	values := make([]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, row.Value("v"))
	}
	return values
}

// logSampleRows logs up to five of the rows one output holds and the other does not, each prefixed
// with label ("only in the reference run").
func logSampleRows(t *testing.T, diag *diagnostics, label, output string, rows []types.Row) {
	for i, row := range rows {
		if i == 5 {
			diag.logf(t, "... and %d more %s", len(rows)-5, label)
			break
		}
		diag.logf(t, "%s (%s): %v", label, output, row)
	}
}

// comparableColumns is the sorted, back-quoted projection compared by value.
func comparableColumns(schema map[string]string, typeOnly []string) []string {
	var valueColumns []string
	for col := range schema {
		if !slices.Contains(typeOnly, col) {
			valueColumns = append(valueColumns, "`"+col+"`")
		}
	}
	slices.Sort(valueColumns)
	return valueColumns
}

// rowsOnlyIn returns the rows of `left` that `right` does not hold, comparing every column in
// valueColumns by value.
//
// EXCEPT ALL, not EXCEPT: the plain form is DISTINCT-based and would hide a duplicate-row
// regression (five identical rows reading as equal to six). The EXCEPT family is also NULL-safe,
// which a join-based diff would not be, and these tables are full of nullable columns.
func rowsOnlyIn(ctx context.Context, t *testing.T, spark sql.SparkSession, left, right string, valueColumns []string) []types.Row {
	projection := strings.Join(valueColumns, ", ")
	query := fmt.Sprintf("SELECT %s FROM %s EXCEPT ALL SELECT %s FROM %s", projection, left, projection, right)
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to diff %s against %s", left, right)
	rows, err := df.Collect(ctx)
	require.NoError(t, err, "failed to collect the row diff")
	return rows
}

func scalarCount(ctx context.Context, t *testing.T, spark sql.SparkSession, query string) int64 {
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to run %q", query)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect %q", query)
	require.NotEmpty(t, rows, "no result for %q", query)
	n, ok := rows[0].Value("n").(int64)
	require.Truef(t, ok, "count is not int64: %T", rows[0].Value("n"))
	return n
}

func describeOutput(ctx context.Context, t *testing.T, spark sql.SparkSession, output string) map[string]string {
	df, err := spark.Sql(ctx, "DESCRIBE TABLE "+output)
	require.NoErrorf(t, err, "failed to describe %s", output)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect the description of %s", output)

	schema := make(map[string]string, len(rows))
	for _, row := range rows {
		col, ok := row.Value("col_name").(string)
		require.Truef(t, ok, "DESCRIBE %s: col_name is not a string: %T", output, row.Value("col_name"))
		dataType, ok := row.Value("data_type").(string)
		require.Truef(t, ok, "DESCRIBE %s: data_type is not a string: %T", output, row.Value("data_type"))
		// DESCRIBE appends partition/metadata sections, all introduced by a "#" heading.
		if col != "" && !strings.HasPrefix(col, "#") {
			schema[col] = dataType
		}
	}
	return schema
}

func opTypeCounts(ctx context.Context, t *testing.T, spark sql.SparkSession, output string) map[string]int64 {
	query := fmt.Sprintf("SELECT `_op_type` AS op, COUNT(*) AS n FROM %s GROUP BY `_op_type`", output)
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to count op types in %s", output)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect op type counts for %s", output)

	counts := make(map[string]int64, len(rows))
	for _, row := range rows {
		op, ok := row.Value("op").(string)
		require.Truef(t, ok, "op type in %s is not a string: %T", output, row.Value("op"))
		n, ok := row.Value("n").(int64)
		require.Truef(t, ok, "op type count in %s is not int64: %T", output, row.Value("n"))
		counts[op] = n
	}
	return counts
}
