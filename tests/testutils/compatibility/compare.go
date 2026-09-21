package compatibility

// Comparing the two runs.
//
// The suite's only assertion: the upgrade run's destination must be indistinguishable from the
// reference run's. Everything here reads the two destinations through the shared Spark session --
// row counts, the destination schema, per-_op_type counts, then every row of every non-volatile
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
func compareVariant(t *testing.T, diag *diagnostics, policies *assertionPolicies, ref, upg *testutils.TestConfig, group compatibilityGroup, v compatibilityVariant) {
	ctx := t.Context()
	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "failed to connect to Spark Connect server")

	refDB, upgDB := ref.DestinationDB, upg.DestinationDB
	refTable, upgTable := ref.GetTableName(), upg.GetTableName()
	var refRel, upgRel string
	switch group.destination {
	case "iceberg":
		refRel = icebergRelation(ctx, t, spark, refDB, refTable)
		upgRel = icebergRelation(ctx, t, spark, upgDB, upgTable)
	case "parquet":
		refRel = parquetRelation(ctx, t, spark, refDB, refTable, "ref")
		upgRel = parquetRelation(ctx, t, spark, upgDB, upgTable, "upg")
		if refRel == "" || upgRel == "" {
			if (refRel == "") != (upgRel == "") {
				diag.fatalf(t, "only one run produced parquet files for %s (reference %q, upgrade %q): the binaries disagree about whether this case writes output", v.name, refDB, upgDB)
			}
			if !v.emptyFinalState {
				diag.fatalf(t, "neither run left parquet files for %s (reference %q, upgrade %q), but its last case writes rows: both binaries produced nothing where output is expected", v.name, refDB, upgDB)
			}
			t.Logf("verified: neither run leaves parquet files for %s -- its last case is a delete-only batch, which writes none", v.name)
			return
		}
	default:
		t.Fatalf("unknown destination %q", group.destination)
	}

	compareRelations(ctx, t, diag, spark, refRel, upgRel, policies.typeOnly)
}

// icebergRelation refreshes and returns the fully-qualified name of an Iceberg table: the shared
// Spark session caches snapshots, so a table written after it was built reads as empty without it.
func icebergRelation(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table string) string {
	name := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, db, table)
	_, err := spark.Sql(ctx, "REFRESH TABLE "+name)
	require.NoErrorf(t, err, "failed to refresh %s -- the run may not have produced it", name)
	return name
}

// parquetRelation stands a temp view over one side's parquet output; "" means the side wrote no
// files, which the caller treats as a comparable state (see the emptyFinalState assertion).
// Do NOT SET spark.sql.parquet.mergeSchema on this session: it breaks every later direct file query
// (UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY), VerifyParquetSync's included.
func parquetRelation(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table, side string) string {
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

// compareRelations is the assertion. Order matters: a schema mismatch has to be reported before a
// row query that would fail confusingly because of it.
func compareRelations(ctx context.Context, t *testing.T, diag *diagnostics, spark sql.SparkSession, refRel, upgRel string, volatile []string) {
	// 1. Non-vacuity FIRST. Two empty tables satisfy every diff below, and an empty reference is a
	//    plausible outcome, not a far-fetched one: a stream the baseline binary could not validate
	//    is skipped with a Warn and the sync still exits 0 (protocol/sync.go, D3 in the doc). Without
	//    this guard that scenario reports a green.
	refCount := scalarCount(ctx, t, spark, "SELECT COUNT(*) AS n FROM "+refRel)
	if refCount == 0 {
		diag.fatalf(t, "the reference run produced no rows in %s; it is the source of truth, so an empty one makes the whole comparison vacuous (a silently skipped stream looks exactly like this)", refRel)
	}
	upgCount := scalarCount(ctx, t, spark, "SELECT COUNT(*) AS n FROM "+upgRel)
	if refCount != upgCount {
		diag.fatalf(t, "row count differs: reference %s has %d, upgrade %s has %d", refRel, refCount, upgRel, upgCount)
	}

	// 2. Schema. Compared as a map, so a column order difference (schema evolution appends in
	//    record-arrival order) is not a failure while an added, dropped or retyped column is. This
	//    is the assertion that catches a type-mapping change -- I6 in the doc.
	refSchema := describeRelation(ctx, t, spark, refRel)
	upgSchema := describeRelation(ctx, t, spark, upgRel)
	if !maps.Equal(refSchema, upgSchema) {
		diag.fatalf(t, "destination schema differs between the reference and upgrade runs.\n%s\n  full reference schema (%s): %v\n  full upgrade schema   (%s): %v",
			indent(require.MapDiff("column", "reference run", "post olake upgrade", refSchema, upgSchema), "  "), refRel, refSchema, upgRel, upgSchema)
	}

	// 3. Per-op-type counts, so a row diff reads as "5 'u' rows where the reference had 6" rather
	//    than an opaque set difference.
	refOps, upgOps := opTypeCounts(ctx, t, spark, refRel), opTypeCounts(ctx, t, spark, upgRel)
	if !maps.Equal(refOps, upgOps) {
		diag.fatalf(t, "per-_op_type row counts differ between the reference and upgrade runs.\n%s\n  reference: %v\n  upgrade:   %v",
			indent(require.MapDiff("op type", "reference run", "post olake upgrade", refOps, upgOps), "  "), refOps, upgOps)
	}

	// 4. Values, both directions. This is the assertion that catches a changed record: every
	//    non-volatile column of every row must hold the same value on both sides.
	cols := comparableColumns(refSchema, volatile)
	require.NotEmpty(t, cols, "every column is volatile; there is nothing left to compare by value")
	t.Logf("comparing values of %d rows over %d columns (%d volatile, type-checked only)", refCount, len(cols), len(volatile))

	onlyInRef := rowsOnlyIn(ctx, t, spark, refRel, upgRel, cols)
	onlyInUpg := rowsOnlyIn(ctx, t, spark, upgRel, refRel, cols)
	if len(onlyInRef) == 0 && len(onlyInUpg) == 0 {
		t.Logf("values identical: all %d rows match on all %d compared columns", refCount, len(cols))
		return
	}

	// Name the columns that actually differ before dumping rows -- with 30-odd columns, a row dump
	// alone leaves you diffing two long tuples by eye.
	reportColumnDiffs(ctx, t, diag, spark, refRel, upgRel, cols)
	logSampleRows(t, diag, "only in the reference run", refRel, onlyInRef)
	logSampleRows(t, diag, "only in the upgrade run", upgRel, onlyInUpg)
	diag.fatalf(t, "row values differ between the reference and upgrade runs: %d row(s) only in %s, %d row(s) only in %s",
		len(onlyInRef), refRel, len(onlyInUpg), upgRel)
}

// reportColumnDiffs names the columns whose values differ, with a sample from each side. Runs one
// query per column, so it is called only after a diff has already been found.
func reportColumnDiffs(ctx context.Context, t *testing.T, diag *diagnostics, spark sql.SparkSession, refRel, upgRel string, cols []string) {
	for _, col := range cols {
		n := scalarCount(ctx, t, spark, fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT %s FROM %s EXCEPT ALL SELECT %s FROM %s)", col, refRel, col, upgRel))
		if n == 0 {
			continue
		}
		diag.logf(t, "column %s differs in %d row(s)\n  reference: %v\n  upgrade:   %v",
			col, n, sampleColumn(ctx, spark, refRel, col), sampleColumn(ctx, spark, upgRel, col))
	}
}

// sampleColumn returns up to three values of one column, for a failure message.
func sampleColumn(ctx context.Context, spark sql.SparkSession, relation, col string) []any {
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT %s AS v FROM %s LIMIT 3", col, relation))
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

func logSampleRows(t *testing.T, diag *diagnostics, what, relation string, rows []types.Row) {
	for i, row := range rows {
		if i == 5 {
			diag.logf(t, "... and %d more %s", len(rows)-5, what)
			break
		}
		diag.logf(t, "%s (%s): %v", what, relation, row)
	}
}

// comparableColumns is the sorted, back-quoted projection compared by value.
func comparableColumns(schema map[string]string, volatile []string) []string {
	var cols []string
	for col := range schema {
		if !slices.Contains(volatile, col) {
			cols = append(cols, "`"+col+"`")
		}
	}
	slices.Sort(cols)
	return cols
}

// rowsOnlyIn returns the rows of `left` that `right` does not hold, comparing every column in
// cols by value.
//
// EXCEPT ALL, not EXCEPT: the plain form is DISTINCT-based and would hide a duplicate-row
// regression (five identical rows reading as equal to six). The EXCEPT family is also NULL-safe,
// which a join-based diff would not be, and these tables are full of nullable columns.
func rowsOnlyIn(ctx context.Context, t *testing.T, spark sql.SparkSession, left, right string, cols []string) []types.Row {
	projection := strings.Join(cols, ", ")
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

func describeRelation(ctx context.Context, t *testing.T, spark sql.SparkSession, relation string) map[string]string {
	df, err := spark.Sql(ctx, "DESCRIBE TABLE "+relation)
	require.NoErrorf(t, err, "failed to describe %s", relation)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect the description of %s", relation)

	schema := make(map[string]string, len(rows))
	for _, row := range rows {
		col, ok := row.Value("col_name").(string)
		require.Truef(t, ok, "DESCRIBE %s: col_name is not a string: %T", relation, row.Value("col_name"))
		dataType, ok := row.Value("data_type").(string)
		require.Truef(t, ok, "DESCRIBE %s: data_type is not a string: %T", relation, row.Value("data_type"))
		// DESCRIBE appends partition/metadata sections, all introduced by a "#" heading.
		if col != "" && !strings.HasPrefix(col, "#") {
			schema[col] = dataType
		}
	}
	return schema
}

func opTypeCounts(ctx context.Context, t *testing.T, spark sql.SparkSession, relation string) map[string]int64 {
	query := fmt.Sprintf("SELECT `_op_type` AS op, COUNT(*) AS n FROM %s GROUP BY `_op_type`", relation)
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to count op types in %s", relation)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect op type counts for %s", relation)

	counts := make(map[string]int64, len(rows))
	for _, row := range rows {
		op, ok := row.Value("op").(string)
		require.Truef(t, ok, "op type in %s is not a string: %T", relation, row.Value("op"))
		n, ok := row.Value("n").(int64)
		require.Truef(t, ok, "op type count in %s is not int64: %T", relation, row.Value("n"))
		counts[op] = n
	}
	return counts
}
