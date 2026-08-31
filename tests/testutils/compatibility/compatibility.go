package compatibility

// Backward-compatibility suite.
//
// Upgrading the OLake binary must not change the records or the column types an existing pipeline produces.
// The state file's `version` pins that, so a candidate binary reading a state file an older binary wrote must keep the older
// binary's semantics.
//
// Rather than encode per-version expectations -- which rot, and which nobody remembers to add when
// the latest state version is bumped -- this suite runs the same scenario twice, concurrently:
//
//	reference : every sync on the BASELINE image
//	upgrade   : the stateless initial load on the BASELINE image, every --state sync after it on
//	            the CANDIDATE image
//
// and then asserts the two destinations are indistinguishable. The reference run IS the
// expectation. A gate that stopped firing, a type map that shifted, a state key that got renamed:
// each shows up as a diff between two tables, with no expectation file to maintain.

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const (
	// compatibilityBaselineEnvVar names the baseline to test the local build against, replacing the
	// manifest sweep with a single run. Three forms are accepted, see resolveBaselineImage;
	// per-driver overrides use the suffixed form, OLAKE_COMPATIBILITY_BASELINE_POSTGRES.
	compatibilityBaselineEnvVar = "OLAKE_COMPATIBILITY_TEST_BASELINE"

	// compatibilityExcludeColumnsEnvVar appends catalog-level column exclusions to every compatibility run
	compatibilityExcludeColumnsEnvVar = "OLAKE_COMPATIBILITY_EXCLUDE_COLUMNS"
)

type Test struct {
	NewConfig        func(t *testing.T, DriverVersion string) *testutils.TestConfig
	DeclaredSchema   map[string]string
	ColumnTypes      map[string][]string
	CDCColumnsSchema map[string]string
}

// Validate checks the fixture wired everything a compatibility run reads.
func (f *Test) Validate(t *testing.T) {
	t.Helper()
	require.NotNil(t, f.NewConfig, "compatibility.Test.NewConfig is not set")
	require.NotEmpty(t, f.DeclaredSchema, "compatibility.Test.DeclaredSchema is not set; type-keyed rules would resolve against nothing")
}

// RunBackwardCompatibility runs one driver's scenarios twice -- a reference run entirely on the baseline
// image and an upgrade run that hands off to the candidate after the initial load -- then asserts
// the two destinations match. Both sides of all three writer groups (iceberg legacy, iceberg
// arrow, parquet) run in parallel, six isolated pipelines at once.
func (f *Test) RunBackwardCompatibility(t *testing.T) {
	currentConf := f.NewConfig(t, testutils.CurrentDriverVersion)
	baselineVersions, err := getCompatibilityBaselines(t, currentConf.OlakeRootPath, currentConf.Driver)
	require.NoError(t, err)

	for _, version := range baselineVersions {
		reason, err := baselineSkipReason(currentConf.OlakeRootPath, currentConf.Driver, version)
		require.NoError(t, err)
		if reason != "" {
			t.Run(version, func(t *testing.T) { t.Skip(reason) })
			continue
		}

		baselineConf := f.NewConfig(t, version)
		if !t.Run(baselineConf.DriverVersion, func(t *testing.T) {
			t.Parallel()
			f.runCompatibilityBaseline(t, baselineConf, currentConf)
		}) {
			t.Logf("compatibility: stopping the sweep at %s; the later baselines carry newer code and would repeat it", version)
			return
		}
	}
}

// baselineSkipReason is why this driver does not run against this baseline at all, or "" when it
// does: the global floor from state-versions.json, then the driver's own gate in
// compatibility_rules.json. Both are answerable from the driver name alone, which is what lets the
// caller skip a baseline before paying for its image.
func baselineSkipReason(rootPath, driver, spec string) (string, error) {
	version, dated := parseReleaseTag(spec)
	floorTag, err := compatibilityGlobalFloor(rootPath)
	if err != nil {
		return "", err
	}
	if globalFloor, _ := parseReleaseTag(floorTag); dated && compareRelease(version, globalFloor) < 0 {
		return fmt.Sprintf("baseline %s predates %s, the oldest state-version baseline; the compatibility suite does not run below it",
			spec, floorTag), nil
	}
	gate := compatibilityRules.Drivers[driver].compatibilityGate
	if reason := gate.skipReason(version, dated); reason != "" {
		return fmt.Sprintf("%s cannot run baseline %s: %s (compatibility_rules.json: %s)", driver, spec, reason, gate.Note), nil
	}

	return "", nil
}

// runCompatibilityBaseline runs every writer group's variants against one baseline: the reference
// side on baseline's image throughout, the upgrade side handing its stateful syncs to upgrade's.
func (f *Test) runCompatibilityBaseline(t *testing.T, baseline, upgrade *testutils.TestConfig) {
	spec := baseline.DriverVersion
	driver, dataFormat := baseline.Driver, baseline.DataFormat

	baselineVersion, baselineDated := parseReleaseTag(spec)
	floorTag, err := compatibilityGlobalFloor(baseline.OlakeRootPath)
	require.NoError(t, err)
	globalFloor, _ := parseReleaseTag(floorTag)
	driverRules := compatibilityRules.Drivers[driver]
	variantRules := driverRules.Variants[dataFormat]
	if reason := variantRules.compatibilityGate.skipReason(baselineVersion, baselineDated); reason != "" {
		t.Skipf("%s/%s cannot run baseline %s: %s (compatibility_rules.json: %s)",
			driver, dataFormat, spec, reason, variantRules.compatibilityGate.Note)
	}

	// Both images were pulled or built when the caller constructed the two configs, serially,
	// before any parallel child starts; the sides below only re-derive the same refs.
	baselineImage, candidateImage := baseline.GetDriverImage(), upgrade.GetDriverImage()
	require.NotEqualf(t, baselineImage, candidateImage,
		"the compatibility baseline and the candidate resolve to the same image (%s); the run would compare it with itself and pass", baselineImage)
	t.Logf("compatibility: baseline %s -> candidate %s", baselineImage, candidateImage)

	// Column policies: the baseline's era decides what each column can be asserted on. Applied to
	// both sides, so a diff is always the binary and never the fixture.
	if declared := driverRules.Variants; len(declared) > 0 {
		formats := slices.Sorted(maps.Keys(declared))
		if !slices.Contains(formats, dataFormat) {
			t.Logf("NOTE: %s runs data format %q, which compatibility_rules.json does not declare (declared: %v); no variant rule or gate applies to this run.",
				driver, dataFormat, formats)
		} else {
			t.Logf("compatibility: %s declares data formats %v; this run is %q", driver, formats, dataFormat)
		}
	}

	// Every rule -- type-keyed, column-keyed, dated, unconditional -- resolves here into the one
	// policy set the run applies: seeding, catalog and comparison all read it, nothing re-derives.
	policies, err := resolveAssertionPolicies(f, spec, floorTag, globalFloor, driverRules, variantRules)
	require.NoError(t, err)
	for _, note := range policies.notes {
		t.Logf("compatibility: %s", note)
	}

	// Writer-level gates: a group whose writer has a known bounded regression against this
	// baseline is left out, and says so -- the other writers keep their coverage instead of the
	// whole baseline being dropped.
	var groups []compatibilityGroup
	for _, g := range compatibilityVariantGroups(driver) {
		if reason := g.gate.skipReason(baselineVersion, baselineDated); reason != "" {
			t.Logf("compatibility: writer group %s not run against this baseline: %s", g.name, reason)
			continue
		}
		groups = append(groups, g)
	}
	require.NotEmpty(t, groups, "no compatibility scenarios for driver %s against this baseline", driver)

	// Each variant runs as its own pair of parallel subtests -- reference entirely on the
	// baseline, upgrade handing off to the candidate after the stateless load -- and is compared
	// as soon as both sides finish
	referencePick := func(bool) string { return baseline.DriverVersion }
	upgradePick := func(useState bool) string {
		return testutils.Ternary(useState, upgrade.DriverVersion, baseline.DriverVersion).(string)
	}
	// Whichever side fails first stops every group at its next variant boundary: the comparison
	// is skipped either way, so the remaining syncs would be minutes of output nothing reads.

	aborted := &atomic.Bool{}

	// What every failed variant found, so the assertion at the end of this function -- the one CI
	// shows in red -- can report the findings themselves rather than the fact that there were some.
	report := &failureReport{driver: driver, spec: spec, baseline: baselineImage, candidate: candidateImage}

	completed := t.Run("g", func(t *testing.T) {
		for _, g := range groups {
			runGroup := func(t *testing.T) {
				for _, v := range g.variants {
					if aborted.Load() {
						t.Logf("compatibility group %s: skipping variant %q onwards; another run already failed", g.name, v.name)
						break
					}
					diag := &diagnostics{}
					ok := t.Run(v.name, func(t *testing.T) {
						// Both sides start on the baseline version; pick moves the upgrade side's
						// stateful syncs to the candidate.
						var ref, upg *testutils.TestConfig
						if !t.Run("run", func(t *testing.T) {
							t.Run("ref", func(t *testing.T) {
								t.Parallel()
								ref = f.NewConfig(t, baseline.DriverVersion)
								runSide(t, ref, g, v, referencePick, policies)
							})
							t.Run("upg", func(t *testing.T) {
								t.Parallel()
								upg = f.NewConfig(t, baseline.DriverVersion)
								runSide(t, upg, g, v, upgradePick, policies)
							})
						}) {
							diag.fatalf(t, "a %s side never finished, so the two destinations were never compared; the side's own subtest output has why", v.name)
						}
						compareVariant(t, diag, policies, ref, upg, g, v)
					})
					if !ok {
						report.add(g.name, v.name, diag)
						aborted.Store(true)
						t.Logf("compatibility group %s: stopping after variant %q", g.name, v.name)
						break
					}
				}
			}
			t.Run(g.name, func(t *testing.T) { t.Parallel(); runGroup(t) })
		}
	})
	require.Truef(t, completed, "%s", report.render(baseline.OlakeRootPath))
}

// getCompatibilityBaselines returns the baselines to run driver against: the
// OLAKE_COMPATIBILITY_TEST_BASELINE override alone, else every release in `state-versions.json`
// whose bump gated this driver, oldest first -- a bump that touched only other drivers changed
// nothing this driver's state file pins, so it is logged and left out.
func getCompatibilityBaselines(t *testing.T, rootPath, driver string) ([]string, error) {
	t.Helper()
	if spec := os.Getenv(compatibilityBaselineEnvVar); spec != "" {
		return []string{spec}, nil
	}
	baselines, err := testutils.StateVersionBaselines(rootPath)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(baselines, func(a, b testutils.StateVersionBaseline) int { return a.StateVersion - b.StateVersion })
	specs := make([]string, 0, len(baselines))
	for _, baseline := range baselines {
		if !baseline.Gates(driver) {
			t.Logf("compatibility: baseline %s not run for %s; state version %d gates only %s", baseline.ReleaseTag, driver, baseline.StateVersion, baseline.Drivers)
			continue
		}
		// One release can cover several state versions (a release that jumps the manifest by
		// more than one carries every version it skipped), and running it twice proves nothing.
		if !slices.Contains(specs, baseline.ReleaseTag) {
			specs = append(specs, baseline.ReleaseTag)
		}
	}
	return specs, nil
}

// compatibilityGroupSpecs is the writer-group fan-out: one group per destination writer, each
// naming the destination config its syncs run against and where its gates live in
// compatibility_rules.json (mode is empty for a destination that has none).
func compatibilityGroupSpecs() []compatibilityGroupSpec {
	return []compatibilityGroupSpec{
		{name: "legacy", destination: "iceberg", mode: "legacy", destinationFile: "iceberg_destination.json"},
		{name: "arrow", destination: "iceberg", mode: "arrow", destinationFile: "iceberg_destination_arrow.json"},
		{name: "pq", destination: "parquet", destinationFile: "parquet_destination.json"},
	}
}

// gateFrom picks this group's gate out of a destinations block: the destination's own gate, and
// its mode's gate when the group names one.
func (s compatibilityGroupSpec) gateFrom(destinations map[string]compatibilityDestination) compatibilityGate {
	dest := destinations[s.destination]
	if s.mode == "" {
		return dest.compatibilityGate
	}
	return mergedGate(dest.compatibilityGate, dest.Modes[s.mode])
}

func compatibilityVariantGroups(driver string) []compatibilityGroup {
	// Same fan-out as TestSync, and the same two skips.
	cdc := !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(driver))
	inc := driver != string(constants.Kafka)

	driverDestinations := compatibilityRules.Drivers[driver].Destinations
	var groups []compatibilityGroup
	for _, spec := range compatibilityGroupSpecs() {
		var variants []compatibilityVariant
		if cdc {
			// The parquet CDC scenario ends on a delete-only batch, and parquet holds only its
			// last case's files -- so both sides ending with none is its verified outcome.
			variants = append(variants, compatibilityVariant{name: "cdc", kind: scenarioCDC, emptyFinalState: spec.destination == "parquet"})
		}
		if inc {
			variants = append(variants, compatibilityVariant{name: "inc", kind: scenarioIncremental})
		}
		if len(variants) == 0 {
			continue
		}
		gate := mergedGate(spec.gateFrom(compatibilityRules.Destinations.gates()), spec.gateFrom(driverDestinations))
		groups = append(groups, compatibilityGroup{compatibilityGroupSpec: spec, gate: gate, variants: variants})
	}
	return groups
}

// compareVariant asserts the upgrade run's destination for one scenario is indistinguishable from
// the reference run's.
func compareVariant(t *testing.T, diag *diagnostics, policies *assertionPolicies, ref, upg *testutils.TestConfig, g compatibilityGroup, v compatibilityVariant) {
	ctx := t.Context()
	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "failed to connect to Spark Connect server")

	refDB, upgDB := ref.DestinationDB, upg.DestinationDB
	refTable, upgTable := ref.GetTableName(), upg.GetTableName()
	var refRel, upgRel string
	switch g.destination {
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
		t.Fatalf("unknown destination %q", g.destination)
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
	path := fmt.Sprintf("s3a://%s/%s/%s", testutils.ParquetBucket, db, table)
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
		diag.fatalf(t, "destination schema differs between the reference and upgrade runs.\n  reference (%s): %v\n  upgrade   (%s): %v", refRel, refSchema, upgRel, upgSchema)
	}

	// 3. Per-op-type counts, so a row diff reads as "5 'u' rows where the reference had 6" rather
	//    than an opaque set difference.
	refOps, upgOps := opTypeCounts(ctx, t, spark, refRel), opTypeCounts(ctx, t, spark, upgRel)
	if !maps.Equal(refOps, upgOps) {
		diag.fatalf(t, "per-_op_type row counts differ between the reference and upgrade runs: reference %v, upgrade %v", refOps, upgOps)
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
		col, _ := row.Value("col_name").(string)
		dataType, _ := row.Value("data_type").(string)
		// DESCRIBE appends partition/metadata sections, all introduced by a "#" heading.
		if col != "" && !strings.HasPrefix(col, "#") {
			schema[col] = dataType
		}
	}
	return schema
}

func opTypeCounts(ctx context.Context, t *testing.T, spark sql.SparkSession, relation string) map[string]int64 {
	query := fmt.Sprintf("SELECT `_op_type` AS op, COUNT(*) AS n FROM %s GROUP BY 1", relation)
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to count op types in %s", relation)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect op type counts for %s", relation)

	counts := make(map[string]int64, len(rows))
	for _, row := range rows {
		op, _ := row.Value("op").(string)
		n, _ := row.Value("n").(int64)
		counts[op] = n
	}
	return counts
}

// compatibilityGlobalFloor is the oldest baseline the suite runs for any driver: the oldest entry in the
// product's state-versions.json. Derived rather than restated, so adding or retiring a baseline
// moves the floor with it.
func compatibilityGlobalFloor(rootPath string) (string, error) {
	baselines, err := testutils.StateVersionBaselines(rootPath)
	if err != nil {
		return "", err
	}
	oldest := baselines[0]
	for _, baseline := range baselines[1:] {
		if baseline.StateVersion < oldest.StateVersion {
			oldest = baseline
		}
	}
	return oldest.ReleaseTag, nil
}
