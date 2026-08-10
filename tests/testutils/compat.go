package testutils

// Backward-compatibility suite.
//
// The contract being tested is docs/backward-compatibility.md: upgrading the OLake binary must not
// change the records or the column types an existing pipeline produces. The state file's `version`
// pins that, so a candidate binary reading a state file an older binary wrote must keep the older
// binary's semantics.
//
// Rather than encode per-version expectations -- which rot, and which nobody remembers to add when
// LatestStateVersion is bumped -- this suite runs the same scenario twice, concurrently:
//
//	reference : every sync on the BASELINE image
//	upgrade   : the stateless initial load on the BASELINE image, every --state sync after it on
//	            the CANDIDATE image
//
// and then asserts the two destinations are indistinguishable. The reference run IS the
// expectation. A gate that stopped firing, a type map that shifted, a state key that got renamed:
// each shows up as a diff between two tables, with no expectation file to maintain.
//
// What this does NOT cover, deliberately: discover output (both runs are seeded from the same
// frozen test_streams.json, so they differ only in the binary -- and discover is ungated by design,
// see A4 in the doc); the reverse direction (a new state file fed to an old image is not a
// supported operation); and any gate older than the baseline being tested.

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/stretchr/testify/require"
)

const (
	// compatBaselineEnvVar names the baseline to test the local build against. Three forms are
	// accepted, see resolveBaselineImage. Per-driver overrides use the suffixed form,
	// OLAKE_COMPAT_BASELINE_POSTGRES.
	compatBaselineEnvVar = "OLAKE_COMPAT_BASELINE"

	// defaultCompatBaseline is the newest published release of the driver. It is the right default
	// for a recurring signal -- "master has not broken anything since the last release" -- but note
	// that it usually sits on the SAME state version as HEAD, so it pins destination continuity (I6)
	// and no version gate at all. Point compatBaselineEnvVar at an older tag (e.g. v0.6.5, the newest
	// release still on state version 5) for gate coverage; assertStateVersionUnchanged says which
	// of the two a given run was.
	defaultCompatBaseline = "latest"

	// releasedImageRepo is where release-tool.sh publishes driver images.
	releasedImageRepo = "olakego/source-%s"

	// keepDataEnvVar leaves the SOURCE table in place at the end of a compat run, for inspecting
	// what the two binaries actually read. The destination is kept regardless (PreserveDestination),
	// so this only covers the source side. Note each scenario opens with resetTable (drop/create/
	// add), so what survives is the LAST scenario's data, not every scenario's.
	keepDataEnvVar = "OLAKE_TEST_KEEP_DATA"

	// compatExcludeColumnsEnvVar appends catalog-level column exclusions to every compat run, a
	// sweep affordance for probing a baseline without editing the driver's rules.
	compatExcludeColumnsEnvVar = "OLAKE_COMPAT_EXCLUDE_COLUMNS"

	// compatRequireBaselineEnvVar turns an unavailable baseline from a skip into a failure. CI
	// sets it: in a release gate, a silently skipped baseline reads as green while testing nothing.
	compatRequireBaselineEnvVar = "OLAKE_COMPAT_REQUIRE_BASELINE"

	compatReferenceSuite = "ref"
	compatUpgradeSuite   = "upg"

	// Olake's own columns, mirrored from the root module's constants package rather than imported
	// -- the same reason docker.go mirrors skipDestinationCheckEnvVar: the tests modules depend on
	// lib, not on the root module. (constants.CdcTimestamp does live in lib and is used as such.)
	olakeIDColumn        = "_olake_id"
	olakeTimestampColumn = "_olake_timestamp"
	opTypeColumn         = "_op_type"
)

// commitSpec matches an abbreviated or full git object name.
var commitSpec = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// baselineSpec resolves which baseline a driver runs against: a per-driver env var, then the
// shared one, then the newest release.
func baselineSpec(driver string) string {
	if spec := os.Getenv(compatBaselineEnvVar + "_" + strings.ToUpper(driver)); spec != "" {
		return spec
	}
	if spec := os.Getenv(compatBaselineEnvVar); spec != "" {
		return spec
	}
	return defaultCompatBaseline
}

// resolveBaselineImage turns a baseline spec into an image ref present on the local daemon:
//
//	"latest", "v0.6.5"                 -> olakego/source-<driver>:<spec>, pulled
//	"olakego/source-postgres:v0.6.5"   -> used verbatim (any spec naming a repository), pulled
//	"9f3c1ab", "sha:9f3c1ab"           -> built from a detached worktree at that commit
//
// A baseline that has no image is a skip, never a failure: a tag older than the driver itself
// legitimately predates its first release.
func resolveBaselineImage(t *testing.T, cfg *TestConfig, spec string) string {
	t.Helper()

	sha := strings.TrimPrefix(spec, "sha:")
	if (spec != sha || commitSpec.MatchString(spec)) && gitHasCommit(cfg.HostRootPath, sha) {
		return buildBaselineFromCommit(t, cfg, sha)
	}

	image := spec
	if !strings.Contains(spec, "/") {
		image = fmt.Sprintf(releasedImageRepo+":%s", cfg.Driver, spec)
	}
	if err := ensureImagePresent(t, image, cfg.ImagePlatform); err != nil {
		if envFlagSet(compatRequireBaselineEnvVar) {
			t.Fatalf("compat baseline %s unavailable and %s is set: %s", image, compatRequireBaselineEnvVar, err)
		}
		t.Skipf("compat baseline %s unavailable, skipping: %s", image, err)
	}
	return image
}

// keepTestData reports whether the run should leave its source data behind for inspection.
func keepTestData() bool {
	return envFlagSet(keepDataEnvVar)
}

// envFlagSet reads an on/off env var: anything other than "" / "false" / "0" counts as set.
func envFlagSet(name string) bool {
	switch strings.ToLower(os.Getenv(name)) {
	case "", "false", "0":
		return false
	default:
		return true
	}
}

func gitHasCommit(root, rev string) bool {
	cmd := exec.Command("git", "-C", root, "cat-file", "-e", rev+"^{commit}")
	return cmd.Run() == nil
}

// buildBaselineFromCommit builds a driver image from a detached worktree at sha. This is a
// debugging affordance for bisecting a break, not the supported path -- released tags need no
// worktree, no maven and no old-toolchain build, and they ship the exact artifact users run.
//
// Two things the old tree needs that the released path does not: its OWN Iceberg writer jar (the
// Dockerfile copies the jar out of the build context, and the old Go side speaks the old jar's
// RPC), and a build entry point that exists in that tree -- `make docker.<d>.build IMAGE_TAG=...`
// is recent, so fall back to a plain `docker build`, whose DRIVER_NAME build-arg is far older.
func buildBaselineFromCommit(t *testing.T, cfg *TestConfig, sha string) string {
	t.Helper()

	image := fmt.Sprintf("olake/source-%s:compat-%s", cfg.Driver, sha)
	if exec.Command("docker", "image", "inspect", image).Run() == nil {
		t.Logf("reusing already-built compat baseline %s", image)
		return image
	}

	worktree := filepath.Join(t.TempDir(), "olake-compat-"+sha)
	run := func(what string, name string, args ...string) {
		cmd := exec.Command(name, args...)
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "failed to %s for compat baseline %s: %s\n%s", what, sha, err, out)
	}

	run("create the worktree", "git", "-C", cfg.HostRootPath, "worktree", "add", "--detach", worktree, sha)
	t.Cleanup(func() {
		_ = exec.Command("git", "-C", cfg.HostRootPath, "worktree", "remove", "--force", worktree).Run()
	})

	defer trackPhaseTiming(t, "compat-baseline", image)()
	run("build the iceberg jar", "make", "-C", worktree, "iceberg.jar")

	if hasMakeTarget(worktree, "docker."+cfg.Driver+".build") {
		run("build the image", "make", "-C", worktree,
			"docker."+cfg.Driver+".build", "IMAGE_TAG=compat-"+sha)
		// That target tags olake/source-<driver>:compat-<sha> -- deliberately never :local, which
		// belongs to the candidate. A baseline built under the candidate's tag would replace it and
		// the suite would compare an image with itself, and pass.
		return image
	}
	run("build the image", "docker", "build", "--build-arg", "DRIVER_NAME="+cfg.Driver, "-t", image, worktree)
	return image
}

func hasMakeTarget(dir, target string) bool {
	return exec.Command("make", "-C", dir, "-n", target).Run() == nil
}

// compatRun is one side of the comparison: an IntegrationTest whose files, source table and
// destination namespaces are scoped to its own suite, and whose syncs are routed to images by
// SyncImage.
type compatRun struct {
	*IntegrationTest
	suite string
	// destBaseDB is DestinationDB before a variant suffix is appended; DestinationDB itself is
	// rewritten per variant so all of a run's scenarios survive to be compared.
	destBaseDB string
	table      string
	// aborted is shared by both runs. They execute under t.Parallel(), so without it the healthy
	// side plays out all six variants after the other has already failed -- and the comparison is
	// skipped either way, so every one of those syncs is discarded.
	aborted *atomic.Bool
}

// compatVariant is one scenario, run once per side. Each gets its own destination namespace, so
// the six scenarios TestIntegration fans out to all survive the run and can be compared
// independently instead of overwriting one table.
type compatVariant struct {
	name        string
	destination string // "iceberg" or "parquet"
	run         func(cfg *IntegrationTest, ctx context.Context, t *testing.T, table string) error
}

func compatVariants(driver string) []compatVariant {
	var variants []compatVariant

	// Same fan-out as TestIntegration, and the same two skips.
	if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(driver)) {
		for _, w := range []struct {
			name  string
			arrow bool
		}{{"ice_legacy_cdc", false}, {"ice_arrow_cdc", true}} {
			variants = append(variants, compatVariant{
				name: w.name, destination: "iceberg",
				run: func(cfg *IntegrationTest, ctx context.Context, t *testing.T, table string) error {
					return cfg.testIcebergWriter(ctx, t, table, w.arrow, cfg.testIcebergFullLoadAndCDC)
				},
			})
		}
		variants = append(variants, compatVariant{
			name: "pq_cdc", destination: "parquet",
			run: func(cfg *IntegrationTest, ctx context.Context, t *testing.T, table string) error {
				return cfg.testParquetFullLoadAndCDC(ctx, t, table)
			},
		})
	}

	if driver != string(constants.Kafka) {
		for _, w := range []struct {
			name  string
			arrow bool
		}{{"ice_legacy_inc", false}, {"ice_arrow_inc", true}} {
			variants = append(variants, compatVariant{
				name: w.name, destination: "iceberg",
				run: func(cfg *IntegrationTest, ctx context.Context, t *testing.T, table string) error {
					return cfg.testIcebergWriter(ctx, t, table, w.arrow, cfg.testIcebergFullLoadAndIncremental)
				},
			})
		}
		variants = append(variants, compatVariant{
			name: "pq_inc", destination: "parquet",
			run: func(cfg *IntegrationTest, ctx context.Context, t *testing.T, table string) error {
				return cfg.testParquetFullLoadAndIncremental(ctx, t, table)
			},
		})
	}

	return variants
}

// RunBackwardCompat runs one driver's scenarios twice in parallel -- a reference run entirely on
// the baseline image and an upgrade run that hands off to the candidate after the initial load --
// then asserts the two destinations match.
//
// newConfig MUST return a fresh IntegrationTest carrying its own *TestConfig on every call.
// applySuite mutates TestConfig in place, so two runs sharing one pointer would silently clobber
// each other's paths and write the same files -- which reads as a compat failure.
func RunBackwardCompat(t *testing.T, newConfig func(t *testing.T) *IntegrationTest) {
	ctx := context.Background()
	probe := newConfig(t)

	baseline := resolveBaselineImage(t, probe.TestConfig, baselineSpec(probe.TestConfig.Driver))
	// Build the candidate once, serially, before any parallel child starts: its sync.Once would
	// otherwise fire inside whichever subtest got there first.
	candidate := getOrBuildDriverImage(t, probe.TestConfig)
	require.NotEqualf(t, baseline, candidate,
		"the compat baseline and the candidate resolve to the same image (%s); the run would compare it with itself and pass", baseline)
	t.Logf("compat: baseline %s -> candidate %s", baseline, candidate)

	// Both sides get the input shape the BASELINE shipped with, not today's. A key introduced
	// after the baseline would otherwise read as a behaviour change when all it means is that the
	// older binary never knew the key -- see inputGeneration.
	generation, why, err := resolveInputGeneration(baselineSpec(probe.TestConfig.Driver))
	require.NoError(t, err)
	t.Logf("compat: input generation %q (%s)", generation.name, why)
	if generation != currentInputGeneration() {
		t.Logf("NOTE: streams.json is written in the %q shape, so this run pins that older input against the candidate. Set %s=current to compare on today's shape instead.",
			generation.name, compatInputGenerationEnvVar)
	}

	ref := prepareCompatRun(t, newConfig(t), compatReferenceSuite, func(bool) string { return baseline })
	upg := prepareCompatRun(t, newConfig(t), compatUpgradeSuite, func(useState bool) string {
		// useState is the upgrade boundary: the stateless initial load is what the baseline wrote
		// the state file with, and every sync after it reads that file -- and so that version's
		// semantics -- on the candidate binary.
		return Ternary(useState, candidate, baseline).(string)
	})
	// Set on both: the point is to hold the input constant and era-correct while only the binary
	// changes. setVariant re-seeds the catalog per variant, and updateSelectedStreams applies this.
	ref.TestConfig.InputGeneration = generation
	upg.TestConfig.InputGeneration = generation

	// Column policies follow the same philosophy: the baseline's era decides what each column can
	// be asserted on. Applied to both sides, so a diff is always the binary and never the fixture.
	policies, err := resolveColumnPolicies(probe.CompatColumnRules, baselineSpec(probe.TestConfig.Driver))
	require.NoError(t, err)
	require.Truef(t, len(policies.seedExcluded) == 0 || probe.SupportsSeedExclusion,
		"columns %s must be excluded from the seed data for this baseline, but the %s fixture does not honour SeedExcludedColumns",
		strings.Join(policies.seedExcluded, ", "), probe.TestConfig.Driver)
	for _, note := range policies.notes {
		t.Logf("compat: %s", note)
	}
	// Seed-excluded columns leave the catalog too, so streams.json never selects a column the
	// fixture left out of the table. The env hook appends sweep-time catalog exclusions on top.
	catalogExcluded := slices.Clone(policies.seedExcluded)
	if raw := os.Getenv(compatExcludeColumnsEnvVar); raw != "" {
		catalogExcluded = append(catalogExcluded, strings.Split(raw, ",")...)
	}
	for _, run := range []*compatRun{ref, upg} {
		run.SeedExcludedColumns = policies.seedExcluded
		run.ExtraExcludedColumns = append(run.ExtraExcludedColumns, catalogExcluded...)
		run.ExtraVolatileColumns = append(run.ExtraVolatileColumns, policies.typeOnly...)
	}

	// One flag, both runs: whichever fails first stops the other at its next variant boundary.
	aborted := &atomic.Bool{}
	ref.aborted, upg.aborted = aborted, aborted

	variants := compatVariants(probe.TestConfig.Driver)
	require.NotEmpty(t, variants, "no compat scenarios for driver %s", probe.TestConfig.Driver)

	// t.Run returns only once its parallel children have finished, so this is the barrier: nothing
	// below it may read a destination either run is still writing.
	completed := t.Run("runs", func(t *testing.T) {
		t.Run("reference", func(t *testing.T) {
			t.Parallel()
			ref.runScenarios(ctx, t, variants)
		})
		t.Run("upgrade", func(t *testing.T) {
			t.Parallel()
			upg.runScenarios(ctx, t, variants)
		})
	})
	require.True(t, completed, "a compat run failed; skipping the comparison, its result would be noise")

	assertStateVersionUnchanged(t, ref, upg)

	t.Run("compare", func(t *testing.T) {
		for _, v := range variants {
			t.Run(v.name, func(t *testing.T) {
				compareVariant(ctx, t, ref, upg, v)
			})
		}
	})
}

// prepareCompatRun scopes one side of the comparison to its own suite: its own testdata
// subdirectory (config, catalog, state, destination configs), its own source table, and -- for
// postgres -- its own replication slot, all courtesy of applySuite.
func prepareCompatRun(t *testing.T, cfg *IntegrationTest, suite string, pick func(useState bool) string) *compatRun {
	t.Helper()
	applySuite(t, cfg.TestConfig, suite)
	// Match the per-suite destination_database seedCatalogFromTestStreams appends, exactly as
	// Test2PCIntegration does: the sync writes to "<ns>_<suite>", so anything reading
	// cfg.DestinationDB back has to target the same namespace.
	cfg.DestinationDB = cfg.DestinationDB + "_" + suite
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	cfg.SyncImage = pick
	// Both sides skip verification. The baseline binary predates the current ExpectedData, and the
	// candidate's syncs run at the baseline's state version and so legitimately produce
	// old-semantics values -- muting only the reference side would red-fail the upgrade side on
	// precisely the gates this suite exists to pin.
	cfg.VerifyDisabled = true
	cfg.PreserveDestination = true

	return &compatRun{
		IntegrationTest: cfg,
		suite:           suite,
		destBaseDB:      cfg.DestinationDB,
		table:           testTableName(cfg.TestConfig),
	}
}

// runScenarios runs every variant in sequence, each into its own destination namespace.
func (r *compatRun) runScenarios(ctx context.Context, t *testing.T, variants []compatVariant) {
	t.Logf("compat run %q: source table %s", r.suite, r.table)

	// The slot lives as long as the source config that names it, and olake validates the CDC
	// configuration at startup for every sync in the suite -- including the incremental ones. Same
	// reasoning as Test2PCIntegration; only postgres needs it.
	if r.TestConfig.Driver == string(constants.Postgres) {
		r.ExecuteQuery(ctx, t, []string{r.table}, "create-slot", false)
		defer r.ExecuteQuery(ctx, t, []string{r.table}, "drop-slot", false)
	}
	if keepTestData() {
		t.Logf("compat run %q: leaving source table %s in place (%s is set); it holds the LAST scenario's data",
			r.suite, r.table, keepDataEnvVar)
	} else {
		defer r.ExecuteQuery(ctx, t, []string{r.table}, "drop", false)
	}

	// Clear every variant's destination once, up front. The bodies' own clearing calls are
	// suppressed for this suite (PreserveDestination) so the candidate binary meets the table the
	// baseline created, which means a previous invocation of the suite would otherwise leave rows
	// behind -- and a stateless CDC or incremental sync does NOT drop the destination the way a
	// full_refresh stream does (only full-load streams are cleared, protocol/sync.go), so nothing
	// else would remove them. Within a run, scenarios stay isolated by having a namespace each.
	r.clearVariantDestinations(t, variants)

	// Stop at the first failed variant, on EITHER side. RunBackwardCompat skips the comparison
	// entirely once one run fails, so the remaining variants would be several minutes of syncs
	// whose output nothing reads -- and on the sweep that cost is paid per version, twice, before
	// the retry does it all again.
	for _, v := range variants {
		if r.aborted != nil && r.aborted.Load() {
			t.Logf("compat run %q: skipping variant %q onwards; the other run already failed", r.suite, v.name)
			break
		}
		if !t.Run(v.name, func(t *testing.T) {
			r.setVariant(t, v.name)
			if err := v.run(r.IntegrationTest, ctx, t, r.table); err != nil {
				t.Fatalf("compat %s scenario %s failed: %v", r.suite, v.name, err)
			}
		}) {
			if r.aborted != nil {
				r.aborted.Store(true)
			}
			t.Logf("compat run %q: stopping after variant %q failed; the comparison is skipped either way", r.suite, v.name)
			break
		}
	}
}

// clearVariantDestinations drops whatever a previous invocation of the suite left behind. Missing
// tables and empty prefixes are not errors -- on a first run there is nothing there.
func (r *compatRun) clearVariantDestinations(t *testing.T, variants []compatVariant) {
	for _, v := range variants {
		db := r.destBaseDB + "_" + v.name
		switch v.destination {
		case "iceberg":
			dropIcebergTable(t, r.table, db)
		case "parquet":
			if err := DeleteParquetFiles(t, db, r.table); err != nil {
				t.Logf("could not clear parquet files at %s/%s (likely absent): %s", db, r.table, err)
			}
		}
	}
}

// setVariant points the next scenario at its own destination namespace, so every scenario's
// output survives the run instead of the next one overwriting it. GetDestinationDatabase returns
// the catalog's baked destination_database verbatim -- --destination-database-prefix is ignored
// once it is set -- so rewriting that field IS the namespace, and cfg.DestinationDB has to track
// it for the comparison to read the same place the sync wrote.
//
// Re-seeding also resets sync_mode to the fixture's, which is what lets an incremental scenario
// follow a CDC one: each body patches what it needs on a clean catalog.
func (r *compatRun) setVariant(t *testing.T, variant string) {
	t.Helper()
	r.DestinationDB = r.destBaseDB + "_" + variant

	seedCatalogFromTestStreams(t, r.TestConfig, r.table)
	require.NoError(t, editJSONFile(r.TestConfig.HostCatalogPath, func(doc map[string]interface{}) error {
		entries, _ := doc["streams"].([]interface{})
		for _, raw := range entries {
			wrapper, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			if ddb, ok := stream["destination_database"].(string); ok && ddb != "" {
				stream["destination_database"] = ddb + "_" + variant
			}
		}
		return nil
	}), "failed to scope the catalog to variant %q", variant)

	// Re-seeding dropped the normalization/partition/filter/exclusion edits with it.
	require.NoError(t, updateSelectedStreams(r.TestConfig, r.Namespace, r.PartitionRegex, r.FilterConfig, []string{r.table}, r.ColumnToExclude, r.ExtraExcludedColumns...),
		"failed to re-apply stream selection for variant %q", variant)
	t.Logf("compat run %q: variant %s -> %s", r.suite, variant, r.DestinationDB)
}

// volatileColumns names the columns that cannot match across two independent runs: wall-clock
// stamps and source-log coordinates. They are compared by TYPE but not by value -- a _cdc_lsn that
// changed from string to bigint is exactly the break this suite exists to catch, even though its
// value was never going to match.
//
// Derived from what each driver already declares, so enabling a new driver needs no new list.
// _olake_id and _op_type are explicitly kept value-compared: GetKeysHash is deterministic over the
// source primary key, and the op type is the record's classification.
func volatileColumns(cfg *IntegrationTest) []string {
	volatile := map[string]bool{olakeTimestampColumn: true}
	for col := range cfg.DefaultCDCColumnsSchema {
		volatile[col] = true
	}
	delete(volatile, olakeIDColumn)
	delete(volatile, opTypeColumn)
	// Applied last, so a driver can put _olake_id back: it is deterministic only when the source
	// primary key is, which mongodb's server-generated ObjectID is not.
	for _, col := range cfg.ExtraVolatileColumns {
		volatile[col] = true
	}
	return slices.Sorted(maps.Keys(volatile))
}

// compareVariant asserts the upgrade run's destination for one scenario is indistinguishable from
// the reference run's.
func compareVariant(ctx context.Context, t *testing.T, ref, upg *compatRun, v compatVariant) {
	spark, err := sparkSession(ctx, t)
	require.NoError(t, err, "failed to connect to Spark Connect server")

	// destBaseDB, not DestinationDB: the latter was rewritten per variant during the run and now
	// holds whichever scenario finished last.
	refDB, upgDB := ref.destBaseDB+"_"+v.name, upg.destBaseDB+"_"+v.name
	var refRel, upgRel string
	switch v.destination {
	case "iceberg":
		refRel = icebergRelation(ctx, t, spark, refDB, ref.table)
		upgRel = icebergRelation(ctx, t, spark, upgDB, upg.table)
	case "parquet":
		refRel = parquetRelation(ctx, t, spark, refDB, ref.table, "ref")
		upgRel = parquetRelation(ctx, t, spark, upgDB, upg.table, "upg")
		// Absence is a comparable state. Both absent means both runs' last case wrote nothing --
		// the normal end of a parquet CDC scenario, whose last case is a delete. Skip rather than
		// pass: there is nothing to compare, and a green here would claim more than it checked.
		// One absent is a genuine finding: the two binaries disagreed about writing anything.
		if refRel == "" || upgRel == "" {
			require.Equalf(t, refRel == "", upgRel == "",
				"only one run produced parquet files for %s (reference %q, upgrade %q): the binaries disagree about whether this case writes output", v.name, refDB, upgDB)
			t.Skipf("neither run left parquet files for %s (its last case writes none); nothing to compare", v.name)
		}
	default:
		t.Fatalf("unknown destination %q", v.destination)
	}

	compareRelations(ctx, t, spark, refRel, upgRel, volatileColumns(ref.IntegrationTest))
}

// icebergRelation refreshes and returns the fully-qualified name of an Iceberg table. The refresh
// matters for the same reason VerifyIcebergSync does one: the shared Spark session caches table
// snapshots, so a table written after the session was built reads as empty without it.
func icebergRelation(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table string) string {
	name := fmt.Sprintf("%s.%s.%s", icebergCatalog, db, table)
	_, err := spark.Sql(ctx, "REFRESH TABLE "+name)
	require.NoErrorf(t, err, "failed to refresh %s -- the run may not have produced it", name)
	return name
}

// parquetRelation registers a temp view over one run's parquet files and returns its name, or ""
// when the run left no files at all.
//
// A parquet variant only ever holds its LAST case's output: the bodies wipe the directory before
// every case because successive syncs write the same column with different types, which Spark
// refuses to read together (CANNOT_MERGE_SCHEMAS, with or without mergeSchema -- F2 in the doc).
// A case that writes nothing therefore leaves the directory absent, which is the normal end state
// of a CDC scenario: its last case is a delete. That is a comparable state, not an error, so it is
// reported to the caller rather than fataled here.
//
// Do NOT `SET spark.sql.parquet.mergeSchema=true` on this session: it makes every subsequent
// direct file query fail with UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY (measured against the
// harness's spark-connect image), which breaks VerifyParquetSync too.
func parquetRelation(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table, side string) string {
	view := fmt.Sprintf("`compat_%s_%s`", side, table)
	path := fmt.Sprintf("s3a://warehouse/%s/%s", db, table)
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
func compareRelations(ctx context.Context, t *testing.T, spark sql.SparkSession, refRel, upgRel string, volatile []string) {
	// 1. Non-vacuity FIRST. Two empty tables satisfy every diff below, and an empty reference is a
	//    plausible outcome, not a far-fetched one: a stream the baseline binary could not validate
	//    is skipped with a Warn and the sync still exits 0 (protocol/sync.go, D3 in the doc). Without
	//    this guard that scenario reports a green.
	refCount := scalarCount(ctx, t, spark, "SELECT COUNT(*) AS n FROM "+refRel)
	require.Greaterf(t, refCount, int64(0),
		"the reference run produced no rows in %s; it is the source of truth, so an empty one makes the whole comparison vacuous (a silently skipped stream looks exactly like this)", refRel)
	upgCount := scalarCount(ctx, t, spark, "SELECT COUNT(*) AS n FROM "+upgRel)
	require.Equalf(t, refCount, upgCount, "row count differs: reference %s has %d, upgrade %s has %d", refRel, refCount, upgRel, upgCount)

	// 2. Schema. Compared as a map, so a column order difference (schema evolution appends in
	//    record-arrival order) is not a failure while an added, dropped or retyped column is. This
	//    is the assertion that catches a type-mapping change -- I6 in the doc.
	refSchema := describeRelation(ctx, t, spark, refRel)
	upgSchema := describeRelation(ctx, t, spark, upgRel)
	require.Equalf(t, refSchema, upgSchema,
		"destination schema differs between the reference and upgrade runs.\n  reference (%s): %v\n  upgrade   (%s): %v", refRel, refSchema, upgRel, upgSchema)

	// 3. Per-op-type counts, so a row diff reads as "5 'u' rows where the reference had 6" rather
	//    than an opaque set difference.
	require.Equalf(t, opTypeCounts(ctx, t, spark, refRel), opTypeCounts(ctx, t, spark, upgRel),
		"per-%s row counts differ between the reference and upgrade runs", opTypeColumn)

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
	reportColumnDiffs(ctx, t, spark, refRel, upgRel, cols)
	logSampleRows(t, "only in the reference run", refRel, onlyInRef)
	logSampleRows(t, "only in the upgrade run", upgRel, onlyInUpg)
	t.Fatalf("row values differ between the reference and upgrade runs: %d row(s) only in %s, %d row(s) only in %s",
		len(onlyInRef), refRel, len(onlyInUpg), upgRel)
}

// reportColumnDiffs names the columns whose values differ, with a sample from each side. Runs one
// query per column, so it is called only after a diff has already been found.
func reportColumnDiffs(ctx context.Context, t *testing.T, spark sql.SparkSession, refRel, upgRel string, cols []string) {
	for _, col := range cols {
		n := scalarCount(ctx, t, spark, fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT %s FROM %s EXCEPT ALL SELECT %s FROM %s)", col, refRel, col, upgRel))
		if n == 0 {
			continue
		}
		t.Logf("  column %s differs in %d row(s)", col, n)
		t.Logf("    reference: %v", sampleColumn(ctx, spark, refRel, col))
		t.Logf("    upgrade:   %v", sampleColumn(ctx, spark, upgRel, col))
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

func logSampleRows(t *testing.T, what, relation string, rows []types.Row) {
	for i, row := range rows {
		if i == 5 {
			t.Logf("  ... and %d more %s", len(rows)-5, what)
			break
		}
		t.Logf("  %s (%s): %v", what, relation, row)
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
	query := fmt.Sprintf("SELECT `%s` AS op, COUNT(*) AS n FROM %s GROUP BY 1", opTypeColumn, relation)
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

// readStateVersion reads the `version` a run's state file ended on. Absent means 0, which is a
// meaningful legacy mode rather than a null -- see C1 in the doc.
func readStateVersion(t *testing.T, c *TestConfig) int {
	t.Helper()
	raw, err := os.ReadFile(c.HostStatePath)
	require.NoErrorf(t, err, "failed to read the state file at %s", c.HostStatePath)
	var state struct {
		Version int `json:"version"`
	}
	require.NoErrorf(t, json.Unmarshal(raw, &state), "failed to parse the state file at %s", c.HostStatePath)
	return state.Version
}

// assertStateVersionUnchanged pins I3 -- a sync never rewrites the version it read -- and reports
// how much the run actually proved. A baseline on the same state version as HEAD exercises
// destination continuity and nothing about any gate; that is a normal outcome for the default
// `latest` baseline, and it must be visible rather than read as a stronger green than it is.
func assertStateVersionUnchanged(t *testing.T, ref, upg *compatRun) {
	refVersion := readStateVersion(t, ref.TestConfig)
	upgVersion := readStateVersion(t, upg.TestConfig)
	require.Equalf(t, refVersion, upgVersion,
		"the upgrade run's state version (%d) does not match the reference run's (%d): the candidate binary rewrote the version it read", upgVersion, refVersion)

	if refVersion == constants.LatestStateVersion {
		t.Logf("NOTE: the baseline is on state version %d, the same as this build. This run pins destination continuity but NO version gate. Set %s to an older release (see the version history in docs/backward-compatibility.md) for gate coverage.",
			refVersion, compatBaselineEnvVar)
		return
	}
	t.Logf("compat: pipeline pinned at state version %d against a build at %d", refVersion, constants.LatestStateVersion)
}
