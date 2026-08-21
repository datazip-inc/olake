package compatibility

// Backward-compatibility suite.
//
// The contract being tested is docs/backward-compatibility.md: upgrading the OLake binary must not
// change the records or the column types an existing pipeline produces. The state file's `version`
// pins that, so a candidate binary reading a state file an older binary wrote must keep the older
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
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/stretchr/testify/require"
)

const (
	// compatibilityBaselineEnvVar names the baseline to test the local build against, replacing the
	// manifest sweep with a single run. Three forms are accepted, see resolveBaselineImage;
	// per-driver overrides use the suffixed form, OLAKE_COMPATIBILITY_BASELINE_POSTGRES.
	compatibilityBaselineEnvVar = "OLAKE_COMPATIBILITY_BASELINE"

	// releasedImageRepo is where release-tool.sh publishes driver images.
	releasedImageRepo = "olakego/source-%s"

	// compatibilityExcludeColumnsEnvVar appends catalog-level column exclusions to every compatibility run, a
	// sweep affordance for probing a baseline without editing the driver's rules.
	compatibilityExcludeColumnsEnvVar = "OLAKE_COMPATIBILITY_EXCLUDE_COLUMNS"

	// compatibilityRequireBaselineEnvVar turns an unavailable baseline from a skip into a failure. CI
	// sets it: in a release gate, a silently skipped baseline reads as green while testing nothing.
	compatibilityRequireBaselineEnvVar = "OLAKE_COMPATIBILITY_REQUIRE_BASELINE"

	compatibilitySuiteName      = "compatibility"
	compatibilityReferenceSuite = "ref"
	compatibilityUpgradeSuite   = "upg"

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
// compatibilityBaselineSpecs is the list of baselines a run covers: an explicit override (per-driver
// first, then global) is a single run, and otherwise the sweep is every release in the product's
// state-versions.json, oldest first. The list lives there rather than in the Makefile so a
// baseline is added by editing the manifest alone.
func compatibilityBaselineSpecs(rootPath, driver string) ([]string, error) {
	if spec := os.Getenv(compatibilityBaselineEnvVar + "_" + strings.ToUpper(driver)); spec != "" {
		return []string{spec}, nil
	}
	if spec := os.Getenv(compatibilityBaselineEnvVar); spec != "" {
		return []string{spec}, nil
	}
	baselines, err := readBaselineManifest(rootPath)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(baselines, func(a, b stateVersionBaseline) int { return a.StateVersion - b.StateVersion })
	specs := make([]string, 0, len(baselines))
	for _, baseline := range baselines {
		// One release can cover several state versions (a release that jumps the manifest by
		// more than one carries every version it skipped), and running it twice proves nothing.
		if !slices.Contains(specs, baseline.ReleaseTag) {
			specs = append(specs, baseline.ReleaseTag)
		}
	}
	return specs, nil
}

// resolveBaselineImage turns a baseline spec into an image ref present on the local daemon:
//
//	"latest", "v0.6.5"                 -> olakego/source-<driver>:<spec>, pulled
//	"olakego/source-postgres:v0.6.5"   -> used verbatim (any spec naming a repository), pulled
//	"9f3c1ab", "sha:9f3c1ab"           -> built from a detached worktree at that commit
//
// A baseline that has no image is a skip, never a failure: a tag older than the driver itself
// legitimately predates its first release.
func resolveBaselineImage(t *testing.T, cfg *testutils.TestConfig, spec string) string {
	t.Helper()

	sha := strings.TrimPrefix(spec, "sha:")
	if (spec != sha || commitSpec.MatchString(spec)) && gitHasCommit(cfg.OlakeRootPath, sha) {
		return buildBaselineFromCommit(t, cfg, sha)
	}

	image := spec
	if !strings.Contains(spec, "/") {
		image = fmt.Sprintf(releasedImageRepo+":%s", cfg.Driver, spec)
	}
	if err := testutils.EnsureImagePresent(t, image, cfg.ImagePlatform); err != nil {
		if envFlagSet(compatibilityRequireBaselineEnvVar) {
			t.Fatalf("compatibility baseline %s unavailable and %s is set: %s", image, compatibilityRequireBaselineEnvVar, err)
		}
		t.Skipf("compatibility baseline %s unavailable, skipping: %s", image, err)
	}
	return image
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
func buildBaselineFromCommit(t *testing.T, cfg *testutils.TestConfig, sha string) string {
	t.Helper()

	image := fmt.Sprintf("olake/source-%s:compatibility-%s", cfg.Driver, sha)
	if exec.Command("docker", "image", "inspect", image).Run() == nil {
		t.Logf("reusing already-built compatibility baseline %s", image)
		return image
	}

	worktree := filepath.Join(t.TempDir(), "olake-compatibility-"+sha)
	run := func(what string, name string, args ...string) {
		cmd := exec.Command(name, args...)
		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "failed to %s for compatibility baseline %s: %s\n%s", what, sha, err, out)
	}

	run("create the worktree", "git", "-C", cfg.OlakeRootPath, "worktree", "add", "--detach", worktree, sha)
	t.Cleanup(func() {
		_ = exec.Command("git", "-C", cfg.OlakeRootPath, "worktree", "remove", "--force", worktree).Run()
	})

	defer testutils.TrackPhaseTiming(t, "compatibility-baseline", image)()
	run("build the iceberg jar", "make", "-C", worktree, "iceberg.jar")

	if hasMakeTarget(worktree, "docker."+cfg.Driver+".build") {
		run("build the image", "make", "-C", worktree,
			"docker."+cfg.Driver+".build", "IMAGE_TAG=compatibility-"+sha)
		// That target tags olake/source-<driver>:compatibility-<sha> -- deliberately never :local, which
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

func compatibilityGroupSpecs() []compatibilityGroupSpec {
	iceberg := func(arrow bool) func(bool, bool) []compatibilityVariant {
		return func(cdc, inc bool) []compatibilityVariant {
			var out []compatibilityVariant
			if cdc {
				out = append(out, compatibilityVariant{
					name: "cdc", destination: "iceberg",
					run: func(cfg *integration.Test, ctx context.Context, t *testing.T, table string) error {
						return cfg.IcebergWriter(ctx, t, table, arrow, cfg.IcebergFullLoadAndCDC)
					},
				})
			}
			if inc {
				out = append(out, compatibilityVariant{
					name: "inc", destination: "iceberg",
					run: func(cfg *integration.Test, ctx context.Context, t *testing.T, table string) error {
						return cfg.IcebergWriter(ctx, t, table, arrow, cfg.IcebergFullLoadAndIncremental)
					},
				})
			}
			return out
		}
	}
	parquet := func(cdc, inc bool) []compatibilityVariant {
		var out []compatibilityVariant
		if cdc {
			out = append(out, compatibilityVariant{
				name: "cdc", destination: "parquet", emptyFinalState: true,
				run: func(cfg *integration.Test, ctx context.Context, t *testing.T, table string) error {
					return cfg.ParquetFullLoadAndCDC(ctx, t, table)
				},
			})
		}
		if inc {
			out = append(out, compatibilityVariant{
				name: "inc", destination: "parquet",
				run: func(cfg *integration.Test, ctx context.Context, t *testing.T, table string) error {
					return cfg.ParquetFullLoadAndIncremental(ctx, t, table)
				},
			})
		}
		return out
	}
	return []compatibilityGroupSpec{
		{name: "ice_legacy", destination: "iceberg", mode: "legacy", variants: iceberg(false)},
		{name: "ice_arrow", destination: "iceberg", mode: "arrow", variants: iceberg(true)},
		{name: "pq", destination: "parquet", variants: parquet},
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
		variants := spec.variants(cdc, inc)
		if len(variants) == 0 {
			continue
		}
		gate := mergedGate(spec.gateFrom(compatibilityRules.Destinations), spec.gateFrom(driverDestinations))
		groups = append(groups, compatibilityGroup{name: spec.name, gate: gate, variants: variants})
	}
	return groups
}

// RunBackwardCompatibility runs one driver's scenarios twice -- a reference run entirely on the baseline
// image and an upgrade run that hands off to the candidate after the initial load -- then asserts
// the two destinations match. Both sides of all three writer groups (iceberg legacy, iceberg
// arrow, parquet) run in parallel, six isolated pipelines at once.
//
// newConfig MUST return a fresh integration.Test carrying its own *testutils.TestConfig on every call.
// applySuite mutates testutils.TestConfig in place, so two runs sharing one pointer would silently clobber
// each other's paths and write the same files -- which reads as a compatibility failure.
// RunBackwardCompatibility runs the compatibility scenarios against every baseline the manifest lists,
// oldest first, stopping at the first that fails -- later baselines are newer code and would only
// repeat it. A single explicit baseline runs on its own, without the extra subtest level.
func RunBackwardCompatibility(t *testing.T, newConfig func() *Test) {
	probe := newConfig()
	// newConfig is called once per baseline and per run below; validating the probe fails the whole
	// sweep on a malformed fixture before any image is resolved or built. The probe is never run,
	// so it carries the suite name only to satisfy that validation.
	probe.IntegrationTest.Suite = compatibilitySuiteName
	probe.Validate(t)
	specs, err := compatibilityBaselineSpecs(testutils.RepoRoot(t), probe.IntegrationTest.TestConfig.Driver)
	require.NoError(t, err)
	if len(specs) == 1 {
		runCompatibilityBaseline(t, newConfig, specs[0])
		return
	}
	t.Logf("compatibility: sweeping %d baselines from state-versions.json: %s", len(specs), strings.Join(specs, " "))
	for _, spec := range specs {
		if !t.Run(spec, func(t *testing.T) { runCompatibilityBaseline(t, newConfig, spec) }) {
			t.Logf("compatibility: stopping the sweep at %s; the later baselines carry newer code and would repeat it", spec)
			return
		}
	}
}

func runCompatibilityBaseline(t *testing.T, newConfig func() *Test, spec string) {
	ctx := context.Background()
	probe := newConfig()

	// Surfaces a manifest that lags the harness's state version (healed when the first release
	// at the new version ships, compatibility-baselines-release workflow) and fails on unparseable.
	assertBaselineManifestCurrent(t, testutils.RepoRoot(t))

	// The driver's own floor, before anything is pulled or built. A skip, not a failure: the
	// driver declares it cannot run against releases this old (the why lives next to the
	// declaration in compatibility_rules.json), and that limitation is data, not a regression.
	baselineVersion, baselineDated := parseReleaseTag(spec)
	floorTag, err := compatibilityGlobalFloor(testutils.RepoRoot(t))
	require.NoError(t, err)
	globalFloor, _ := parseReleaseTag(floorTag)
	if baselineDated && compareRelease(baselineVersion, globalFloor) < 0 {
		t.Skipf("baseline %s predates %s, the oldest state-version baseline; the compatibility suite does not run below it",
			spec, floorTag)
	}
	driverRules := compatibilityRules.Drivers[probe.IntegrationTest.TestConfig.Driver]
	variantRules := driverRules.Variants[probe.IntegrationTest.TestConfig.DataFormat]
	for _, scoped := range []struct {
		scope string
		gate  compatibilityGate
	}{
		{probe.IntegrationTest.TestConfig.Driver, driverRules.compatibilityGate},
		{probe.IntegrationTest.TestConfig.Driver + "/" + probe.IntegrationTest.TestConfig.DataFormat, variantRules.compatibilityGate},
	} {
		if reason := scoped.gate.skipReason(baselineVersion, baselineDated); reason != "" {
			t.Skipf("%s cannot run baseline %s: %s (compatibility_rules.json: %s)",
				scoped.scope, spec, reason, scoped.gate.Note)
		}
	}

	baseline := resolveBaselineImage(t, probe.IntegrationTest.TestConfig, spec)
	// Build the candidate once, serially, before any parallel child starts: its sync.Once would
	// otherwise fire inside whichever subtest got there first.
	candidate := probe.IntegrationTest.TestConfig.DriverImage
	require.NotEqualf(t, baseline, candidate,
		"the compatibility baseline and the candidate resolve to the same image (%s); the run would compare it with itself and pass", baseline)
	t.Logf("compatibility: baseline %s -> candidate %s", baseline, candidate)

	// Both sides get the input shape the BASELINE shipped with, not today's. A key introduced
	// after the baseline would otherwise read as a behavior change when all it means is that the
	// older binary never knew the key -- see inputGeneration.
	generation, why, err := resolveInputGeneration(spec)
	require.NoError(t, err)
	t.Logf("compatibility: input generation %q (%s)", generation.name, why)
	if generation != currentInputGeneration() {
		t.Logf("NOTE: streams.json is written in the %q shape, so this run pins that older input against the candidate. Set %s=current to compare on today's shape instead.",
			generation.name, compatibilityInputGenerationEnvVar)
	}

	// Column policies follow the same philosophy: the baseline's era decides what each column can
	// be asserted on. Applied to both sides, so a diff is always the binary and never the fixture.
	typeRules := slices.Clone(driverRules.Rules)
	typeRules = append(typeRules, variantRules.Rules...)
	if declared := driverRules.Variants; len(declared) > 0 {
		formats := slices.Sorted(maps.Keys(declared))
		if !slices.Contains(formats, probe.IntegrationTest.TestConfig.DataFormat) {
			t.Logf("NOTE: %s runs data format %q, which compatibility_rules.json does not declare (declared: %v); no variant rule or gate applies to this run.",
				probe.IntegrationTest.TestConfig.Driver, probe.IntegrationTest.TestConfig.DataFormat, formats)
		} else {
			t.Logf("compatibility: %s declares data formats %v; this run is %q", probe.IntegrationTest.TestConfig.Driver, formats, probe.IntegrationTest.TestConfig.DataFormat)
		}
	}
	// A threshold below the oldest baseline the sweep can reach is dead config: the rule reads as
	// protection but can never fire, so it must be an error rather than a quiet no-op.
	for _, rule := range typeRules {
		for _, threshold := range []string{rule.ExcludeBelow, rule.AssertValueFrom} {
			if threshold == "" {
				continue
			}
			bound, ok := parseReleaseTag(threshold)
			require.Truef(t, ok, "compatibility_rules.json: %q is not a release tag", threshold)
			require.Falsef(t, compareRelease(bound, globalFloor) <= 0,
				"compatibility_rules.json: %s rule threshold %s is at or below the oldest reachable baseline %s, so it can never fire (%s). Drop the rule or record it as a note.",
				probe.IntegrationTest.TestConfig.Driver, threshold, floorTag, rule.Note)
		}
	}

	columnRules, err := resolveTypeRules(typeRules, declaredColumnTypes(probe))
	require.NoError(t, err)
	policies, err := resolveColumnPolicies(columnRules, spec)
	require.NoError(t, err)
	require.Truef(t, len(policies.seedExcluded) == 0 || probe.SupportsSeedExclusion,
		"columns %s must be excluded from the seed data for this baseline, but the %s fixture does not honor SeedExcludedColumns",
		strings.Join(policies.seedExcluded, ", "), probe.IntegrationTest.TestConfig.Driver)
	for _, note := range policies.notes {
		t.Logf("compatibility: %s", note)
	}
	// Seed-excluded columns leave the catalog too, so streams.json never selects a column the
	// fixture left out of the table. The env hook appends sweep-time catalog exclusions on top.
	catalogExcluded := slices.Clone(policies.seedExcluded)
	if raw := os.Getenv(compatibilityExcludeColumnsEnvVar); raw != "" {
		catalogExcluded = append(catalogExcluded, strings.Split(raw, ",")...)
	}

	// Writer-level gates: a group whose writer has a known bounded regression against this
	// baseline is left out, and says so -- the other writers keep their coverage instead of the
	// whole baseline being dropped.
	var groups []compatibilityGroup
	for _, g := range compatibilityVariantGroups(probe.IntegrationTest.TestConfig.Driver) {
		reason := g.gate.skipReason(baselineVersion, baselineDated)
		require.NoError(t, err)
		if reason != "" {
			t.Logf("compatibility: writer group %s not run against this baseline: %s", g.name, reason)
			continue
		}
		groups = append(groups, g)
	}
	require.NotEmpty(t, groups, "no compatibility scenarios for driver %s against this baseline", probe.IntegrationTest.TestConfig.Driver)

	// One suite per side x writer group: six isolated pipelines -- each with its own source
	// table, working dir, catalog, state file and (postgres) replication slot -- so the three
	// writers run concurrently on both sides at once instead of six variants in sequence. The
	// suite carries the group name precisely so destination namespaces come out identical to the
	// serial layout's ("..._ref_ice_legacy_cdc"): the group moved from the variant name into the
	// suite, and the comparison and leftover-clearing read the same places they always did.
	type groupPair struct {
		group    compatibilityGroup
		ref, upg *compatibilityRun
	}
	// One flag, every run: whichever fails first stops the rest at their next variant boundary.
	aborted := &atomic.Bool{}
	pairs := make([]groupPair, 0, len(groups))
	for _, g := range groups {
		ref := prepareCompatibilityRun(t, newConfig(), compatibilityReferenceSuite+"_"+g.name, func(bool) string { return baseline })
		upg := prepareCompatibilityRun(t, newConfig(), compatibilityUpgradeSuite+"_"+g.name, func(useState bool) string {
			// useState is the upgrade boundary: the stateless initial load is what the baseline
			// wrote the state file with, and every sync after it reads that file -- and so that
			// version's semantics -- on the candidate binary.
			return testutils.Ternary(useState, candidate, baseline).(string)
		})
		for _, run := range []*compatibilityRun{ref, upg} {
			// Held constant and era-correct on every run, so only the binary varies. setVariant
			// re-seeds the catalog per variant, and updateSelectedStreams applies these.
			run.IntegrationTest.TestConfig.FilterFlags = func(flags []string) []string {
				return dropUnsupportedFlags(generation, flags)
			}
			run.SeedExcludedColumns = policies.seedExcluded
			run.ExtraExcludedColumns = append(run.ExtraExcludedColumns, catalogExcluded...)
			run.ExtraVolatileColumns = append(run.ExtraVolatileColumns, policies.typeOnly...)
			run.aborted = aborted
		}
		pairs = append(pairs, groupPair{group: g, ref: ref, upg: upg})
	}

	// t.Run returns only once its parallel children have finished, so this is the barrier: nothing
	// below it may read a destination any run is still writing.
	completed := t.Run("runs", func(t *testing.T) {
		for _, p := range pairs {
			if probe.SerialGroups {
				// One group at a time; the inner t.Run is the barrier between groups. A group
				// failing still stops the rest through the shared aborted flag.
				t.Run(p.group.name, func(t *testing.T) {
					t.Run("reference", func(t *testing.T) {
						t.Parallel()
						p.ref.runScenarios(ctx, t, p.group.variants)
					})
					t.Run("upgrade", func(t *testing.T) {
						t.Parallel()
						p.upg.runScenarios(ctx, t, p.group.variants)
					})
				})
				continue
			}
			t.Run("reference-"+p.group.name, func(t *testing.T) {
				t.Parallel()
				p.ref.runScenarios(ctx, t, p.group.variants)
			})
			t.Run("upgrade-"+p.group.name, func(t *testing.T) {
				t.Parallel()
				p.upg.runScenarios(ctx, t, p.group.variants)
			})
		}
	})
	require.True(t, completed, "a compatibility run failed; skipping the comparison, its result would be noise")

	// Every group ran the same two binaries, so they must all have pinned the same version; a
	// split would mean the state handoff itself is timing-dependent.
	pinned := -1
	for _, p := range pairs {
		version := assertStateVersionUnchanged(t, p.ref, p.upg)
		if pinned == -1 {
			pinned = version
		}
		require.Equalf(t, pinned, version,
			"writer group %q pinned state version %d while another group pinned %d", p.group.name, version, pinned)
	}
	buildVersion, err := testutils.ProductStateVersion(testutils.RepoRoot(t))
	require.NoError(t, err)
	logStateVersionCoverage(t, pinned, buildVersion)

	t.Run("compare", func(t *testing.T) {
		for _, p := range pairs {
			for _, v := range p.group.variants {
				t.Run(p.group.name+"_"+v.name, func(t *testing.T) {
					compareVariant(ctx, t, p.ref, p.upg, v)
				})
			}
		}
	})
}

// prepareCompatibilityRun scopes one side of the comparison to its own suite: its own testdata
// subdirectory (config, catalog, state, destination configs), its own source table, and -- for
// postgres -- its own replication slot, all courtesy of testutils.TestConfig.Setup.
func prepareCompatibilityRun(t *testing.T, cfg *Test, suite string, pick func(useState bool) string) *compatibilityRun {
	t.Helper()
	// A fixture that already isolated itself keeps that scope: the compatibility suite nests under it, so
	// two variants sharing a DataFormat (s3's Parquet and ParquetInMemory) never collide on a
	// source prefix, table or namespace when the groups run concurrently.
	compatibilitySuite := suite
	if base := cfg.IntegrationTest.TestConfig.Suite; base != "" {
		suite = base + "_" + compatibilitySuite
	}
	cfg.IntegrationTest.TestConfig.Suite = suite
	// Six pipelines run at once, so each needs the source resources its own: for postgres that is
	// the replication slot they would otherwise all advance.
	cfg.IntegrationTest.TestConfig.IsolateSource = true
	cfg.IntegrationTest.TestConfig.Setup(t)

	cfg.IntegrationTest.TestConfig.SyncImage = pick
	// Both sides skip verification. The baseline binary predates the current ExpectedData, and the
	// candidate's syncs run at the baseline's state version and so legitimately produce
	// old-semantics values -- muting only the reference side would red-fail the upgrade side on
	// precisely the gates this suite exists to pin.
	cfg.IntegrationTest.VerifyDisabled = true
	cfg.IntegrationTest.PreserveDestination = true

	return &compatibilityRun{
		Test:       cfg,
		suite:      suite,
		destBaseDB: cfg.IntegrationTest.TestConfig.DestinationDB,
		table:      cfg.IntegrationTest.TestConfig.GetTableName(),
	}
}

// runScenarios runs every variant in sequence, each into its own destination namespace.
func (r *compatibilityRun) runScenarios(ctx context.Context, t *testing.T, variants []compatibilityVariant) {
	t.Logf("compatibility run %q: source table %s", r.suite, r.table)

	// The slot lives as long as the source config that names it, and olake validates the CDC
	// configuration at startup for every sync in the suite -- including the incremental ones. Same
	// reasoning as Test2PCIntegration; only postgres needs it.
	if r.IntegrationTest.TestConfig.Driver == string(constants.Postgres) {
		r.IntegrationTest.TestConfig.ExecuteQuery(ctx, t, r.IntegrationTest.TestConfig, "create-slot")
		defer r.IntegrationTest.TestConfig.ExecuteQuery(ctx, t, r.IntegrationTest.TestConfig, "drop-slot")
	}
	if testutils.KeepTestData() {
		t.Logf("compatibility run %q: leaving source table %s in place (OLAKE_TEST_KEEP_DATA is set); it holds the LAST scenario's data",
			r.suite, r.table)
	} else {
		defer r.IntegrationTest.TestConfig.ExecuteQuery(ctx, t, r.IntegrationTest.TestConfig, "drop")
	}

	// Clear every variant's destination once, up front. The bodies' own clearing calls are
	// suppressed for this suite (PreserveDestination) so the candidate binary meets the table the
	// baseline created, which means a previous invocation of the suite would otherwise leave rows
	// behind -- and a stateless CDC or incremental sync does NOT drop the destination the way a
	// full_refresh stream does (only full-load streams are cleared, protocol/sync.go), so nothing
	// else would remove them. Within a run, scenarios stay isolated by having a namespace each.
	r.clearVariantDestinations(t, variants)

	// Stop at the first failed variant, on EITHER side. RunBackwardCompatibility skips the comparison
	// entirely once one run fails, so the remaining variants would be several minutes of syncs
	// whose output nothing reads -- and on the sweep that cost is paid per version, twice, before
	// the retry does it all again.
	for _, v := range variants {
		if r.aborted != nil && r.aborted.Load() {
			t.Logf("compatibility run %q: skipping variant %q onwards; another run already failed", r.suite, v.name)
			break
		}
		if !t.Run(v.name, func(t *testing.T) {
			r.setVariant(t, v.name)
			if err := v.run(r.IntegrationTest, ctx, t, r.table); err != nil {
				t.Fatalf("compatibility %s scenario %s failed: %v", r.suite, v.name, err)
			}
		}) {
			if r.aborted != nil {
				r.aborted.Store(true)
			}
			t.Logf("compatibility run %q: stopping after variant %q failed; the comparison is skipped either way", r.suite, v.name)
			break
		}
	}
}

// clearVariantDestinations drops whatever a previous invocation of the suite left behind. Missing
// tables and empty prefixes are not errors -- on a first run there is nothing there.
func (r *compatibilityRun) clearVariantDestinations(t *testing.T, variants []compatibilityVariant) {
	for _, v := range variants {
		db := r.destBaseDB + "_" + v.name
		switch v.destination {
		case "iceberg":
			integration.DropIcebergTable(t, r.table, db)
		case "parquet":
			if err := integration.DeleteParquetFiles(t, db, r.table); err != nil {
				t.Logf("could not clear parquet files at %s/%s (likely absent): %s", db, r.table, err)
			}
		}
	}
}

// setVariant points the next scenario at its own destination namespace, so every scenario's
// output survives the run instead of the next one overwriting it. GetDestinationDatabase returns
// the catalog's baked destination_database verbatim -- --destination-database-prefix is ignored
// once it is set -- so rewriting that field IS the namespace, and cfg.IntegrationTest.TestConfig.DestinationDB has to track
// it for the comparison to read the same place the sync wrote.
//
// Re-seeding also resets sync_mode to the fixture's, which is what lets an incremental scenario
// follow a CDC one: each body patches what it needs on a clean catalog.
func (r *compatibilityRun) setVariant(t *testing.T, variant string) {
	t.Helper()
	r.IntegrationTest.TestConfig.DestinationDB = r.destBaseDB + "_" + variant

	r.IntegrationTest.TestConfig.SeedCatalog(t)
	require.NoError(t, testutils.EditJSONFile(r.IntegrationTest.TestConfig.GetFilePath("test_streams.json"), func(doc map[string]interface{}) error {
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

	// Re-seeding dropped the normalization/partition/exclusion edits with it. No filter is applied:
	// this suite compares what a sync produces, and a filter would only narrow both sides equally.
	require.NoError(t, testutils.UpdateSelectedStreams(r.IntegrationTest.TestConfig, r.IntegrationTest.TestConfig.Namespace, r.IntegrationTest.TestConfig.PartitionRegex, "", []string{r.table}, r.IntegrationTest.TestConfig.ColumnToExclude, r.ExtraExcludedColumns...),
		"failed to re-apply stream selection for variant %q", variant)
	t.Logf("compatibility run %q: variant %s -> %s", r.suite, variant, r.IntegrationTest.TestConfig.DestinationDB)
}

// declaredColumnTypes is what a data_types rule resolves against: the driver's own per-column type
// declarations, which the sync suite already asserts through testutils.GlobalTypeMapping and so cannot drift
// from the fixture. CompatibilityColumnTypes layers on top for the few types a declaration cannot
// express (a charset, say, is a modifier on varchar rather than a type of its own).
func declaredColumnTypes(cfg *Test) map[string][]string {
	types := map[string][]string{}
	for column, declared := range cfg.IntegrationTest.DestinationDataTypeSchema {
		if declared = strings.ToLower(strings.TrimSpace(declared)); declared != "" {
			types[column] = append(types[column], declared)
		}
	}
	for column, tags := range cfg.ColumnTypes {
		for _, tag := range tags {
			if !slices.Contains(types[column], tag) {
				types[column] = append(types[column], tag)
			}
		}
	}
	return types
}

// volatileColumns names the columns that cannot match across two independent runs: wall-clock
// stamps and source-log coordinates. They are compared by TYPE but not by value -- a _cdc_lsn that
// changed from string to bigint is exactly the break this suite exists to catch, even though its
// value was never going to match.
//
// Derived from what each driver already declares, so enabling a new driver needs no new list.
// _olake_id and _op_type are explicitly kept value-compared: GetKeysHash is deterministic over the
// source primary key, and the op type is the record's classification.
func volatileColumns(cfg *Test) []string {
	volatile := map[string]bool{olakeTimestampColumn: true}
	for col := range cfg.IntegrationTest.DefaultCDCColumnsSchema {
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
func compareVariant(ctx context.Context, t *testing.T, ref, upg *compatibilityRun, v compatibilityVariant) {
	spark, err := integration.SparkSession(ctx, t)
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
		// Absence is a comparable state, so it is asserted rather than skipped over. One side
		// absent is a genuine finding: the binaries disagree about whether this case writes
		// output. Both sides absent is the verified outcome for a variant that ENDS empty
		// (emptyFinalState), and a shared failure to produce rows for any other -- the one shape
		// of regression a row diff can never catch, because there are no rows to diff.
		if refRel == "" || upgRel == "" {
			require.Equalf(t, refRel == "", upgRel == "",
				"only one run produced parquet files for %s (reference %q, upgrade %q): the binaries disagree about whether this case writes output", v.name, refDB, upgDB)
			require.Truef(t, v.emptyFinalState,
				"neither run left parquet files for %s (reference %q, upgrade %q), but its last case writes rows: both binaries produced nothing where output is expected", v.name, refDB, upgDB)
			t.Logf("verified: neither run leaves parquet files for %s -- its last case is a delete-only batch, which writes none", v.name)
			return
		}
	default:
		t.Fatalf("unknown destination %q", v.destination)
	}

	compareRelations(ctx, t, spark, refRel, upgRel, volatileColumns(ref.Test))
}

// icebergRelation refreshes and returns the fully-qualified name of an Iceberg table. The refresh
// matters for the same reason VerifyIcebergSync does one: the shared Spark session caches table
// snapshots, so a table written after the session was built reads as empty without it.
func icebergRelation(ctx context.Context, t *testing.T, spark sql.SparkSession, db, table string) string {
	name := fmt.Sprintf("%s.%s.%s", integration.IcebergCatalog, db, table)
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
	view := fmt.Sprintf("`compatibility_%s_%s`", side, table)
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
func readStateVersion(t *testing.T, c *testutils.TestConfig) int {
	t.Helper()
	raw, err := os.ReadFile(c.GetFilePath("state.json"))
	require.NoErrorf(t, err, "failed to read the state file at %s", c.GetFilePath("state.json"))
	var state struct {
		Version int `json:"version"`
	}
	require.NoErrorf(t, json.Unmarshal(raw, &state), "failed to parse the state file at %s", c.GetFilePath("state.json"))
	return state.Version
}

// assertStateVersionUnchanged pins I3 -- a sync never rewrites the version it read -- for one
// side pair, and returns the version the pipelines were pinned at.
// assertBaselineManifestCurrent checks the baselines in the product's constants/state-versions.json
// against the state version this harness tests for. A manifest that lags is a NOTE, not a failure:
// entries are added by the compatibility-baselines-release workflow when the first release at a version
// ships, so between a bump and its release the gap is the expected state. A manifest that cannot
// be parsed does fail: the sweep would silently shrink.
// stateVersionBaseline is one row of the product's constants/state-versions.json.
type stateVersionBaseline struct {
	StateVersion int    `json:"state_version"`
	ReleaseTag   string `json:"release_tag"`
}

func baselineManifestPath(rootPath string) string {
	return filepath.Join(rootPath, "constants", "state-versions.json")
}

func readBaselineManifest(rootPath string) ([]stateVersionBaseline, error) {
	path := baselineManifestPath(rootPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the baseline manifest at %s: %w", path, err)
	}
	var manifest struct {
		Baselines []stateVersionBaseline `json:"baselines"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	if len(manifest.Baselines) == 0 {
		return nil, fmt.Errorf("%s carries no baselines; the sweep would silently shrink", path)
	}
	return manifest.Baselines, nil
}

// compatibilityGlobalFloor is the oldest baseline the suite runs for any driver: the oldest entry in the
// product's state-versions.json. Derived rather than restated, so adding or retiring a baseline
// moves the floor with it.
func compatibilityGlobalFloor(rootPath string) (string, error) {
	baselines, err := readBaselineManifest(rootPath)
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

func assertBaselineManifestCurrent(t *testing.T, rootPath string) {
	t.Helper()
	path := baselineManifestPath(rootPath)
	baselines, err := readBaselineManifest(rootPath)
	require.NoError(t, err)

	manifestMax := -1
	for _, baseline := range baselines {
		_, ok := parseReleaseTag(baseline.ReleaseTag)
		require.Truef(t, ok, "baseline for state version %d in %s: release_tag %q is not a release tag",
			baseline.StateVersion, path, baseline.ReleaseTag)
		manifestMax = max(manifestMax, baseline.StateVersion)
	}
	buildVersion, err := testutils.ProductStateVersion(rootPath)
	require.NoError(t, err)
	if manifestMax != buildVersion {
		t.Logf("NOTE: state-versions.json's baselines top out at state version %d while the build is at %d. The entry is added when the first release at the new version ships (compatibility-baselines-release workflow); until then sweeps have no baseline for it.",
			manifestMax, buildVersion)
	}
}

func assertStateVersionUnchanged(t *testing.T, ref, upg *compatibilityRun) int {
	refVersion := readStateVersion(t, ref.IntegrationTest.TestConfig)
	upgVersion := readStateVersion(t, upg.IntegrationTest.TestConfig)
	require.Equalf(t, refVersion, upgVersion,
		"the upgrade run's state version (%d, suite %s) does not match the reference run's (%d, suite %s): the candidate binary rewrote the version it read",
		upgVersion, upg.suite, refVersion, ref.suite)
	return refVersion
}

// logStateVersionCoverage reports how much the run actually proved. A baseline on the same state
// version as HEAD exercises destination continuity and nothing about any gate; that is a normal
// outcome for the default `latest` baseline, and it must be visible rather than read as a
// stronger green than it is.
func logStateVersionCoverage(t *testing.T, version, buildVersion int) {
	if version == buildVersion {
		t.Logf("NOTE: the baseline is on state version %d, the same as this build. This run pins destination continuity but NO version gate. Set %s to an older release (see the version history in docs/backward-compatibility.md) for gate coverage.",
			version, compatibilityBaselineEnvVar)
		return
	}
	t.Logf("compatibility: pipeline pinned at state version %d against a build at %d", version, buildVersion)
}
