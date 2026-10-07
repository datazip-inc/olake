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
// Each side runs every sync case on its own long-lived config. After every case both sides wait at
// an output comparison checkpoint, where the two destinations are asserted indistinguishable before
// either moves on; a parquet side holds only that case's files, so each batch is compared once.
//
// The reference run IS the expectation. A gate that stopped firing, a type map that shifted, a
// state key that got renamed: each shows up as a diff at the case that caused it, with no
// expectation file to maintain.

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
	"golang.org/x/mod/semver"
)

const (
	// compatibilityBaselineEnvVar names the baseline to test the local build against, in place of all
	// the older releases with a state version update. Three forms are accepted, see resolveBaselineImage;
	// per-driver overrides use the suffixed form, OLAKE_COMPATIBILITY_BASELINE_POSTGRES.
	compatibilityBaselineEnvVar = "OLAKE_COMPATIBILITY_TEST_BASELINE"
)

type TestHandler struct {
	NewConfig         func(t *testing.T, DriverVersion string) *testutils.TestConfig
	DestinationSchema map[string]string
	ColumnTypes       map[string][]string
	CDCColumnsSchema  map[string]string
}

// RunBackwardCompatibility runs one driver's scenarios twice -- a reference run entirely on the baseline
// image and an upgrade run that hands off to the candidate after the initial load -- then asserts
// the two destinations match. Both sides of all three writer groups (iceberg legacy, iceberg
// arrow, parquet) run in parallel, six isolated pipelines at once.
func (th *TestHandler) RunBackwardCompatibility(t *testing.T) {
	currentConf := th.NewConfig(t, testutils.GetCurrentDriverVersion())
	require.NoError(t, compatibilityRules.validateThresholds())
	baselineVersions, err := getCompatibilityBaselines(t, currentConf.Driver)
	require.NoError(t, err)

	for _, version := range baselineVersions {
		// A commit reads the gates and rules as the newest release it contains; every other spec
		// reads them as itself.
		ruleSpec := version
		if commitID, ok := testutils.ResolveToCommit(currentConf.OlakeRootPath, version); ok {
			version = commitID
			if release := equivalentRelease(currentConf.OlakeRootPath, commitID); release != "" {
				ruleSpec = release
				t.Logf("compatibility: baseline %s reads the gates and rules as %s, the newest release reachable from it", version, release)
			} else {
				t.Logf("compatibility: baseline %s has no release reachable from it; only unconditional rules apply", version)
			}
		}
		reason, err := baselineSkipReason(currentConf, version, ruleSpec)
		require.NoError(t, err)
		if reason != "" {
			t.Run(version, func(t *testing.T) { t.Skip(reason) })
			continue
		}

		t.Run(version, func(t *testing.T) {
			t.Parallel()
			th.runCompatibilityBaseline(t, version, currentConf, ruleSpec)
		})
	}
}

// baselineSkipReason is why this driver does not run against this baseline at all, or "" when it
// does: a baseline that is the candidate itself, the global floor from state-versions.json, then the
// driver's own gate and its data format's in compatibility_rules.json. None needs the baseline's image,
// which is what lets the caller skip a baseline before paying for it.
func baselineSkipReason(conf *testutils.TestConfig, baseline, spec string) (string, error) {
	if baseline == conf.DriverVersion {
		return fmt.Sprintf("baseline %s and candidate %s are the same build, so they are compatible", baseline, conf.DriverVersion), nil
	}
	floorTag, err := compatibilityGlobalFloor()
	if err != nil {
		return "", err
	}
	if semver.IsValid(spec) && semver.Compare(spec, floorTag) < 0 {
		return fmt.Sprintf("baseline %s predates %s, the oldest state-version baseline; the compatibility suite does not run below it",
			spec, floorTag), nil
	}
	gate := compatibilityRules.Drivers[conf.Driver].compatibilityGate
	if reason := gate.skipReason(spec); reason != "" {
		return fmt.Sprintf("%s cannot run baseline %s: %s (compatibility_rules.json: %s)", conf.Driver, spec, reason, gate.Note), nil
	}
	formatGate := compatibilityRules.Drivers[conf.Driver].Formats[conf.DataFormat].compatibilityGate
	if reason := formatGate.skipReason(spec); reason != "" {
		return fmt.Sprintf("%s/%s cannot run baseline %s: %s (compatibility_rules.json: %s)", conf.Driver, conf.DataFormat, baseline, reason, formatGate.Note), nil
	}

	return "", nil
}

// runCompatibilityBaseline runs every writer group's sync modes against one baseline: the reference
// side on baseline's image throughout, the upgrade side handing its stateful syncs to upgrade's.
// ruleSpec is the release the gates and rules read this baseline as (see RunBackwardCompatibility).
func (th *TestHandler) runCompatibilityBaseline(t *testing.T, baselineVersion string, conf *testutils.TestConfig, ruleSpec string) {
	upgradedVersion, driver, dataFormat := conf.DriverVersion, conf.Driver, conf.DataFormat

	driverRules := compatibilityRules.Drivers[driver]

	// Column policies: the baseline's era decides what each column can be asserted on. Applied to
	// both sides, so a diff is always the binary and never the fixture.
	if declared := driverRules.Formats; len(declared) > 0 {
		formats := slices.Sorted(maps.Keys(declared))
		if !slices.Contains(formats, dataFormat) {
			t.Logf("NOTE: %s runs data format %q, which compatibility_rules.json does not declare (declared: %v); no format rule or gate applies to this run.",
				driver, dataFormat, formats)
		} else {
			t.Logf("compatibility: %s declares data formats %v; this run is %q", driver, formats, dataFormat)
		}
	}

	// Every rule -- type-keyed, column-keyed, dated, unconditional -- resolves here into the one
	// policy set the run applies: seeding, catalog and comparison all read it, nothing re-derives.
	policies, err := resolveAssertionPolicies(th, ruleSpec, driverRules, dataFormat)
	require.NoError(t, err)
	for _, note := range policies.notes {
		t.Logf("compatibility: %s", note)
	}

	// Writer-level gates: a group whose writer has a known bounded regression against this
	// baseline is left out, and says so -- the other writers keep their coverage instead of the
	// whole baseline being dropped.
	var groups []compatibilityGroup
	for _, group := range compatibilitySyncModeGroups(driver) {
		if reason := group.gate.skipReason(ruleSpec); reason != "" {
			t.Logf("compatibility: writer group %s not run against this baseline: %s", group.name, reason)
			continue
		}
		groups = append(groups, group)
	}
	require.NotEmpty(t, groups, "no compatibility scenarios for driver %s against this baseline", driver)
	// Whichever side fails first stops every group at its next sync mode boundary: the comparison
	// is skipped either way, so the remaining syncs would be minutes of output nothing reads.

	aborted := &atomic.Bool{}

	// Reports what every failed sync mode found once the parallel groups below finish; registered after
	// the pre-checks so an early failure prints no empty report.
	report := &failureReport{driver: driver, baselineVersion: baselineVersion, upgradedVersion: upgradedVersion}
	t.Cleanup(func() {
		if t.Failed() {
			t.Errorf("%s", testutils.Red(report.render()))
		}
	})

	for _, group := range groups {
		t.Run(group.name, func(t *testing.T) {
			t.Parallel()
			for _, v := range group.syncModes {
				// running sync modes in series as all the compatibility runs are already in parallel so too much parallelism can degrade performance
				if aborted.Load() {
					t.Logf("compatibility group %s: skipping sync mode %q onwards; another run already failed", group.name, v.name)
					break
				}
				diag := &diagnostics{}
				ok := t.Run(v.name, func(t *testing.T) {
					th.runSyncMode(t, conf, baselineVersion, ruleSpec, group, v, policies, diag)
				})
				if !ok {
					report.add(group.name, v.name, diag)
					aborted.Store(true)
					t.Logf("compatibility group %s: stopping after sync mode %q", group.name, v.name)
					break
				}
			}
		})
	}
}

// runSyncMode runs one sync mode's reference and upgrade sides in parallel and compares the two
// destinations after every case, holding both sides at the checkpoint until the comparison is done.
func (th *TestHandler) runSyncMode(t *testing.T, conf *testutils.TestConfig, baselineVersion, ruleSpec string, group compatibilityGroup, v compatibilitySyncMode, policies *assertionPolicies, diag *diagnostics) {
	upgradedVersion := conf.DriverVersion
	cases := syncCasesForDriver(conf.Driver, v.kind)
	checkpoint := newOutputComparisonCheckpoint()
	var configs [2]*testutils.TestConfig // reference, upgraded

	// Each side runs every case on its own long-lived config, both starting on the
	// baseline; the upgrade side hands its stateful syncs to the candidate.
	sides := []struct {
		name        string
		pickVersion func(useState bool) string
	}{
		{"ref", func(bool) string { return baselineVersion }},
		{"upg", func(useState bool) string {
			return testutils.Ternary(useState, upgradedVersion, baselineVersion).(string)
		}},
	}
	var sidesDone sync.WaitGroup
	for i, side := range sides {
		sidesDone.Go(func() {
			t.Run(side.name, func(t *testing.T) {
				t.Cleanup(func() { checkpoint.stopIfFailed(t) })
				cfg := th.NewConfig(t, baselineVersion)
				configs[i] = cfg
				perpareSourceTable(t, cfg, group, v, policies)
				for _, c := range cases {
					cfg.DriverVersion = side.pickVersion(c.useState)
					runSync(t, cfg, group, c)
					if !checkpoint.sideDone() {
						t.Logf("stopping after case %q: the other side or the comparison failed", c.operation)
						return
					}
				}
			})
		})
	}

	// Output comparison checkpoint after every case, once both sides have finished it.
	t.Run("compare", func(t *testing.T) {
		t.Cleanup(func() { checkpoint.stopIfFailed(t) })
		for _, c := range cases {
			if !checkpoint.bothDone() {
				diag.fatalf(t, "a %s side never finished case %q, so the two destinations were never compared; the side's own subtest output has why", v.name, c.operation)
			}
			compareSyncMode(t, diag, policies, configs[0], configs[1], group, v, ruleSpec)
			checkpoint.release()
		}
	})
	checkpoint.stop()
	sidesDone.Wait()
}

// getCompatibilityBaselines returns the baselines to run driver against: the
// OLAKE_COMPATIBILITY_TEST_BASELINE override alone, else every release in `state-versions.json`
// whose bump gated this driver, oldest first -- a bump that touched only other drivers changed
// nothing this driver's state file pins, so it is logged and left out.
func getCompatibilityBaselines(t *testing.T, driver string) ([]string, error) {
	t.Helper()
	if spec := os.Getenv(compatibilityBaselineEnvVar); spec != "" {
		return []string{spec}, nil
	}
	versionBumps, err := testutils.StateVersionBaselines()
	if err != nil {
		return nil, err
	}
	latest, err := testutils.LatestStateVersion()
	if err != nil {
		return nil, err
	}
	specs := make([]string, 0, len(versionBumps))
	for _, bump := range versionBumps {
		if bump.StateVersion == latest {
			t.Logf("no need to run compatibility test with same state version")
			continue
		}
		if !bump.Gates(driver) {
			t.Logf("compatibility: baseline %s not run for %s; state version %d gates only %s", bump.ReleaseTag, driver, bump.StateVersion, bump.Drivers)
			continue
		}
		specs = append(specs, bump.ReleaseTag)
	}
	return specs, nil
}
