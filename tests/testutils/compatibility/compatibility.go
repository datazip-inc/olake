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
	"fmt"
	"maps"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const (
	// compatibilityBaselineEnvVar names the baseline to test the local build against, replacing the
	// manifest sweep with a single run. Three forms are accepted, see resolveBaselineImage;
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
	currentConf := th.NewConfig(t, testutils.CurrentDriverVersion)
	require.NoError(t, compatibilityRules.validateThresholds())
	baselineVersions, err := getCompatibilityBaselines(t, currentConf.Driver)
	require.NoError(t, err)

	for _, version := range baselineVersions {
		// A commit reads the gates and rules as the newest release it contains; every other spec
		// reads them as itself.
		ruleSpec := version
		if commitID, ok := testutils.ResolveToCommit(currentConf.OlakeRootPath, version); ok {
			if release := equivalentRelease(currentConf.OlakeRootPath, commitID); release != "" {
				ruleSpec = release
				t.Logf("compatibility: baseline %s reads the gates and rules as %s, the newest release reachable from it", version, release)
			} else {
				t.Logf("compatibility: baseline %s has no release reachable from it; only unconditional rules apply", version)
			}
		}
		reason, err := baselineSkipReason(currentConf.Driver, ruleSpec)
		require.NoError(t, err)
		if reason != "" {
			t.Run(version, func(t *testing.T) { t.Skip(reason) })
			continue
		}

		baselineConf := th.NewConfig(t, version)
		if passed := t.Run(baselineConf.DriverVersion, func(t *testing.T) {
			t.Parallel()
			th.runCompatibilityBaseline(t, baselineConf, currentConf, ruleSpec)
		}); !passed {
			t.Logf("compatibility: stopping the sweep at %s; the later baselines carry newer code and would repeat it", version)
			return
		}
	}
}

// baselineSkipReason is why this driver does not run against this baseline at all, or "" when it
// does: the global floor from state-versions.json, then the driver's own gate in
// compatibility_rules.json. Both are answerable from the driver name alone, which is what lets the
// caller skip a baseline before paying for its image.
func baselineSkipReason(driver, spec string) (string, error) {
	version, isRelease := parseReleaseTag(spec)
	floorTag, err := compatibilityGlobalFloor()
	if err != nil {
		return "", err
	}
	if globalFloor, _ := parseReleaseTag(floorTag); isRelease && compareRelease(version, globalFloor) < 0 {
		return fmt.Sprintf("baseline %s predates %s, the oldest state-version baseline; the compatibility suite does not run below it",
			spec, floorTag), nil
	}
	gate := compatibilityRules.Drivers[driver].compatibilityGate
	if reason := gate.skipReason(version, isRelease); reason != "" {
		return fmt.Sprintf("%s cannot run baseline %s: %s (compatibility_rules.json: %s)", driver, spec, reason, gate.Note), nil
	}

	return "", nil
}

// runCompatibilityBaseline runs every writer group's variants against one baseline: the reference
// side on baseline's image throughout, the upgrade side handing its stateful syncs to upgrade's.
// ruleSpec is the release the gates and rules read this baseline as (see RunBackwardCompatibility).
func (th *TestHandler) runCompatibilityBaseline(t *testing.T, baseline, upgrade *testutils.TestConfig, ruleSpec string) {
	spec, driver, dataFormat := baseline.DriverVersion, baseline.Driver, baseline.DataFormat

	baselineVersion, baselineIsRelease := parseReleaseTag(ruleSpec)
	driverRules := compatibilityRules.Drivers[driver]
	variantRules := driverRules.Variants[dataFormat]
	if reason := variantRules.compatibilityGate.skipReason(baselineVersion, baselineIsRelease); reason != "" {
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
	policies, err := resolveAssertionPolicies(th, ruleSpec, driverRules, dataFormat)
	require.NoError(t, err)
	for _, note := range policies.notes {
		t.Logf("compatibility: %s", note)
	}

	// Writer-level gates: a group whose writer has a known bounded regression against this
	// baseline is left out, and says so -- the other writers keep their coverage instead of the
	// whole baseline being dropped.
	var groups []compatibilityGroup
	for _, group := range compatibilityVariantGroups(driver) {
		if reason := group.gate.skipReason(baselineVersion, baselineIsRelease); reason != "" {
			t.Logf("compatibility: writer group %s not run against this baseline: %s", group.name, reason)
			continue
		}
		groups = append(groups, group)
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
		for _, group := range groups {
			t.Run(group.name, func(t *testing.T) {
				t.Parallel()
				for _, v := range group.variants {
					// running variants in series as all the compatibility runs are already in parallel so too much parallelism can degrade performance
					if aborted.Load() {
						t.Logf("compatibility group %s: skipping variant %q onwards; another run already failed", group.name, v.name)
						break
					}
					diag := &diagnostics{}
					ok := t.Run(v.name, func(t *testing.T) {
						// Both sides start on the baseline version; pick moves the upgrade side's
						// stateful syncs to the candidate.
						var ref, upg *testutils.TestConfig
						if passed := t.Run("run", func(t *testing.T) {
							t.Run("ref", func(t *testing.T) {
								t.Parallel()
								ref = th.NewConfig(t, baseline.DriverVersion)
								runVariantSide(t, ref, group, v, referencePick, policies)
							})
							t.Run("upg", func(t *testing.T) {
								t.Parallel()
								upg = th.NewConfig(t, baseline.DriverVersion)
								runVariantSide(t, upg, group, v, upgradePick, policies)
							})
						}); !passed {
							diag.fatalf(t, "a %s side never finished, so the two destinations were never compared; the side's own subtest output has why", v.name)
						}
						compareVariant(t, diag, policies, ref, upg, group, v)
					})
					if !ok {
						report.add(group.name, v.name, diag)
						aborted.Store(true)
						t.Logf("compatibility group %s: stopping after variant %q", group.name, v.name)
						break
					}
				}
			})
		}
	})
	require.Truef(t, completed, "%s", report.render())
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
	slices.SortFunc(versionBumps, func(a, b testutils.StateVersionBaseline) int { return a.StateVersion - b.StateVersion })
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
		// One release can cover several state versions (a release that jumps the manifest by
		// more than one carries every version it skipped), and running it twice proves nothing.
		if !slices.Contains(specs, bump.ReleaseTag) {
			specs = append(specs, bump.ReleaseTag)
		}
	}
	return specs, nil
}
