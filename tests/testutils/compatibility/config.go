package compatibility

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/stretchr/testify/require"
)

// Test is what a backward-compatibility run needs beyond a single-image run. It
// holds an integration.Test rather than growing it: that type stays the description of running one
// image once, and everything two-image-specific -- the fixture's type vocabulary, the seed
// exclusion contract, the cross-run comparison's column policies -- lives here.
type Test struct {
	// IntegrationTest is the single-image run this compatibility run is built from. Named rather
	// than embedded, so the two configurations stay distinguishable at every call site.
	IntegrationTest *integration.Test
	// ExtraExcludedColumns drops columns from the compatibility runs' catalogs entirely, on both sides.
	// For columns a baseline image simply cannot produce -- e.g. mysql's ucs2/utf16le/latin1
	// columns, which before #940 (first released in v0.7.2) reach the Iceberg writer as invalid
	// UTF-8 and fail the gRPC marshal. Without this the baseline sync fails and retries with a
	// doubling backoff, which reads as a hang rather than an error.
	ExtraExcludedColumns []string
	// ExtraVolatileColumns names further columns that cannot match across two independent runs,
	// and so are compared by type but not by value. Applied after the defaults, so it can also put
	// _olake_id back into the volatile set for a driver whose primary key is server-generated
	// (mongodb's _id). See volatileColumns.
	ExtraVolatileColumns []string
	// ColumnTypes tags fixture columns with the data-type identifiers compatibility_rules.json's
	// type-keyed rules select on; resolveTypeRules maps the rules onto columns through it.
	ColumnTypes map[string][]string
	// SeedExcludedColumns is derived from the resolved compatibility rules by RunBackwardCompatibility. A
	// driver's ExecuteQuery wrapper reads it at call time to keep these columns out of its DDL and DML.
	SeedExcludedColumns []string
	// SupportsSeedExclusion declares that the driver's fixture honors SeedExcludedColumns. A rule
	// needing seed exclusion on a driver without it is a hard failure, never a silent no-op.
	SupportsSeedExclusion bool
	// SerialGroups runs the writer groups one after another instead of all in parallel,
	// for drivers whose pipelines interfere across suites: kafka's discover enumerates the whole
	// broker (every group's topics), and its per-partition writers multiply the JVM count.
	// Reference and upgrade still run in parallel within each group.
	SerialGroups bool
}

// Validate checks that the Test is valid and complete in order to derive/setup.
func (c *Test) Validate(t *testing.T) {
	t.Helper()
	require.NotNil(t, c.IntegrationTest, "Test.IntegrationTest is not set")
	c.IntegrationTest.Validate(t)
	// RunBackwardCompatibility derives this per baseline and overwrites whatever is here, so a
	// fixture setting it has written something that will silently never be read.
	require.Empty(t, c.SeedExcludedColumns,
		"Test.SeedExcludedColumns is derived per baseline by RunBackwardCompatibility; setting it here has no effect")
	// A column dropped from the catalog never reaches the comparison the volatile list exempts it
	// from, so naming it in both says two different things about the same column.
	for _, excluded := range c.ExtraExcludedColumns {
		require.NotContainsf(t, c.ExtraVolatileColumns, excluded,
			"column %q is in both ExtraExcludedColumns and ExtraVolatileColumns", excluded)
	}
	// TODO: assert ColumnTypes names only columns the fixture declares -- a rule keyed on a type
	// nothing carries resolves to nothing, and the gate it encodes stops firing with no diagnostic.
}

// compatibilityRun is one side of the comparison: a Test whose files, source table and
// destination namespaces are scoped to its own suite, and whose syncs are routed to images by
// SyncImage.
type compatibilityRun struct {
	*Test
	suite string
	// destBaseDB is DestinationDB before a variant suffix is appended; DestinationDB itself is
	// rewritten per variant so all of a run's scenarios survive to be compared.
	destBaseDB string
	table      string
	// aborted is shared by every run. They execute under t.Parallel(), so without it the healthy
	// runs play out their remaining variants after one has already failed -- and the comparison
	// is skipped either way, so every one of those syncs is discarded.
	aborted *atomic.Bool
}

// compatibilityVariant is one scenario, run once per side. Each gets its own destination namespace, so
// the six scenarios TestSync fans out to all survive the run and can be compared independently
// instead of overwriting one table.
type compatibilityVariant struct {
	name        string
	destination string // "iceberg" or "parquet"
	run         func(cfg *integration.Test, ctx context.Context, t *testing.T, table string) error
	// emptyFinalState marks a variant whose LAST case writes no output (parquet CDC ends on a
	// delete-only batch, and parquet compares only its last case's files -- see the per-case wipe
	// in ParquetFullLoadAndCDC). Both sides ending empty is then the asserted outcome; for
	// every other variant it is a failure to produce output that no row diff would catch.
	emptyFinalState bool
}

// compatibilityGroup is one destination writer's scenarios, CDC then incremental, in that order. Groups
// are the parallelism unit: each side x group pair runs as its own suite, so the three writers
// proceed concurrently while the modes inside a group stay serial -- they share the group's
// source table, and interleaved DML would read as CDC diffs.
//
// Groups are also the unit of the writer-level version gates: a bounded regression in ONE writer
// must not cost the sweep the other writers' coverage on those baselines, the way a global floor
// would. Same philosophy as ColumnRule, one level up.
type compatibilityGroup struct {
	name string
	// gate bounds which baselines this group runs against; it comes from compatibility_rules.json.
	gate     compatibilityGate
	variants []compatibilityVariant
}

// compatibilityGroupSpec binds a group name to the harness functions its variants call. This list is the
// one part of the compatibility fan-out that must be code; every baseline gate on these groups is config
// (compatibility_rules.json's groups block, overlaid by the driver's own).
type compatibilityGroupSpec struct {
	name string
	// destination and mode locate this group's gates in compatibility_rules.json; mode is empty
	// for a destination that has none.
	destination string
	mode        string
	variants    func(cdc, inc bool) []compatibilityVariant
}
