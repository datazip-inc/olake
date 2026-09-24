package compatibility

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

const (
	scenarioCDC         = "cdc"
	scenarioIncremental = "inc"
)

// compatibilityVariant is one scenario, run once per side. Each pair gets its own destination
// namespace through its subtest-derived suite, so every scenario's output survives to be compared.
type compatibilityVariant struct {
	name string
	kind string
	// emptyFinalState marks a variant whose LAST case writes no output; both sides ending empty is
	// then the asserted outcome, for every other variant it is a failure no row diff would catch.
	emptyFinalState bool
}

// compatibilityGroup is one destination writer's scenarios. Groups are the parallelism unit, and
// the unit of the writer-level version gates from compatibility_rules.json.
type compatibilityGroup struct {
	compatibilityGroupSpec
	gate     compatibilityGate
	variants []compatibilityVariant
}

type compatibilityGroupSpec struct {
	name string
	// destination and mode locate this group's gates in compatibility_rules.json; destinationFile
	// is the destination config its syncs run against.
	destination     string
	mode            string
	destinationFile string
}

// outputComparisonCheckpoint holds both sides after every sync case until their outputs are
// compared. Any participant failing stops the other two instead of leaving them blocked.
type outputComparisonCheckpoint struct {
	synced   chan struct{}
	compared chan struct{}
	stopped  chan struct{}
	stop     func()
}

// syncCase is one sync of a scenario: the DML that precedes it and whether it reads state.
type syncCase struct {
	operation string
	useState  bool
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

// syncCasesForDriver is the sequence of operations TestSync runs
func syncCasesForDriver(driver, kind string) []syncCase {
	switch {
	case kind == scenarioIncremental:
		return []syncCase{{operation: "", useState: false}, {operation: "insert", useState: true}, {operation: "update", useState: true}}
	case driver == string(constants.Kafka):
		// Kafka is strict-CDC: no stateless full load, and no deletes to replay.
		return []syncCase{{operation: "", useState: false}, {operation: "update", useState: true}}
	default:
		return []syncCase{
			{operation: "", useState: false},
			{operation: "insert", useState: true},
			{operation: "update", useState: true},
			{operation: "delete", useState: true},
		}
	}
}

// getDriverVersionFoSync gives what driver version the sync should run with in case of stateful version upgrade
func getDriverVersionForSync(useState bool, oldVersion, newVersion string) string {
	return testutils.Ternary(useState, newVersion, oldVersion).(string)
}

// prepareSourceTable will execute necessary queries to setup the source before the first stateless sync
func perpareSourceTable(
	t *testing.T,
	cfg *testutils.TestConfig,
	group compatibilityGroup,
	v compatibilityVariant,
	policies *assertionPolicies,
) {
	ctx := t.Context()
	table := cfg.GetTableName()
	t.Logf("compatibility side %q: source table %s", cfg.Suite, table)

	// The driver's own ExecuteQuery reads this, so a column a rule excluded for this baseline
	// never reaches the seed DDL or DML.
	cfg.SeedExcludedColumns = policies.seedExcluded

	// Stream selection on the freshly rendered catalog. No filter and no column selection: this
	// suite compares what a sync produces, and either would only narrow both sides equally.
	require.NoError(t, cfg.IsolateDestinationDB(), "failed to isolate the compatibility destination database")
	require.NoError(t, testutils.UpdateSelectedStreams(cfg, cfg.Namespace, cfg.PartitionRegex, "", []string{table}, policies.seedExcluded),
		"failed to select the compatibility stream")
	// A seed-excluded column is absent from the table, so it leaves the catalog's schema too: a
	// binary that predates column selection writes every column the catalog declares.
	require.NoError(t, removeColumnsFromTypeSchema(cfg, policies.seedExcluded), "failed to remove the seed-excluded columns from the streams.json type_schema")
	if v.kind == scenarioIncremental {
		require.NoError(t, setIncrementalMode(cfg, table), "failed to patch streams.json for incremental")
	}

	// Whatever a previous invocation of this same test name left behind, cleared up front; the
	// scenarios themselves never clear, so the candidate binary meets the table the baseline made.
	clearDestination(t, group, cfg.DestinationDB, table)

	// Dropped when the side finishes all its cases; t.Context() is already canceled by then.
	if !testutils.KeepTestData() {
		t.Cleanup(func() { cfg.ExecuteQuery(context.Background(), t, cfg, "drop") })
	}

	// Seed the source: the same reset every TestSync scenario starts from.
	cfg.ExecuteQuery(ctx, t, cfg, "drop")
	cfg.ExecuteQuery(ctx, t, cfg, "create")
	cfg.ExecuteQuery(ctx, t, cfg, "add")
}

func newOutputComparisonCheckpoint() *outputComparisonCheckpoint {
	stopped := make(chan struct{})
	return &outputComparisonCheckpoint{
		synced:   make(chan struct{}),
		compared: make(chan struct{}),
		stopped:  stopped,
		stop:     sync.OnceFunc(func() { close(stopped) }),
	}
}

// sideDone reports a side's case finished and waits for its comparison; false means the run stopped.
func (b *outputComparisonCheckpoint) sideDone() bool {
	select {
	case b.synced <- struct{}{}:
	case <-b.stopped:
		return false
	}
	select {
	case <-b.compared:
		return true
	case <-b.stopped:
		return false
	}
}

// bothDone waits for both sides to finish the current case; false means one of them failed.
func (b *outputComparisonCheckpoint) bothDone() bool {
	for range 2 {
		select {
		case <-b.synced:
		case <-b.stopped:
			return false
		}
	}
	return true
}

// release lets both sides move on to the next case.
func (b *outputComparisonCheckpoint) release() {
	for range 2 {
		select {
		case b.compared <- struct{}{}:
		case <-b.stopped:
			return
		}
	}
}

func (b *outputComparisonCheckpoint) stopIfFailed(t *testing.T) {
	if t.Failed() {
		b.stop()
	}
}

// runSync seeds, syncs and tears down one side of a variant on its own config. pick routes each
// sync to a driver version: the reference side always answers the baseline, the upgrade side
// hands stateful syncs to the candidate.
func runSync(
	t *testing.T,
	cfg *testutils.TestConfig,
	group compatibilityGroup,
	syncCase syncCase,
) {
	t.Helper()

	ctx := t.Context()

	if syncCase.operation == "update" && !cfg.SkipSchemaEvolution {
		cfg.ExecuteQuery(ctx, t, cfg, "evolve-schema")
	}
	if syncCase.useState && syncCase.operation != "" {
		cfg.ExecuteQuery(ctx, t, cfg, syncCase.operation)
	}

	// Successive syncs write the same parquet column with different types, which Spark refuses
	// to read together (CANNOT_MERGE_SCHEMAS; F2 in docs/backward-compatibility.md) -- so a
	// parquet variant holds, and compares, only its last case's files.
	if group.destination == "parquet" {
		require.NoErrorf(t, testutils.DeleteParquetFiles(t, cfg.DestinationDB, cfg.GetTableName()), "failed to clear parquet files before %q", syncCase.operation)
	}

	t.Logf("running %s sync on image %s", testutils.Ternary(syncCase.useState, "stateful", "stateless").(string), cfg.GetDriverImage())

	if err := testutils.RunSync(ctx, t, cfg, group.destinationFile, syncCase.useState); err != nil {
		t.Fatal(err)
	}
}

// setIncrementalMode patches the catalog's stream to incremental with the driver's cursor, the
// same edit TestSync's incremental scenarios make.
func setIncrementalMode(cfg *testutils.TestConfig, table string) error {
	streamName := testutils.NormalizeStreamName(cfg.Driver, table)
	return testutils.EditJSONFile(cfg.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
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
			if stream["name"] == streamName && stream["namespace"] == cfg.Namespace {
				stream["sync_mode"] = "incremental"
				if cfg.CursorField != "" {
					stream["cursor_field"] = cfg.CursorField
				}
				return nil
			}
		}
		return fmt.Errorf("stream %s.%s not found in streams.json", cfg.Namespace, streamName)
	})
}

// removeColumnsFromTypeSchema removes columns the seed left out of the table from the stream's type_schema.
func removeColumnsFromTypeSchema(cfg *testutils.TestConfig, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	return testutils.EditJSONFile(cfg.GetFilePath("streams.json"), func(doc map[string]interface{}) error {
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
			schema, ok := stream["type_schema"].(map[string]interface{})
			if !ok {
				return fmt.Errorf("stream %v has no type_schema in streams.json", stream["name"])
			}
			properties, ok := schema["properties"].(map[string]interface{})
			if !ok {
				return fmt.Errorf("stream %v has no type_schema.properties in streams.json", stream["name"])
			}
			for _, column := range columns {
				delete(properties, column)
			}
		}
		return nil
	})
}

// clearDestination drops whatever a previous invocation of this test name left at the variant's
// destination; missing tables and empty prefixes are simply nothing to clear.
func clearDestination(t *testing.T, group compatibilityGroup, db, table string) {
	switch group.destination {
	case "iceberg":
		testutils.DropIcebergTable(t, table, db)
	case "parquet":
		if err := testutils.DeleteParquetTable(t, db, table); err != nil {
			t.Logf("could not clear parquet files at %s/%s (likely absent): %s", db, table, err)
		}
	}
}
