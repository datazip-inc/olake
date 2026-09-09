package compatibility

import (
	"context"
	"fmt"
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

// syncCase is one sync of a scenario: the DML that precedes it and whether it reads state.
type syncCase struct {
	operation string
	useState  bool
}

// scenarioCases is the same case sequence TestSync runs, minus its verification: the comparison
// against the reference run is this suite's only assertion.
func scenarioCases(driver, kind string) []syncCase {
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

// runVariantSide seeds, syncs and tears down one side of a variant on its own config. pick routes each
// sync to a driver version: the reference side always answers the baseline, the upgrade side
// hands stateful syncs to the candidate.
func runVariantSide(
	t *testing.T,
	cfg *testutils.TestConfig,
	group compatibilityGroup,
	v compatibilityVariant,
	pick func(useState bool) string,
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
	require.NoError(t, testutils.UpdateSelectedStreams(cfg, cfg.Namespace, cfg.PartitionRegex, "", []string{table}, policies.catalogExcluded),
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

	if testutils.KeepTestData() {
		t.Logf("compatibility side %q: leaving source table %s in place (%s is set); it holds the last case's data",
			cfg.Suite, table, testutils.KeepTestDataEnvVar)
	} else {
		defer cfg.ExecuteQuery(ctx, t, cfg, "drop")
	}

	// Seed the source: the same reset every TestSync scenario starts from.
	cfg.ExecuteQuery(ctx, t, cfg, "drop")
	cfg.ExecuteQuery(ctx, t, cfg, "create")
	cfg.ExecuteQuery(ctx, t, cfg, "add")

	for _, c := range scenarioCases(cfg.Driver, v.kind) {
		if c.operation == "update" && !cfg.SkipSchemaEvolution {
			cfg.ExecuteQuery(ctx, t, cfg, "evolve-schema")
		}
		if c.useState && c.operation != "" {
			cfg.ExecuteQuery(ctx, t, cfg, c.operation)
		}
		// Successive syncs write the same parquet column with different types, which Spark refuses
		// to read together (CANNOT_MERGE_SCHEMAS; F2 in docs/backward-compatibility.md) -- so a
		// parquet variant holds, and compares, only its last case's files.
		if group.destination == "parquet" {
			require.NoErrorf(t, testutils.DeleteParquetFiles(t, cfg.DestinationDB, table), "failed to clear parquet files before %q", c.operation)
		}
		runSync(ctx, t, cfg, group.destinationFile, pick(c.useState), c.useState)
	}
}

// runSync runs one sync of the scenario on the image of the given driver version.
func runSync(ctx context.Context, t *testing.T, cfg *testutils.TestConfig, destinationFile, version string, useState bool) {
	t.Helper()
	cfg.DriverVersion = version
	t.Logf("running %s sync on image %s", testutils.Ternary(useState, "stateful", "stateless").(string), cfg.GetDriverImage())

	if err := testutils.RunSync(ctx, t, cfg, destinationFile, useState); err != nil {
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
		if err := testutils.DeleteParquetFiles(t, db, table); err != nil {
			t.Logf("could not clear parquet files at %s/%s (likely absent): %s", db, table, err)
		}
	}
}
