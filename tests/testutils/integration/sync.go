package integration

import (
	"fmt"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// TestSync runs the happy-path sync suite: full load, CDC and incremental, over both Iceberg writers
// and Parquet. It seeds its catalog from test_streams.json instead of discovering one, the way the
// 2PC and rebalance suites do -- TestDiscover already proves the two are identical.
func (cfg *Test) TestSync(t *testing.T) {
	if cfg.Suite == "" {
		cfg.Suite = "sync"
	} else {
		cfg.Suite += "_sync"
	}
	cfg.IsolateSource = true
	cfg.Validate(t)
	cfg.Setup(t)

	ctx := t.Context()
	testTable := cfg.GetTableName()

	// 1. Query on test table; drop first so an aborted run's leftovers cannot survive
	// the CREATE IF NOT EXISTS
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "add")

	// 2. Enable normalization, partition regex, filter and column exclusion in streams.json
	if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, cfg.ColumnToExclude); err != nil {
		t.Fatalf("failed to enable normalization and partition regex in streams.json: %s", err)
	}
	t.Logf("Enabled normalization and added partition regex in %s", cfg.GetFilePath("test_streams.json"))

	writerTypes := []struct {
		name     string
		useArrow bool
	}{
		{"Legacy", false},
		{"Arrow", true},
	}

	// Skip cdc tests for drivers not supporting cdc mode
	if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(cfg.Driver)) {
		for _, wt := range writerTypes {
			t.Run(fmt.Sprintf("Iceberg (%s) Full load + CDC tests", wt.name), func(t *testing.T) {
				if err := cfg.IcebergWriter(ctx, t, testTable, wt.useArrow, cfg.IcebergFullLoadAndCDC); err != nil {
					t.Fatalf("Iceberg (%s) Full load + CDC tests failed: %v", wt.name, err)
				}
			})
		}

		t.Run("Parquet Full load + CDC tests", func(t *testing.T) {
			if err := cfg.ParquetFullLoadAndCDC(ctx, t, testTable); err != nil {
				t.Fatalf("Parquet Full load + CDC tests failed: %v", err)
			}
		})
	}

	// Skip incremental tests for drivers not supporting incremental mode
	if cfg.Driver != string(constants.Kafka) {
		for _, wt := range writerTypes {
			t.Run(fmt.Sprintf("Iceberg (%s) Full load + Incremental tests", wt.name), func(t *testing.T) {
				if err := cfg.IcebergWriter(ctx, t, testTable, wt.useArrow, cfg.IcebergFullLoadAndIncremental); err != nil {
					t.Fatalf("Iceberg (%s) Full load + Incremental tests failed: %v", wt.name, err)
				}
			})
		}

		t.Run("Parquet Full load + Incremental tests", func(t *testing.T) {
			if err := cfg.ParquetFullLoadAndIncremental(ctx, t, testTable); err != nil {
				t.Fatalf("Parquet Full load + Incremental tests failed: %v", err)
			}
		})
	}

	// Asserts the writer splits bulk output into size-bounded files without losing rows. Runs
	// last: it replaces the table contents and clears streams.json's regex/filter config.
	if hasParquetRollingTest(cfg.Driver) {
		t.Run("Parquet Rolling", func(t *testing.T) {
			if err := cfg.testParquetRolling(ctx, t, testTable); err != nil {
				t.Fatalf("Parquet Rolling test failed: %v", err)
			}
		})
	}

	// 3. Clean up
	if testutils.KeepTestData() {
		t.Logf("keeping %s source data for Sync as (%s) is set", cfg.Driver, testutils.KeepTestDataEnvVar)
	} else {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		t.Logf("%s sync test cleanup", cfg.Driver)
	}
}
