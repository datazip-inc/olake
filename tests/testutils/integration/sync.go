package integration

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// TestSync runs the happy-path sync suite: full load, CDC and incremental, over both Iceberg writers
// and Parquet. It seeds its catalog from streams.template.json instead of discovering one, the way the
// 2PC and rebalance suites do -- TestDiscover already proves the two are identical.
func (cfg *Test) TestSync(t *testing.T) {
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
	t.Logf("Enabled normalization and added partition regex in %s", cfg.GetFilePath("streams.json"))

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

// IcebergFullLoadAndCDC tests Full load and CDC operations
func (cfg *Test) IcebergFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg Full load + CDC tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	dbTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
		{
			name:      "CDC - delete",
			operation: "delete",
			useState:  true,
			opSymbol:  "d",
			expected:  nil,
		},
	}

	kafkaTestCases := []syncTestCase{
		{
			name:      "CDC - strict - insert",
			operation: "",
			useState:  false,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - strict - update",
			operation: "update",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	testCases := testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), kafkaTestCases, dbTestCases).([]syncTestCase)

	// Run each test case. t.Fatalf below ends only its own subtest, so stop the loop explicitly:
	// every case after the first failure is a stateful sync built on state the failed one never
	// wrote, and it costs a full sync each to learn nothing.
	for _, tc := range testCases {
		if !t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != "mongodb" && cfg.TestConfig.Driver != "mssql" && cfg.TestConfig.Driver != "kafka" {
					cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"iceberg",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		}) {
			t.Logf("stopping this scenario after %q failed; the remaining cases depend on the state it did not write", tc.name)
			break
		}
	}

	t.Log("Iceberg Full load + CDC tests completed successfully")

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data (%s) is set", cfg.TestConfig.Driver, testutils.KeepTestDataEnvVar)
	} else {
		// Drop the Iceberg table after all tests are finished
		testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
		t.Logf("Dropped Iceberg table: %s", testTable)
	}

	return nil
}

// IcebergFullLoadAndCDC tests Full load and CDC operations
func (cfg *Test) ParquetFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Parquet Full load + CDC tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %s", err)
	}
	if err := testutils.DeleteParquetTable(t, cfg.TestConfig.DestinationDB, testTable); err != nil {
		return fmt.Errorf("failed to reset parquet table: %s", err)
	}

	dbTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
		{
			name:      "CDC - delete",
			operation: "delete",
			useState:  true,
			opSymbol:  "d",
			expected:  nil,
		},
	}

	kafkaTestCases := []syncTestCase{
		{
			name:      "CDC - strict - insert",
			operation: "",
			useState:  false,
			opSymbol:  "c",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "CDC - strict - update",
			operation: "update",
			useState:  true,
			opSymbol:  "c",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	testCases := testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), kafkaTestCases, dbTestCases).([]syncTestCase)

	// Run each test case, stopping at the first failure -- see the same loop in
	// IcebergFullLoadAndCDC for why continuing only burns syncs.
	for _, tc := range testCases {
		if !t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != "mongodb" && cfg.TestConfig.Driver != "mssql" && cfg.TestConfig.Driver != "kafka" {
					cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			// Delete parquet files before next operation to avoid error due to schema changes.
			// Kept even for the compatibility suite (unlike the Iceberg drops), because the files are
			// genuinely unreadable together: successive syncs write the same column with different
			// types -- measured on postgres, col_float4 FLOAT then DOUBLE and col_int INT then
			// BIGINT -- so Spark rejects the directory with CANNOT_MERGE_SCHEMAS, mergeSchema or
			// not. That is F2 in docs/backward-compatibility.md (parquet has no schema evolution;
			// the break surfaces in the reader). The consequence for compatibility is that a parquet
			// variant compares only its LAST case's output; see compareVariant.
			if err := testutils.DeleteParquetFiles(t, cfg.TestConfig.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"parquet",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		}) {
			t.Logf("stopping this scenario after %q failed; the remaining cases depend on the state it did not write", tc.name)
			break
		}
	}

	t.Log("Parquet Full load + CDC tests completed successfully")
	return nil
}

// TODO: add incremntal test for string time, timestamp with timezone, datetime, float, int as cursor field
// IcebergFullLoadAndIncremental tests Full load and Incremental operations
func (cfg *Test) IcebergFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg Full load + Incremental tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %s", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field = "id"
	if err := updateStreamConfig(cfg.TestConfig, cfg.TestConfig.Namespace, testTable, "incremental", cfg.TestConfig.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
	}

	// Test cases for incremental sync
	incrementalTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case
	for _, tc := range incrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != string(constants.MongoDB) && cfg.TestConfig.Driver != "mssql" {
					cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			// drop iceberg table before sync
			testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
			t.Logf("Dropped Iceberg table: %s", testTable)

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"iceberg",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Iceberg Full load + Incremental tests completed successfully")

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data (%s) is set", cfg.TestConfig.Driver, testutils.KeepTestDataEnvVar)
	} else {
		testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
		t.Logf("Dropped Iceberg table: %s", testTable)
	}

	return nil
}

// ParquetFullLoadAndIncremental tests Full load and Incremental operations for Parquet
func (cfg *Test) ParquetFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Parquet Full load + Incremental tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %s", err)
	}
	if err := testutils.DeleteParquetTable(t, cfg.TestConfig.DestinationDB, testTable); err != nil {
		return fmt.Errorf("failed to reset parquet table: %s", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field = "id"
	if err := updateStreamConfig(cfg.TestConfig, cfg.TestConfig.Namespace, testTable, "incremental", cfg.TestConfig.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
	}

	// Test cases for incremental sync
	incrementalTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedData,
		},
		{
			name:      "Incremental - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case
	for _, tc := range incrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if cfg.TestConfig.Driver != string(constants.MongoDB) && cfg.TestConfig.Driver != "mssql" {
					cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "evolve-schema")
				}
			}

			// Delete parquet files before next operation to avoid error due to schema changes.
			// Kept even for the compatibility suite (unlike the Iceberg drops), because the files are
			// genuinely unreadable together: successive syncs write the same column with different
			// types -- measured on postgres, col_float4 FLOAT then DOUBLE and col_int INT then
			// BIGINT -- so Spark rejects the directory with CANNOT_MERGE_SCHEMAS, mergeSchema or
			// not. That is F2 in docs/backward-compatibility.md (parquet has no schema evolution;
			// the break surfaces in the reader). The consequence for compatibility is that a parquet
			// variant compares only its LAST case's output; see compareVariant.
			if err := testutils.DeleteParquetFiles(t, cfg.TestConfig.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				testTable,
				tc.useState,
				"parquet",
				tc.operation,
				tc.opSymbol,
				tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Parquet Full load + Incremental tests completed successfully")
	return nil
}
