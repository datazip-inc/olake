package integration

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// TestSync runs the happy-path sync suite: full load, CDC and incremental, over both Iceberg writers
// and Parquet. It seeds its catalog from the committed catalog templates instead of discovering one, the way the
// 2PC and rebalance suites do -- TestDiscover already proves the two are identical.
func (th *TestHandler) TestSync(t *testing.T) {
	ctx := t.Context()
	testTable := th.GetTableName()

	th.TestCommonSync(t)

	if (th.Driver == string(constants.Postgres) || th.Driver == string(constants.MySQL)) && th.PrimaryKey == "" && len(th.DedupKeys) == 0 {
		t.Run("Iceberg CDC Primary Key Update", func(t *testing.T) {
			for _, writer := range []struct {
				name     string
				useArrow bool
			}{{"Legacy", false}, {"Arrow", true}} {
				t.Run(writer.name, func(t *testing.T) {
					if err := th.IcebergWriter(ctx, t, testTable, writer.useArrow, th.testCDCPrimaryKeyUpdate); err != nil {
						t.Fatalf("Iceberg CDC primary key update failed: %v", err)
					}
				})
			}
		})
	}

	// Iceberg row-index / delete-mode tests run on the base (legacy) writer config, the one
	// the java writer's table indexer serves; IcebergWriter pins it back after the arrow runs.
	if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(th.Driver)) && hasIcebergTableIndexTest(th.Driver) {
		t.Run("Iceberg Table Index Eq to Pos Conversion", func(t *testing.T) {
			if err := th.IcebergWriter(ctx, t, testTable, false, th.testIcebergEqToPosConversion); err != nil {
				t.Fatalf("Iceberg Table Index Eq to Pos Conversion test failed: %v", err)
			}
		})
		t.Run("Iceberg Table Index Clean Table Positional", func(t *testing.T) {
			if err := th.IcebergWriter(ctx, t, testTable, false, th.testIcebergCleanTablePositionalWithPebbleIndex); err != nil {
				t.Fatalf("Iceberg Table Index Clean Table Positional test failed: %v", err)
			}
		})
		t.Run("Iceberg Table Index Rebuild Index From Scratch", func(t *testing.T) {
			if err := th.IcebergWriter(ctx, t, testTable, false, th.testIcebergRebuildIndexFromScratch); err != nil {
				t.Fatalf("Iceberg Table Index Rebuild Index From Scratch test failed: %v", err)
			}
		})
	}

	t.Run("Azurite", func(t *testing.T) {
		if err := th.testAzureBlob(ctx, t, testTable); err != nil {
			t.Fatalf("Parquet Azurite test failed: %v", err)
		}
	})

	// Asserts the writer splits bulk output into size-bounded files without losing rows. Runs
	// last: it replaces the table contents and clears the catalog's regex/filter config.
	if hasParquetRollingTest(th.Driver) {
		t.Run("Parquet Rolling", func(t *testing.T) {
			if err := th.testParquetRolling(ctx, t, testTable); err != nil {
				t.Fatalf("Parquet Rolling test failed: %v", err)
			}
		})
	}

	// 3. Clean up
	if testutils.KeepTestData() {
		t.Logf("keeping %s source data for Sync as (%s) is set", th.Driver, testutils.KeepTestDataEnvVar)
		return
	}
	th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
	t.Logf("%s sync test cleanup", th.Driver)
}

// testCDCPrimaryKeyUpdate checks both identities, including the retained delete tombstone.
func (th *TestHandler) testCDCPrimaryKeyUpdate(ctx context.Context, t *testing.T, testTable string) error {
	th.resetTable(ctx, t)
	defer th.resetCatalog(t)
	if err := testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, "", "", []string{testTable}, []string{th.ColumnToExclude}); err != nil {
		return fmt.Errorf("failed to clear partition and filter config: %w", err)
	}
	if err := updateStreamConfig(th.TestConfig, th.Namespace, testTable, "cdc", ""); err != nil {
		return err
	}
	if err := setUpdateType(th.TestConfig, th.Namespace, testTable, "eq"); err != nil {
		return err
	}

	testutils.DropIcebergTable(t, testTable, th.DestinationDB)
	if !testutils.KeepTestData() {
		defer testutils.DropIcebergTable(t, testTable, th.DestinationDB)
	}
	if err := th.runIcebergSync(ctx, t, "primary key initial sync", false); err != nil {
		return err
	}

	spark := getSparkSession(ctx, t)
	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, th.DestinationDB, testTable)
	readRows := func() map[string]string {
		refreshTable(ctx, t, spark, fullTableName)
		df, err := spark.Sql(ctx, fmt.Sprintf("SELECT _olake_id, _op_type FROM %s", fullTableName))
		require.NoError(t, err)
		rows, err := df.Collect(ctx)
		require.NoError(t, err)
		result := make(map[string]string, len(rows))
		for _, row := range rows {
			id := fmt.Sprint(row.Value("_olake_id"))
			_, duplicate := result[id]
			require.Falsef(t, duplicate, "duplicate destination identity %s", id)
			result[id] = fmt.Sprint(row.Value("_op_type"))
		}
		return result
	}
	want := map[string]string{"1": "r", "2": "r", "3": "r", "4": "r", "5": "r"}
	// The seed's normally filtered row is present in this unfiltered scenario too.
	if th.Driver == string(constants.Postgres) {
		want["6"] = "r"
	} else {
		want["998"] = "r"
	}
	require.Equal(t, want, readRows())

	th.ExecuteQuery(ctx, t, th.TestConfig, "update_primary_key")
	if err := th.runIcebergSync(ctx, t, "primary key update sync", true); err != nil {
		return err
	}
	want["1"] = "d"
	want["101"] = "u"
	require.Equal(t, want, readRows())

	// Keep source state and destination metadata so a new process exercises normal resume.
	if err := th.runIcebergSync(ctx, t, "primary key repeat sync", true); err != nil {
		return err
	}
	require.Equal(t, want, readRows())
	return nil
}

// TestCommonSync is the boilerplate every sync suite runs: seed the source, point the catalog at
// the handler's settings, then full load, CDC and incremental over both Iceberg writers and Parquet.
// It leaves the source table in place: dropping it afterwards is the caller's job, as TestSync does.
func (th *TestHandler) TestCommonSync(t *testing.T) {
	ctx := t.Context()
	testTable := th.GetTableName()

	// 1. Query on test table; drop first so an aborted run's leftovers cannot survive
	// the CREATE IF NOT EXISTS
	th.ExecuteQuery(ctx, t, th.TestConfig, "drop")
	th.ExecuteQuery(ctx, t, th.TestConfig, "create")
	th.ExecuteQuery(ctx, t, th.TestConfig, "clean")
	th.ExecuteQuery(ctx, t, th.TestConfig, testutils.Ternary(len(th.DedupKeys) > 0, testutils.Ternary(th.UpsertAddOp != "", th.UpsertAddOp, "upsert_add"), "add").(string))

	// 2. Give the catalog this suite's own destination namespace, then enable normalization,
	// partition regex, filter and column exclusion in the catalog
	th.resetCatalog(t)
	t.Logf("Enabled normalization and added partition regex in %s", th.Catalog())

	writerTypes := []struct {
		name     string
		useArrow bool
	}{
		{"Legacy", false},
		{"Arrow", true},
	}

	// Skip cdc tests for drivers not supporting cdc mode
	if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(th.Driver)) {
		for _, wt := range writerTypes {
			t.Run(fmt.Sprintf("Iceberg (%s) Full load + CDC tests", wt.name), func(t *testing.T) {
				if err := th.IcebergWriter(ctx, t, testTable, wt.useArrow, th.IcebergFullLoadAndCDC); err != nil {
					t.Fatalf("Iceberg (%s) Full load + CDC tests failed: %v", wt.name, err)
				}
			})
		}

		t.Run("Parquet Full load + CDC tests", func(t *testing.T) {
			if err := th.ParquetFullLoadAndCDC(ctx, t, testTable); err != nil {
				t.Fatalf("Parquet Full load + CDC tests failed: %v", err)
			}
		})
	}

	// Skip incremental tests for drivers not supporting incremental mode
	if th.Driver != string(constants.Kafka) {
		for _, wt := range writerTypes {
			t.Run(fmt.Sprintf("Iceberg (%s) Full load + Incremental tests", wt.name), func(t *testing.T) {
				if err := th.IcebergWriter(ctx, t, testTable, wt.useArrow, th.IcebergFullLoadAndIncremental); err != nil {
					t.Fatalf("Iceberg (%s) Full load + Incremental tests failed: %v", wt.name, err)
				}
			})
		}

		t.Run("Parquet Full load + Incremental tests", func(t *testing.T) {
			if err := th.ParquetFullLoadAndIncremental(ctx, t, testTable); err != nil {
				t.Fatalf("Parquet Full load + Incremental tests failed: %v", err)
			}
		})
	}
}

// IcebergFullLoadAndCDC tests Full load and CDC operations
func (th *TestHandler) IcebergFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg Full load + CDC tests")

	th.resetTable(ctx, t)

	dbTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  th.ExpectedData,
		},
		{
			name:      "CDC - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "c",
			expected:  th.ExpectedData,
		},
		{
			name:      "CDC - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  th.ExpectedUpdatedData,
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
			expected:  th.ExpectedData,
		},
		{
			name:      "CDC - strict - update",
			operation: "update",
			useState:  true,
			opSymbol:  "c",
			expected:  th.ExpectedUpdatedData,
		},
	}

	testCases := testutils.Ternary(th.TestConfig.Driver == string(constants.Kafka), kafkaTestCases, dbTestCases).([]syncTestCase)
	if len(th.DedupKeys) > 0 {
		updateOp := "upsert_update"
		if th.UpsertUpdateOp != "" {
			updateOp = th.UpsertUpdateOp
		}
		testCases = []syncTestCase{
			{
				name:      "CDC - strict - insert",
				operation: "",
				useState:  false,
				opSymbol:  "c",
				expected:  th.ExpectedData,
			},
			{
				name:      "CDC - strict - update",
				operation: updateOp,
				useState:  true,
				opSymbol:  "u",
				expected:  th.ExpectedUpdatedData,
			},
		}
		if !th.SkipTombstone {
			testCases = append(testCases, syncTestCase{
				name:      "CDC - strict - delete",
				operation: "upsert_tombstone",
				useState:  true,
				opSymbol:  "d",
				expected:  nil,
			})
		}
	}

	// Run each test case. t.Fatalf below ends only its own subtest, so stop the loop explicitly:
	// every case after the first failure is a stateful sync built on state the failed one never
	// wrote, and it costs a full sync each to learn nothing.
	for _, tc := range testCases {
		if passed := t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if !th.TestConfig.SkipSchemaEvolution {
					th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "evolve-schema")
				}
			}

			if err := th.runSyncAndVerify(
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
		}); !passed {
			t.Logf("stopping this scenario after %q failed; the remaining cases depend on the state it did not write", tc.name)
			break
		}
	}

	t.Log("Iceberg Full load + CDC tests completed successfully")

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data (%s) is set", th.TestConfig.Driver, testutils.KeepTestDataEnvVar)
		return nil
	}
	// Drop the Iceberg table after all tests are finished
	testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// IcebergFullLoadAndCDC tests Full load and CDC operations
func (th *TestHandler) ParquetFullLoadAndCDC(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Parquet Full load + CDC tests")

	th.resetTable(ctx, t)
	if err := testutils.DeleteParquetTable(t, th.TestConfig.DestinationDB, testTable); err != nil {
		return fmt.Errorf("failed to reset parquet table: %s", err)
	}

	dbTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  th.ExpectedData,
		},
		{
			name:      "CDC - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "c",
			expected:  th.ExpectedData,
		},
		{
			name:      "CDC - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  th.ExpectedUpdatedData,
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
			expected:  th.ExpectedData,
		},
		{
			name:      "CDC - strict - update",
			operation: "update",
			useState:  true,
			opSymbol:  "c",
			expected:  th.ExpectedUpdatedData,
		},
	}

	testCases := testutils.Ternary(th.TestConfig.Driver == string(constants.Kafka), kafkaTestCases, dbTestCases).([]syncTestCase)
	if len(th.DedupKeys) > 0 {
		updateOp := "upsert_update"
		if th.UpsertUpdateOp != "" {
			updateOp = th.UpsertUpdateOp
		}
		testCases = []syncTestCase{
			{
				name:      "CDC - strict - insert",
				operation: "",
				useState:  false,
				opSymbol:  "u",
				expected:  th.ExpectedData,
			},
			{
				name:      "CDC - strict - update",
				operation: updateOp,
				useState:  true,
				opSymbol:  "u",
				expected:  th.ExpectedUpdatedData,
			},
		}
		if !th.SkipTombstone {
			testCases = append(testCases, syncTestCase{
				name:      "CDC - strict - delete",
				operation: "upsert_tombstone",
				useState:  true,
				opSymbol:  "d",
				expected:  nil,
			})
		}
	}

	// Run each test case, stopping at the first failure -- see the same loop in
	// IcebergFullLoadAndCDC for why continuing only burns syncs.
	for _, tc := range testCases {
		if passed := t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if !th.TestConfig.SkipSchemaEvolution {
					th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "evolve-schema")
				}
			}

			if err := testutils.DeleteParquetFiles(t, th.TestConfig.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := th.runSyncAndVerify(
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
		}); !passed {
			t.Logf("stopping this scenario after %q failed; the remaining cases depend on the state it did not write", tc.name)
			break
		}
	}

	t.Log("Parquet Full load + CDC tests completed successfully")
	return nil
}

// TODO: add incremntal test for string time, timestamp with timezone, datetime, float, int as cursor field
// IcebergFullLoadAndIncremental tests Full load and Incremental operations
func (th *TestHandler) IcebergFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg Full load + Incremental tests")

	th.resetTable(ctx, t)

	// Patch the catalog: set sync_mode = incremental, cursor_field = "id"
	if err := updateStreamConfig(th.TestConfig, th.TestConfig.Namespace, testTable, "incremental", th.TestConfig.CursorField); err != nil {
		return fmt.Errorf("failed to patch the catalog for incremental: %s", err)
	}

	// Test cases for incremental sync
	incrementalTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  th.ExpectedData,
		},
		{
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  th.ExpectedData,
		},
		{
			name:      "Incremental - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  th.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case, stopping at the first failure -- see the same loop in
	// IcebergFullLoadAndCDC for why continuing only burns syncs.
	for _, tc := range incrementalTestCases {
		if passed := t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if !th.TestConfig.SkipSchemaEvolution {
					th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "evolve-schema")
				}
			}

			// drop iceberg table before sync
			testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)
			t.Logf("Dropped Iceberg table: %s", testTable)

			if err := th.runSyncAndVerify(
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
		}); !passed {
			t.Logf("stopping this scenario after %q failed; the remaining cases depend on the state it did not write", tc.name)
			break
		}
	}

	t.Log("Iceberg Full load + Incremental tests completed successfully")

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data (%s) is set", th.TestConfig.Driver, testutils.KeepTestDataEnvVar)
		return nil
	}
	testutils.DropIcebergTable(t, testTable, th.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// ParquetFullLoadAndIncremental tests Full load and Incremental operations for Parquet
func (th *TestHandler) ParquetFullLoadAndIncremental(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Parquet Full load + Incremental tests")

	th.resetTable(ctx, t)
	if err := testutils.DeleteParquetTable(t, th.TestConfig.DestinationDB, testTable); err != nil {
		return fmt.Errorf("failed to reset parquet table: %s", err)
	}

	// Patch the catalog: set sync_mode = incremental, cursor_field = "id"
	if err := updateStreamConfig(th.TestConfig, th.TestConfig.Namespace, testTable, "incremental", th.TestConfig.CursorField); err != nil {
		return fmt.Errorf("failed to patch the catalog for incremental: %s", err)
	}

	// Test cases for incremental sync
	incrementalTestCases := []syncTestCase{
		{
			name:      "Full-Refresh",
			operation: "",
			useState:  false,
			opSymbol:  "r",
			expected:  th.ExpectedData,
		},
		{
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  th.ExpectedData,
		},
		{
			name:      "Incremental - update",
			operation: "update",
			useState:  true,
			opSymbol:  "u",
			expected:  th.ExpectedUpdatedData,
		},
	}

	// Run each incremental test case, stopping at the first failure -- see the same loop in
	// IcebergFullLoadAndCDC for why continuing only burns syncs.
	for _, tc := range incrementalTestCases {
		if passed := t.Run(tc.name, func(t *testing.T) {
			// schema evolution
			if tc.operation == "update" {
				if !th.TestConfig.SkipSchemaEvolution {
					th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "evolve-schema")
				}
			}

			if err := testutils.DeleteParquetFiles(t, th.TestConfig.DestinationDB, testTable); err != nil {
				t.Fatalf("Failed to delete parquet files before %s: %v", tc.name, err)
			}

			if err := th.runSyncAndVerify(
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
		}); !passed {
			t.Logf("stopping this scenario after %q failed; the remaining cases depend on the state it did not write", tc.name)
			break
		}
	}

	t.Log("Parquet Full load + Incremental tests completed successfully")
	return nil
}
