package testutils

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// to get backfill streams from cdc streams e.g. "demo_cdc" -> "demo"
func GetBackfillStreamsFromCDC(cdcStreams []string) []string {
	backfillStreams := []string{}
	for _, stream := range cdcStreams {
		backfillStreams = append(backfillStreams, strings.TrimSuffix(stream, "_cdc"))
	}
	return backfillStreams
}

// testTableName returns the source table used by this driver's tests.
func (cfg *IntegrationTest) testTableName() string {
	if cfg.TestConfig.DataFormat == "" {
		return fmt.Sprintf("%s_test_table_olake", cfg.TestConfig.Driver)
	}
	return fmt.Sprintf("%s_%s_test_table_olake", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat)
}

// reset table and add back data to the table
func (cfg *IntegrationTest) resetTable(ctx context.Context, t *testing.T, testTable string) error {
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "drop", false)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "create", false)
	cfg.ExecuteQuery(ctx, t, []string{testTable}, "add", false)
	if cfg.TestConfig.Driver == "db2" {
		// to populate stats for DB2
		cfg.ExecuteQuery(ctx, t, []string{testTable}, "populate-stats", false)
	}
	return nil
}

// syncTestCase represents a test case for sync operations
type syncTestCase struct {
	name                     string
	operation                string
	useState                 bool
	opSymbol                 string
	expected                 map[string]interface{}
	preSetup                 []func(*TestConfig) error // host-side actions executed before the sync
	verifyNoDuplicates       bool                      // if true, assert COUNT(*) == COUNT(DISTINCT _olake_id) after sync
	expectedRowCountByOpType int64                     // when > 0, assert COUNT(DISTINCT _olake_id) == this value (catches over-sync and under-sync)
}

// runSyncAndVerify executes a sync command and verifies the results in Iceberg
func (cfg *IntegrationTest) runSyncAndVerify(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
	useState bool,
	destinationType string,
	operation string,
	opSymbol string,
	schema map[string]interface{},
	isCDC bool,
) error {
	destDBPrefix := ternary(cfg.TestConfig.DataFormat != "", fmt.Sprintf("integration_%s_%s", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat), fmt.Sprintf("integration_%s", cfg.TestConfig.Driver))
	cmd := syncCommand(*cfg.TestConfig, useState, destinationType, "--destination-database-prefix", destDBPrefix)

	// Execute operation before sync if needed
	if useState && operation != "" {
		cfg.ExecuteQuery(ctx, t, []string{testTable}, operation, false)
		// SQL Server CDC is asynchronous: the capture job only picks up the DML above on its next
		// transaction-log scan, and the sync's change window ends at the job's processed max LSN
		// (sys.fn_cdc_get_max_lsn), so syncing too early would see no changes. Wait for the capture
		// job to advance past the DML. Incremental runs read the table directly and need no wait.
		if isCDC && cfg.TestConfig.Driver == "mssql" {
			cfg.ExecuteQuery(ctx, t, []string{testTable}, "wait-cdc-catchup", false)
		}
	}

	// Run sync command
	code, out, err := ExecCommand(ctx, c, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("sync failed (%d): %s\n%s", code, err, out)
	}

	logBuildRunTimings(t, cfg.TestConfig.Driver, destinationType+" sync", out)
	t.Logf("Sync successful for %s driver", cfg.TestConfig.Driver)

	// Use evolved schema only for CDC "update" operation (where schema evolution is expected)
	// Incremental "insert" uses opSymbol "u" but doesn't have schema evolution
	evolvedSchema := operation == "update"

	// Verification reads the destination back through Spark Connect (with retries), a real slice of
	// sync wall-clock; time it as its own phase.
	defer trackPhaseTiming(t, cfg.TestConfig.Driver, destinationType+" verify")()

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, testTable, cfg.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			} else {
				VerifyIcebergSync(t, testTable, cfg.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			}
		}
	case "parquet":
		{
			if evolvedSchema {
				VerifyParquetSync(t, testTable, cfg.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			} else {
				VerifyParquetSync(t, testTable, cfg.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.ColumnToExclude)
			}
		}
	}

	return nil
}

func (cfg *IntegrationTest) testIcebergWriter(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
	useArrowWriter bool,
	testFunc func(context.Context, *testing.T, testcontainers.Container, string) error,
) error {
	if err := toggleArrowIcebergWrites(cfg.TestConfig, useArrowWriter); err != nil {
		return fmt.Errorf("failed to toggle arrow_writes: %s", err)
	}

	return testFunc(ctx, t, c, testTable)
}

// destination captures how a full-load suite writes to an Iceberg or Parquet target and
// how that target's state is reset around each sync. Iceberg CDC accumulates rows and
// drops the table once at the end; Iceberg incremental verifies each op as a fresh full
// load, so it drops the table before every sync; Parquet always clears stale files first
// (it has no in-place merge / schema evolution).
type destination struct {
	name string // "iceberg" | "parquet"
	// prepareBeforeSync, when set, runs before every sync in the suite.
	prepareBeforeSync func(t *testing.T, destinationDB, testTable string)
	// cleanupAfterSuite, when set, runs once after all cases complete.
	cleanupAfterSuite func(t *testing.T, destinationDB, testTable string)
}

func dropIcebergDestination(t *testing.T, destinationDB, testTable string) {
	dropIcebergTable(t, testTable, destinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)
}

func clearParquetDestination(t *testing.T, destinationDB, testTable string) {
	if err := DeleteParquetFiles(t, destinationDB, testTable); err != nil {
		t.Fatalf("Failed to delete parquet files: %v", err)
	}
}

var (
	icebergCDCDestination         = destination{name: "iceberg", cleanupAfterSuite: dropIcebergDestination}
	icebergIncrementalDestination = destination{name: "iceberg", prepareBeforeSync: dropIcebergDestination}
	parquetDestination            = destination{name: "parquet", prepareBeforeSync: clearParquetDestination}
)

// runFullLoadSuite drives a sequence of sync operations against one destination, resetting
// the source table (and running any suite setup) first, then executing each case with the
// destination's per-sync and end-of-suite hooks. When suiteIsCDC is true every case except
// Full-Refresh is verified as a CDC change.
func (cfg *IntegrationTest) runFullLoadSuite(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
	suiteName string,
	dest destination,
	cases []syncTestCase,
	suiteIsCDC bool,
	setup func() error,
) error {
	t.Logf("Starting %s tests", suiteName)

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	if setup != nil {
		if err := setup(); err != nil {
			return err
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// schema evolution (kafka and the schemaless drivers do not evolve here)
			if tc.operation == "update" && cfg.TestConfig.Driver != "mongodb" && cfg.TestConfig.Driver != "mssql" && cfg.TestConfig.Driver != "kafka" {
				cfg.ExecuteQuery(ctx, t, []string{testTable}, "evolve-schema", false)
			}

			if dest.prepareBeforeSync != nil {
				dest.prepareBeforeSync(t, cfg.DestinationDB, testTable)
			}

			if err := cfg.runSyncAndVerify(
				ctx,
				t,
				c,
				testTable,
				tc.useState,
				dest.name,
				tc.operation,
				tc.opSymbol,
				tc.expected,
				suiteIsCDC && tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}
		})
	}

	t.Logf("%s tests completed successfully", suiteName)

	if dest.cleanupAfterSuite != nil {
		dest.cleanupAfterSuite(t, cfg.DestinationDB, testTable)
	}

	return nil
}

// fullLoadCDCCases returns the Full-load + CDC operation sequence for this driver.
func (cfg *IntegrationTest) fullLoadCDCCases() []syncTestCase {
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

	return ternary(cfg.TestConfig.Driver == "kafka", kafkaTestCases, dbTestCases)
}

// TODO: add incremntal test for string time, timestamp with timezone, datetime, float, int as cursor field
// incrementalCases returns the Full-load + Incremental operation sequence.
func (cfg *IntegrationTest) incrementalCases() []syncTestCase {
	return []syncTestCase{
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
}

// setupIncremental switches the selected stream to incremental mode and clears state so the
// first sync behaves like an initial full load.
func (cfg *IntegrationTest) setupIncremental(testTable string) error {
	if err := updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
	}
	return nil
}

func (cfg *IntegrationTest) testIcebergFullLoadAndCDC(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	return cfg.runFullLoadSuite(ctx, t, c, testTable, "Iceberg Full load + CDC", icebergCDCDestination, cfg.fullLoadCDCCases(), true, nil)
}

func (cfg *IntegrationTest) testParquetFullLoadAndCDC(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	return cfg.runFullLoadSuite(ctx, t, c, testTable, "Parquet Full load + CDC", parquetDestination, cfg.fullLoadCDCCases(), true, nil)
}

func (cfg *IntegrationTest) testIcebergFullLoadAndIncremental(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	return cfg.runFullLoadSuite(ctx, t, c, testTable, "Iceberg Full load + Incremental", icebergIncrementalDestination, cfg.incrementalCases(), false, func() error {
		return cfg.setupIncremental(testTable)
	})
}

func (cfg *IntegrationTest) testParquetFullLoadAndIncremental(ctx context.Context, t *testing.T, c testcontainers.Container, testTable string) error {
	return cfg.runFullLoadSuite(ctx, t, c, testTable, "Parquet Full load + Incremental", parquetDestination, cfg.incrementalCases(), false, func() error {
		return cfg.setupIncremental(testTable)
	})
}

// testIceberg2PCCDCRecovery tests 2PC (Two-Phase Commit) failure recovery for CDC mode using
// the Iceberg destination. It simulates a state-save failure mid-sync: saves a pre-insert
// checkpoint, performs a CDC insert, then restores to the checkpoint and inserts a second
// record (insert_2pc) to verify the driver correctly recovers without duplicating rows.
func (cfg *IntegrationTest) testIceberg2PCCDCRecovery(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Iceberg 2PC CDC Recovery tests")

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	twoPCCDCTestCases := []syncTestCase{
		{
			name:                     ternary(cfg.TestConfig.Driver == "kafka", "CDC - initial load", "Full-Refresh"),
			operation:                "",
			useState:                 false,
			opSymbol:                 ternary(cfg.TestConfig.Driver == "kafka", "c", "r"),
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 5,
		},
		{
			name:                     "CDC - insert",
			operation:                ternary(cfg.TestConfig.Driver == "kafka", "add", "insert"),
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			preSetup:                 ternary(cfg.TestConfig.Driver == "kafka", []func(*TestConfig) error{}, []func(*TestConfig) error{saveStateFile}),
			verifyNoDuplicates:       cfg.TestConfig.Driver == "kafka",
			expectedRowCountByOpType: 10,
		},
		{
			// Simulate 2PC failure: restore state to pre-insert checkpoint, insert a
			// second record, run sync. The driver recovers: it advances state to the
			// committed metadata LSN by making a bounded sync.
			// expectedRowCountByOpType=1 because no new data lands in Iceberg here,
			// as it just recovers the sync from state -> metadata LSN.
			name:                     "CDC - Recovery Sync",
			operation:                "insert_2pc",
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: int64(ternary(cfg.TestConfig.Driver == "kafka", 11, 1)),
			preSetup:                 ternary(cfg.TestConfig.Driver == "kafka", []func(*TestConfig) error{}, []func(*TestConfig) error{restoreStateFile}),
		},
		{
			// After the recovery sync advanced state to the committed metadata LSN,
			// a normal sync should see both the original insert and insert_2pc rows.
			name:                     "CDC - Post Recovery Sync",
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: int64(ternary(cfg.TestConfig.Driver == "kafka", 12, 2)),
		},
	}

	for _, tc := range twoPCCDCTestCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, preSetup := range tc.preSetup {
				if err := preSetup(cfg.TestConfig); err != nil {
					t.Fatalf("%s pre-sync setup failed: %v", tc.name, err)
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx, t, c, testTable, tc.useState, "iceberg",
				tc.operation, tc.opSymbol, tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}

			if tc.verifyNoDuplicates {
				VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.DestinationDB, tc.opSymbol, tc.expectedRowCountByOpType)
			}
		})
	}

	t.Log("Iceberg 2PC CDC Recovery tests completed successfully")
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC CDC tests: %s", testTable)
	return nil
}

// testIceberg2PCIncrementalRecovery tests 2PC (Two-Phase Commit) failure recovery for
// incremental mode using the Iceberg destination. It simulates a state-save failure after
// the cursor advances: saves a pre-insert checkpoint, performs an incremental insert, then
// restores to the checkpoint and inserts a second record (insert_2pc) to verify that the
// cursor re-reads the overlapping range, deduplicates the original insert via MERGE INTO,
// and correctly surfaces only the net-new insert_2pc row.
func (cfg *IntegrationTest) testIceberg2PCIncrementalRecovery(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Iceberg 2PC Incremental Recovery tests")

	if err := cfg.resetTable(ctx, t, testTable); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Patch streams.json: set sync_mode = incremental, cursor_field
	if err := updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "incremental", cfg.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state for incremental: %s", err)
	}

	twoPCIncrementalTestCases := []syncTestCase{
		{
			name:                     "Full-Refresh",
			operation:                "",
			useState:                 false,
			opSymbol:                 "r",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 5,
		},
		{
			name:      "Incremental - insert",
			operation: "insert",
			useState:  true,
			opSymbol:  "u",
			expected:  cfg.ExpectedData,
			preSetup: []func(*TestConfig) error{
				saveStateFile,
			},
		},
		{
			// Simulate 2PC failure: restore cursor to pre-insert checkpoint, insert a
			// second record, run sync. The cursor re-reads the range and deduplicates
			// the original insert via MERGE INTO; insert_2pc is net-new.
			// expectedRowCountByOpType=1: only insert_2pc is visible (original deduplicated).
			name:                     "Incremental - State Save Failure Sync",
			operation:                "insert_2pc",
			useState:                 true,
			opSymbol:                 "u",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 1,
			preSetup: []func(*TestConfig) error{
				restoreStateFile,
			},
		},
		{
			// After recovery, state is now consistent. A normal sync should see both
			// the original insert row and insert_2pc row — 2 distinct records total.
			name:                     "Incremental - Post Recovery Sync",
			useState:                 true,
			opSymbol:                 "u",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 2, // insert row + insert_2pc row, both unique by _olake_id
		},
	}

	for _, tc := range twoPCIncrementalTestCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, preSetup := range tc.preSetup {
				if err := preSetup(cfg.TestConfig); err != nil {
					t.Fatalf("%s pre-sync setup failed: %v", tc.name, err)
				}
			}

			if err := cfg.runSyncAndVerify(
				ctx, t, c, testTable, tc.useState, "iceberg",
				tc.operation, tc.opSymbol, tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental 2PC test %s failed: %v", tc.name, err)
			}

			if tc.verifyNoDuplicates {
				VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.DestinationDB, tc.opSymbol, tc.expectedRowCountByOpType)
			}
		})
	}

	t.Log("Iceberg 2PC Incremental Recovery tests completed successfully")
	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC Incremental tests: %s", testTable)
	return nil
}

// TestDiscover runs the driver's discover command against the seeded source table and
// validates the generated streams.json against the expected test_streams.json.
func (cfg *IntegrationTest) TestDiscover(t *testing.T) {
	ctx := context.Background()
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := cfg.testTableName()

	runInTestContainer(ctx, t, cfg.TestConfig, func(ctx context.Context, c testcontainers.Container) error {
		// 1. Query on test table
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "create", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "clean", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "add", false)

		// 2. Run discover command
		discoverCmd := discoverCommand(*cfg.TestConfig)
		code, out, err := ExecCommand(ctx, c, discoverCmd)
		if err != nil || code != 0 {
			return fmt.Errorf("discover failed (%d): %s\n%s", code, err, string(out))
		}
		logBuildRunTimings(t, cfg.TestConfig.Driver, "discover", out)

		// 3. Verify streams.json file
		streamsJSON, err := os.ReadFile(cfg.TestConfig.HostTestCatalogPath)
		if err != nil {
			return fmt.Errorf("failed to read expected streams JSON: %s", err)
		}
		testStreamsJSON, err := os.ReadFile(cfg.TestConfig.HostCatalogPath)
		if err != nil {
			return fmt.Errorf("failed to read actual streams JSON: %s", err)
		}
		if !normalizedEqual(string(streamsJSON), string(testStreamsJSON)) {
			return fmt.Errorf("streams.json does not match expected test_streams.json\nExpected:\n%s\nGot:\n%s", string(streamsJSON), string(testStreamsJSON))
		}
		t.Logf("Generated streams validated with test streams")

		// 4. Clean up
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
		t.Logf("%s discover test-container clean up", cfg.TestConfig.Driver)
		return nil
	})
}

// TestSync runs the full-load/CDC/incremental sync suites against the Iceberg and Parquet
// destinations. It seeds streams.json from test_streams.json, so it does not depend on
// TestDiscover having run first.
func (cfg *IntegrationTest) TestSync(t *testing.T) {
	ctx := context.Background()
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := cfg.testTableName()

	seedCatalog(t, cfg.TestConfig)

	runInTestContainer(ctx, t, cfg.TestConfig, func(ctx context.Context, c testcontainers.Container) error {
		// 1. Query on test table
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "create", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "clean", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "add", false)

		// 2. Enable normalization, partition regex, filter and column exclusion in streams.json
		if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			return fmt.Errorf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.HostCatalogPath)

		writerTypes := []struct {
			name     string
			useArrow bool
		}{
			{"Legacy", false},
			{"Arrow", true},
		}

		// Skip cdc tests for drivers not supporting cdc mode
		if !slices.Contains(skipCDCDrivers, cfg.TestConfig.Driver) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) Full load + CDC tests", wt.name), func(t *testing.T) {
					if err := cfg.testIcebergWriter(ctx, t, c, currentTestTable, wt.useArrow, cfg.testIcebergFullLoadAndCDC); err != nil {
						t.Fatalf("Iceberg (%s) Full load + CDC tests failed: %v", wt.name, err)
					}
				})
			}

			t.Run("Parquet Full load + CDC tests", func(t *testing.T) {
				if err := cfg.testParquetFullLoadAndCDC(ctx, t, c, currentTestTable); err != nil {
					t.Fatalf("Parquet Full load + CDC tests failed: %v", err)
				}
			})
		}

		// Skip incremental tests for drivers not supporting incremental mode
		if cfg.TestConfig.Driver != "kafka" {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) Full load + Incremental tests", wt.name), func(t *testing.T) {
					if err := cfg.testIcebergWriter(ctx, t, c, currentTestTable, wt.useArrow, cfg.testIcebergFullLoadAndIncremental); err != nil {
						t.Fatalf("Iceberg (%s) Full load + Incremental tests failed: %v", wt.name, err)
					}
				})
			}

			t.Run("Parquet Full load + Incremental tests", func(t *testing.T) {
				if err := cfg.testParquetFullLoadAndIncremental(ctx, t, c, currentTestTable); err != nil {
					t.Fatalf("Parquet Full load + Incremental tests failed: %v", err)
				}
			})
		}

		// 3. Clean up
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
		t.Logf("%s sync test-container clean up", cfg.TestConfig.Driver)
		return nil
	})
}

// Test2PCIntegration runs the full Two-Phase Commit (2PC) failure-recovery integration test
// suite in an isolated container. It exercises CDC and incremental state-recovery scenarios
// independently of the happy-path integration tests, allowing them to be scheduled and
// reported separately.
func (cfg *IntegrationTest) Test2PCIntegration(t *testing.T) {
	ctx := context.Background()
	cfg.ExecuteQuery = timedExecuteQuery(cfg.TestConfig.Driver, cfg.ExecuteQuery)

	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := cfg.testTableName()

	// 2PC tests don't need schema discovery — the schema is already validated by the regular integration test.
	seedCatalog(t, cfg.TestConfig)

	runInTestContainer(ctx, t, cfg.TestConfig, func(ctx context.Context, c testcontainers.Container) error {
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "create", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "clean", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "add", false)

		if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			return fmt.Errorf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.HostCatalogPath)

		writerTypes := []struct {
			name     string
			useArrow bool
		}{
			{"Legacy", false},
			{"Arrow", true},
		}

		if !slices.Contains(skipCDCDrivers, cfg.TestConfig.Driver) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC CDC Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.testIcebergWriter(ctx, t, c, currentTestTable, wt.useArrow, cfg.testIceberg2PCCDCRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC CDC Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if cfg.TestConfig.Driver != "kafka" {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC Incremental Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.testIcebergWriter(ctx, t, c, currentTestTable, wt.useArrow, cfg.testIceberg2PCIncrementalRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC Incremental Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
		t.Logf("%s 2PC sync test-container clean up", cfg.TestConfig.Driver)
		return nil
	})
}

// runRebalanceSync runs a sync command for the rebalance test.
func (cfg *IntegrationTest) runRebalanceSync(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	useState bool,
) error {
	t.Helper()

	destDBPrefix := fmt.Sprintf("integration_%s_%s", cfg.TestConfig.Driver, cfg.TestConfig.DataFormat)
	cmd := syncCommand(*cfg.TestConfig, useState, "iceberg", "--destination-database-prefix", destDBPrefix)

	code, out, err := ExecCommand(ctx, c, cmd)
	if err != nil {
		return fmt.Errorf("sync exec error: %w\n%s", err, out)
	}
	if code != 0 {
		return fmt.Errorf("sync failed (%d): %s", code, out)
	}
	t.Logf("sync completed successfully")
	return nil
}

// testKafkaRebalance exercises consumer-group rebalance recovery while syncing a large bulk of messages.
func (cfg *IntegrationTest) testKafkaRebalance(
	ctx context.Context,
	t *testing.T,
	c testcontainers.Container,
	testTable string,
) error {
	t.Log("Starting Kafka rebalance recovery test")

	dropIcebergTable(t, testTable, cfg.DestinationDB)
	if err := resetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state file: %s", err)
	}

	rebalanceTestCases := []syncTestCase{
		{
			name:      "CDC - first rebalance sync",
			operation: "insert_rebalance",
			useState:  true,
		},
		{
			// Stop the trigger consumer before resuming so it cannot hold partition assignments.
			name:      "CDC - second rebalance sync",
			operation: "stop_rebalance",
			useState:  true,
		},
	}

	for _, tc := range rebalanceTestCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg.ExecuteQuery(ctx, t, []string{testTable}, tc.operation, false)

			if err := cfg.runRebalanceSync(ctx, t, c, tc.useState); err != nil {
				t.Fatalf("%s failed: %v", tc.name, err)
			}
		})
	}

	VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.DestinationDB, "c", kafkaRebalanceBulkMessageCount)

	t.Log("Kafka rebalance recovery test completed successfully")

	dropIcebergTable(t, testTable, cfg.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// TestRebalance runs the Kafka consumer-group rebalance recovery integration test in an isolated container.
func (cfg *IntegrationTest) TestRebalance(t *testing.T) {
	ctx := context.Background()

	t.Logf("Test data directory: %s", cfg.TestConfig.HostTestDataPath)
	currentTestTable := cfg.testTableName()

	seedCatalog(t, cfg.TestConfig)

	runInTestContainer(ctx, t, cfg.TestConfig, func(ctx context.Context, c testcontainers.Container) error {
		// 1. Query on test table
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "create", false)
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "clean", false)

		// 2. Enable normalization and partition regex in streams.json
		if err := updateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			return fmt.Errorf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", cfg.TestConfig.HostCatalogPath)

		// 3. Run Kafka rebalance recovery test (legacy Iceberg writer)
		if err := cfg.testIcebergWriter(ctx, t, c, currentTestTable, false, cfg.testKafkaRebalance); err != nil {
			t.Fatalf("Kafka rebalance test failed: %v", err)
		}

		// 4. Clean up
		cfg.ExecuteQuery(ctx, t, []string{currentTestTable}, "drop", false)
		t.Logf("%s rebalance test-container clean up", cfg.TestConfig.Driver)
		return nil
	})
}
