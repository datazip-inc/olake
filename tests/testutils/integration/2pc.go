package integration

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// Test2PCIntegration runs the full Two-Phase Commit (2PC) failure-recovery integration test
// suite against the driver image. It exercises CDC and incremental state-recovery scenarios
// independently of the happy-path integration tests, allowing them to be scheduled and
// reported separately.
func (cfg *Test) Test2PCIntegration(t *testing.T) {
	ctx := t.Context()

	currentTestTable := cfg.GetTableName()

	t.Run("Sync", func(t *testing.T) {
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "add")

		if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{currentTestTable}, cfg.ColumnToExclude); err != nil {
			t.Fatalf("failed to enable normalization and partition regex in streams.json: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", "test_stream.json")

		writerTypes := []struct {
			name     string
			useArrow bool
		}{
			{"Legacy", false},
			{"Arrow", true},
		}

		if !slices.Contains(constants.SkipCDCDrivers, constants.DriverType(cfg.TestConfig.Driver)) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC CDC Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.IcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.Iceberg2PCCDCRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC CDC Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if cfg.TestConfig.Driver != string(constants.Kafka) {
			for _, wt := range writerTypes {
				t.Run(fmt.Sprintf("Iceberg (%s) 2PC Incremental Recovery tests", wt.name), func(t *testing.T) {
					if err := cfg.IcebergWriter(ctx, t, currentTestTable, wt.useArrow, cfg.Iceberg2PCIncrementalRecovery); err != nil {
						t.Fatalf("Iceberg (%s) 2PC Incremental Recovery tests failed: %v", wt.name, err)
					}
				})
			}
		}

		if testutils.KeepTestData() {
			t.Logf("keeping %s 2PC sync test data (%s) is set", cfg.TestConfig.Driver, testutils.KeepTestDataEnvVar)
		} else {
			cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
			t.Logf("%s 2PC sync test cleanup", cfg.TestConfig.Driver)
		}
	})
}

// Iceberg2PCCDCRecovery tests 2PC (Two-Phase Commit) failure recovery for CDC mode using
// the Iceberg destination. It simulates a state-save failure mid-sync: saves a pre-insert
// checkpoint, performs a CDC insert, then restores to the checkpoint and inserts a second
// record (insert_2pc) to verify the driver correctly recovers without duplicating rows.
func (cfg *Test) Iceberg2PCCDCRecovery(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg 2PC CDC Recovery tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Drop the Iceberg table and reset state before the first sync, so stale rows and the
	// olake_2pc table property left by a previous run can't leak into this run's recovery timeline.
	testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state: %w", err)
	}

	twoPCCDCTestCases := []syncTestCase{
		{
			name:                     testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), "CDC - initial load", "Full-Refresh").(string),
			operation:                "",
			useState:                 false,
			opSymbol:                 testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), "c", "r").(string),
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: 5,
		},
		{
			name:                     "CDC - insert",
			operation:                testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), "add", "insert").(string),
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			preSetup:                 testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), []func(*testutils.TestConfig) error{}, []func(*testutils.TestConfig) error{testutils.SaveStateFile}).([]func(*testutils.TestConfig) error),
			verifyNoDuplicates:       cfg.TestConfig.Driver == string(constants.Kafka),
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
			expectedRowCountByOpType: int64(testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), 11, 1).(int)),
			preSetup:                 testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), []func(*testutils.TestConfig) error{}, []func(*testutils.TestConfig) error{testutils.RestoreStateFile}).([]func(*testutils.TestConfig) error),
		},
		{
			// After the recovery sync advanced state to the committed metadata LSN,
			// a normal sync should see both the original insert and insert_2pc rows.
			name:                     "CDC - Post Recovery Sync",
			useState:                 true,
			opSymbol:                 "c",
			expected:                 cfg.ExpectedData,
			verifyNoDuplicates:       true,
			expectedRowCountByOpType: int64(testutils.Ternary(cfg.TestConfig.Driver == string(constants.Kafka), 12, 2).(int)),
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
				ctx, t, testTable, tc.useState, "iceberg",
				tc.operation, tc.opSymbol, tc.expected,
				tc.name != "Full-Refresh",
			); err != nil {
				t.Fatalf("%s test failed: %v", tc.name, err)
			}

			if tc.verifyNoDuplicates {
				VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.TestConfig.DestinationDB, tc.opSymbol, tc.expectedRowCountByOpType)
			}
		})
	}

	t.Log("Iceberg 2PC CDC Recovery tests completed successfully")
	testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC CDC tests: %s", testTable)
	return nil
}

// Iceberg2PCIncrementalRecovery tests 2PC (Two-Phase Commit) failure recovery for
// incremental mode using the Iceberg destination. It simulates a state-save failure after
// the cursor advances: saves a pre-insert checkpoint, performs an incremental insert, then
// restores to the checkpoint and inserts a second record (insert_2pc) to verify that the
// cursor re-reads the overlapping range, deduplicates the original insert via MERGE INTO,
// and correctly surfaces only the net-new insert_2pc row.
func (cfg *Test) Iceberg2PCIncrementalRecovery(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Iceberg 2PC Incremental Recovery tests")

	if err := cfg.resetTable(ctx, t); err != nil {
		return fmt.Errorf("failed to reset table: %w", err)
	}

	// Drop the Iceberg table before the first sync, so stale rows and the olake_2pc table
	// property left by a previous run can't leak into this run's recovery timeline.
	testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)

	// Patch streams.json: set sync_mode = incremental, cursor_field
	if err := updateStreamConfig(cfg.TestConfig, cfg.TestConfig.Namespace, testTable, "incremental", cfg.TestConfig.CursorField); err != nil {
		return fmt.Errorf("failed to patch streams.json for incremental: %s", err)
	}

	// Reset state so initial incremental behaves like a first full incremental load
	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
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
			preSetup: []func(*testutils.TestConfig) error{
				testutils.SaveStateFile,
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
			preSetup: []func(*testutils.TestConfig) error{
				testutils.RestoreStateFile,
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
				ctx, t, testTable, tc.useState, "iceberg",
				tc.operation, tc.opSymbol, tc.expected,
				false,
			); err != nil {
				t.Fatalf("Incremental 2PC test %s failed: %v", tc.name, err)
			}

			if tc.verifyNoDuplicates {
				VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.TestConfig.DestinationDB, tc.opSymbol, tc.expectedRowCountByOpType)
			}
		})
	}

	t.Log("Iceberg 2PC Incremental Recovery tests completed successfully")
	testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC Incremental tests: %s", testTable)
	return nil
}
