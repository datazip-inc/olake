package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// waitForSyncProgress blocks until the running sync has reported its first records in stats.json,
// which is how the rebalance trigger joins the group at a point where the sync is demonstrably
// mid-flight rather than at a guessed offset into a sleep.
func waitForSyncProgress(ctx context.Context, t *testing.T, statsPath string) {
	t.Helper()

	require.Eventually(t, func() bool {
		if ctx.Err() != nil {
			return true
		}

		var stats struct {
			SyncedRecords int64 `json:"Synced Records"`
		}
		if err := testutils.UnmarshalFile(statsPath, &stats, false); err != nil {
			return false
		}
		if stats.SyncedRecords > 0 {
			t.Logf("sync started: %d records synced", stats.SyncedRecords)
			return true
		}
		return false
	}, testutils.SyncTimeout, time.Second)
}

// runRebalanceSuite drives the consumer-group rebalance recovery test: the bulk topic is synced
// twice while a rival consumer takes partitions away and gives them back, and the destination must
// still hold every message exactly once.
func runRebalanceSuite(t *testing.T, cfg *integration.Test) {
	ctx := t.Context()
	testTable := cfg.GetTableName()

	t.Run("Sync", func(t *testing.T) {
		// 1. Query on test table
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")

		// 2. Enable normalization, partition regex, filter and column exclusion in the catalog
		if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, cfg.ColumnToExclude); err != nil {
			t.Fatalf("failed to enable normalization and partition regex in the catalog: %s", err)
		}
		t.Logf("Enabled normalization and added partition regex in %s", cfg.GetFilePath("streams.json"))

		// 3. Run the recovery test against the legacy Iceberg writer
		recoverFn := func(ctx context.Context, t *testing.T, testTable string) error {
			return rebalanceRecovery(ctx, t, cfg, testTable)
		}
		if err := cfg.IcebergWriter(ctx, t, testTable, false, recoverFn); err != nil {
			t.Fatalf("Kafka rebalance test failed: %v", err)
		}

		// 4. Clean up
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		t.Logf("%s rebalance test cleanup", cfg.Driver)
	})
}

// rebalanceRecovery syncs the bulk topic across a rebalance and asserts the destination holds each
// message once: a consumer that resumes from the wrong offset shows up here as duplicates.
func rebalanceRecovery(ctx context.Context, t *testing.T, cfg *integration.Test, testTable string) error {
	t.Log("Starting Kafka rebalance recovery test")

	integration.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
		return fmt.Errorf("failed to reset state file: %s", err)
	}

	rebalanceTestCases := []struct {
		name      string
		operation string
	}{
		{name: "CDC - first rebalance sync", operation: "insert_rebalance"},
		// Stop the trigger consumer before resuming so it cannot hold partition assignments.
		{name: "CDC - second rebalance sync", operation: "stop_rebalance"},
	}

	for _, tc := range rebalanceTestCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, tc.operation)

			if err := runRebalanceSync(ctx, t, cfg); err != nil {
				t.Fatalf("%s failed: %v", tc.name, err)
			}
		})
	}

	integration.VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.TestConfig.DestinationDB, "c", rebalanceBulkMessageCount)

	t.Log("Kafka rebalance recovery test completed successfully")

	integration.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// runRebalanceSync runs one stateful sync of the bulk topic.
func runRebalanceSync(ctx context.Context, t *testing.T, cfg *integration.Test) error {
	t.Helper()

	cmd := testutils.SyncArgs(true, cfg.IcebergDestinationFile(), "--destination-database-prefix", cfg.UniqueID())

	code, out, err := testutils.RunOlake(ctx, cfg.TestConfig, cmd...)
	if err != nil {
		return fmt.Errorf("sync exec error: %w\n%s", err, out)
	}
	if code != 0 {
		return testutils.RenderOlakeFailure(code, nil, out)
	}
	t.Logf("sync completed successfully")
	return nil
}
