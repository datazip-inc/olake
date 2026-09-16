package kafka

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/integration"
)

// testRebalance drives the consumer-group rebalance recovery test: the bulk topic is synced
// twice while a rival consumer takes partitions away and gives them back, and the destination must
// still hold every message exactly once.
func testRebalance(t *testing.T, cfg *integration.TestHandler) {
	ctx := t.Context()
	testTable := cfg.GetTableName()

	t.Run("Sync", func(t *testing.T) {
		// 1. Query on test table
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")

		// 2. Give the catalog this suite's own destination namespace, then enable normalization,
		// partition regex, filter and column exclusion in the catalog
		if err := cfg.IsolateDestinationDB(); err != nil {
			t.Fatalf("failed to isolate the destination database in the catalog: %s", err)
		}
		if err := testutils.UpdateSelectedStreams(cfg.TestConfig, cfg.Namespace, cfg.PartitionRegex, cfg.FilterConfig, []string{testTable}, []string{cfg.ColumnToExclude}); err != nil {
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
func rebalanceRecovery(ctx context.Context, t *testing.T, cfg *integration.TestHandler, testTable string) error {
	t.Log("Starting Kafka rebalance recovery test")

	testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)

	rebalanceTestCases := []struct {
		name      string
		operation string
		useState  bool
	}{
		{name: "CDC - first rebalance sync", operation: "insert_rebalance", useState: false},
		// Stop the trigger consumer before resuming so it cannot hold partition assignments.
		{name: "CDC - second rebalance sync", operation: "stop_rebalance", useState: true},
	}

	for _, tc := range rebalanceTestCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg.ExecuteQuery(ctx, t, cfg.TestConfig, tc.operation)

			if err := runRebalanceSync(ctx, t, cfg, tc.useState); err != nil {
				t.Fatalf("%s failed: %v", tc.name, err)
			}
		})
	}

	integration.VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.TestConfig.DestinationDB, "c", rebalanceBulkMessageCount)

	t.Log("Kafka rebalance recovery test completed successfully")

	testutils.DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// runRebalanceSync runs one sync of the bulk topic.
func runRebalanceSync(ctx context.Context, t *testing.T, cfg *integration.TestHandler, useState bool) error {
	t.Helper()

	if err := testutils.RunSync(ctx, t, cfg.TestConfig, cfg.IcebergDestinationFile(), useState); err != nil {
		return err
	}
	t.Logf("sync completed successfully")
	return nil
}
