package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

const (
	IcebergCatalog    = "olake_iceberg"
	parquetTestBucket = "warehouse"
	// IP literal, not "localhost": a hostname sends grpc-go through a DNS resolver that stalls
	// every new connection ~20s when the DNS servers are slow (measured 20.22s vs 42ms).
	sparkConnectAddress = "sc://127.0.0.1:15002"

	kafkaRebalanceBulkMessageCount = int64(100_000)
)

// updateStreamConfig sets sync_mode and cursor_field on the stream identified by
// namespace+name in streams[].
func updateStreamConfig(config *testutils.TestConfig, namespace, streamName, syncMode, cursorField string) error {
	// in case of Oracle, the stream names are in uppercase in stream.json
	streamName = testutils.NormalizeStreamName(config.Driver, streamName)
	return testutils.EditJSONFile(config.GetFilePath("test_streams.json"), func(doc map[string]interface{}) error {
		streams, _ := doc["streams"].([]interface{})
		for _, raw := range streams {
			wrapper, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			stream, ok := wrapper["stream"].(map[string]interface{})
			if !ok {
				continue
			}
			if stream["namespace"] == namespace && stream["name"] == streamName {
				stream["sync_mode"] = syncMode
				stream["cursor_field"] = cursorField
			}
		}
		return nil
	})
}

// reset table and add back data to the table
func (cfg *Test) resetTable(ctx context.Context, t *testing.T) error {
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "add")
	if cfg.TestConfig.Driver == string(constants.DB2) {
		// to populate stats for DB2
		cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "populate-stats")
	}
	return nil
}

// newMinIOClient returns a client for the MinIO instance backing the parquet destination in tests.
func newMinIOClient() (*minio.Client, error) {
	client, err := minio.New("localhost:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("admin", "password", ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %s", err)
	}
	return client, nil
}

// listParquetObjects lists the .parquet objects lying directly in a table's folder in MinIO.
func listParquetObjects(ctx context.Context, client *minio.Client, parquetDB, tableName string) ([]minio.ObjectInfo, error) {
	objects := []minio.ObjectInfo{}
	for object := range client.ListObjects(ctx, parquetTestBucket, minio.ListObjectsOptions{
		Prefix:    parquetTablePath(parquetDB, tableName),
		Recursive: false,
	}) {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %s", object.Err)
		}
		if strings.HasSuffix(object.Key, ".parquet") {
			objects = append(objects, object)
		}
	}
	return objects, nil
}

// parquetTablePath is the MinIO key prefix a stream's parquet files are written under.
func parquetTablePath(parquetDB, tableName string) string {
	return fmt.Sprintf("%s/%s/", parquetDB, tableName)
}

// DeleteParquetFiles deletes only .parquet files directly in the table folder in MinIO
func DeleteParquetFiles(t *testing.T, parquetDB, tableName string) error {
	t.Helper()
	parquetPath := parquetTablePath(parquetDB, tableName)

	t.Logf("Cleaning up .parquet files in: s3a://%s/%s", parquetTestBucket, parquetPath)

	minioClient, err := newMinIOClient()
	if err != nil {
		return err
	}

	ctx := t.Context()

	objects, err := listParquetObjects(ctx, minioClient, parquetDB, tableName)
	if err != nil {
		return err
	}

	for _, object := range objects {
		t.Logf("Deleting: %s", strings.TrimPrefix(object.Key, parquetPath))

		if err := minioClient.RemoveObject(ctx, parquetTestBucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("failed to delete %s: %s", object.Key, err)
		}
	}

	t.Logf("--- Cleanup Complete: Deleted %d files ---", len(objects))
	return nil
}

// runSyncAndVerify executes a sync command and verifies the results in Iceberg
func (cfg *Test) runSyncAndVerify(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useState bool,
	destinationType string,
	operation string,
	opSymbol string,
	schema map[string]interface{},
	isCDC bool,
) error {
	if cfg.SyncImage != nil {
		previous := cfg.DriverImage
		cfg.DriverImage = cfg.SyncImage(useState)
		t.Logf("running %s sync on image %s", testutils.Ternary(useState, "stateful", "stateless").(string), cfg.DriverImage)
		defer func() { cfg.DriverImage = previous }()
	}

	cmd := testutils.SyncArgs(*cfg.TestConfig, useState, destinationType, "--destination-database-prefix", cfg.GetDestinationDBPrefix())

	// Execute operation before sync if needed
	if useState && operation != "" {
		cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, operation)
		// SQL Server CDC is asynchronous: the capture job only picks up the DML above on its next
		// transaction-log scan, and the sync's change window ends at the job's processed max LSN
		// (sys.fn_cdc_get_max_lsn), so syncing too early would see no changes. Wait for the capture
		// job to advance past the DML. Incremental runs read the table directly and need no wait.
		if isCDC && cfg.TestConfig.Driver == "mssql" {
			cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "wait-cdc-catchup")
		}
	}

	// Run sync against the driver image
	code, out, err := testutils.RunOlake(ctx, t, cfg.TestConfig, cmd...)
	if err != nil || code != 0 {
		return testutils.SyncFailure(code, err, out)
	}

	t.Logf("Sync successful for %s driver", cfg.TestConfig.Driver)

	if cfg.VerifyDisabled {
		// The sync exiting 0 is still asserted above -- a binary that fails to start is a finding,
		// not noise. Only the expectation-based checks are skipped.
		t.Log("verification disabled for this run; the destination is checked by the cross-run comparison")
		return nil
	}

	// Use evolved schema only for CDC "update" operation (where schema evolution is expected)
	// Incremental "insert" uses opSymbol "u" but doesn't have schema evolution
	evolvedSchema := operation == "update"

	// Verification reads the destination back through Spark Connect (with retries), a real slice of
	// sync wall-clock; time it as its own phase.
	defer testutils.TrackPhaseTiming(t, cfg.TestConfig.Driver, destinationType+" verify")()

	switch destinationType {
	case "iceberg":
		{
			if evolvedSchema {
				VerifyIcebergSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			} else {
				VerifyIcebergSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.PartitionRegex, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			}
		}
	case "parquet":
		{
			if evolvedSchema {
				VerifyParquetSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.UpdatedDestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			} else {
				VerifyParquetSync(t, testTable, cfg.TestConfig.DestinationDB, cfg.DestinationDataTypeSchema, cfg.DefaultCDCColumnsSchema, schema, opSymbol, cfg.TestConfig.Driver, isCDC, cfg.TestConfig.ColumnToExclude)
			}
		}
	}

	return nil
}

func (cfg *Test) IcebergWriter(
	ctx context.Context,
	t *testing.T,
	testTable string,
	useArrowWriter bool,
	testFunc func(context.Context, *testing.T, string) error,
) error {
	// Writer variants are separate config files, so no suite ever edits one in place; SyncArgs
	// hands whichever is named here to --destination.
	file := "iceberg_destination.json"
	if useArrowWriter {
		file = "iceberg_destination_arrow.json"
	}
	cfg.IcebergDestinationFile = file

	return testFunc(ctx, t, testTable)
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

	// The seed rows sit in the CDC log, and before #843 (v0.5.1) the mssql driver captured its
	// initial LSN without waiting for the async capture agent -- a lagging agent puts that LSN
	// before the seed, and the first stateful sync replays the seed rows as CDC inserts
	// (relabeling r to c through the upsert). Wait here so every binary snapshots past the seed.
	if cfg.TestConfig.Driver == "mssql" {
		cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "wait-cdc-catchup")
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
	} else if !cfg.PreserveDestination {
		// Drop the Iceberg table after all tests are finished
		DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
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

	// The seed rows sit in the CDC log, and before #843 (v0.5.1) the mssql driver captured its
	// initial LSN without waiting for the async capture agent -- a lagging agent puts that LSN
	// before the seed, and the first stateful sync replays the seed rows as CDC inserts
	// (relabeling r to c through the upsert). Wait here so every binary snapshots past the seed.
	if cfg.TestConfig.Driver == "mssql" {
		cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, "wait-cdc-catchup")
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
			if err := DeleteParquetFiles(t, cfg.TestConfig.DestinationDB, testTable); err != nil {
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
			if !cfg.PreserveDestination {
				DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
				t.Logf("Dropped Iceberg table: %s", testTable)
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
				false,
			); err != nil {
				t.Fatalf("Incremental test %s failed: %v", tc.name, err)
			}
		})
	}

	t.Log("Iceberg Full load + Incremental tests completed successfully")

	if testutils.KeepTestData() {
		t.Logf("keeping %s source data (%s) is set", cfg.TestConfig.Driver, testutils.KeepTestDataEnvVar)
	} else if !cfg.PreserveDestination {
		DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
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
			if err := DeleteParquetFiles(t, cfg.TestConfig.DestinationDB, testTable); err != nil {
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
	DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
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
	if !cfg.PreserveDestination {
		DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	}
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
	DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)

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
	DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table after 2PC Incremental tests: %s", testTable)
	return nil
}

// WaitForSyncProgress blocks until the running sync has reported its first records in stats.json.
// A driver uses it to time an event at a point where the sync is demonstrably mid-flight, rather
// than guessing with a sleep.
func WaitForSyncProgress(ctx context.Context, t *testing.T, statsPath string) {
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

// runRebalanceSync runs a sync command for the rebalance test.
func (cfg *Test) runRebalanceSync(
	ctx context.Context,
	t *testing.T,
	useState bool,
) error {
	t.Helper()

	cmd := testutils.SyncArgs(*cfg.TestConfig, useState, "iceberg", "--destination-database-prefix", cfg.GetDestinationDBPrefix())

	code, out, err := testutils.RunOlake(ctx, t, cfg.TestConfig, cmd...)
	if err != nil {
		return fmt.Errorf("sync exec error: %w\n%s", err, out)
	}
	if code != 0 {
		return testutils.SyncFailure(code, nil, out)
	}
	t.Logf("sync completed successfully")
	return nil
}

// testKafkaRebalance exercises consumer-group rebalance recovery while syncing a large bulk of messages.
func (cfg *Test) testKafkaRebalance(
	ctx context.Context,
	t *testing.T,
	testTable string,
) error {
	t.Log("Starting Kafka rebalance recovery test")

	DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	if err := testutils.ResetStateFile(cfg.TestConfig); err != nil {
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
			cfg.TestConfig.ExecuteQuery(ctx, t, cfg.TestConfig, tc.operation)

			if err := cfg.runRebalanceSync(ctx, t, tc.useState); err != nil {
				t.Fatalf("%s failed: %v", tc.name, err)
			}
		})
	}

	VerifyIcebergNoDuplicates(ctx, t, testTable, cfg.TestConfig.DestinationDB, "c", kafkaRebalanceBulkMessageCount)

	t.Log("Kafka rebalance recovery test completed successfully")

	DropIcebergTable(t, testTable, cfg.TestConfig.DestinationDB)
	t.Logf("Dropped Iceberg table: %s", testTable)

	return nil
}

// TestRebalance runs the Kafka consumer-group rebalance recovery integration test in an isolated container.
func (cfg *Test) TestRebalance(t *testing.T) {
	if cfg.Suite == "" {
		cfg.Suite = "rebalance"
	} else {
		cfg.Suite += "_rebalance"
	}
	cfg.IsolateSource = true
	cfg.Validate(t)
	cfg.Setup(t)

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
		t.Logf("Enabled normalization and added partition regex in %s", cfg.GetFilePath("test_streams.json"))

		// 3. Run Kafka rebalance recovery test (legacy Iceberg writer)
		if err := cfg.IcebergWriter(ctx, t, testTable, false, cfg.testKafkaRebalance); err != nil {
			t.Fatalf("Kafka rebalance test failed: %v", err)
		}

		// 4. Clean up
		cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
		t.Logf("%s rebalance test cleanup", cfg.Driver)
	})
}

// sparkSession returns the shared Spark Connect session, building it on first use and warming it
// so the one-off server bootstrap is timed here instead of inflating whichever verify runs first.
func SparkSession(ctx context.Context, t *testing.T) (sql.SparkSession, error) {
	sharedSparkOnce.Do(func() {
		// The shared session outlives whichever test builds it, so its construction must not be
		// tied to that test's context (t.Context cancels when the test ends).
		ctx := context.WithoutCancel(ctx)
		defer testutils.TrackPhaseTiming(t, "spark", "session build")()
		for attempt := 1; ; attempt++ {
			sharedSpark, sharedSparkErr = sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
			if sharedSparkErr == nil || attempt == 3 {
				break
			}
			t.Logf("Attempt %d/3: Failed to connect to Spark, retrying in 2s: %v", attempt, sharedSparkErr)
			time.Sleep(2 * time.Second)
		}
		if sharedSparkErr != nil {
			return
		}
		// Spark's vectorized parquet reader mis-decodes DELTA_LENGTH_BYTE_ARRAY columns that hold
		// nulls, reading every value after a null back as "" -- which reads as a data bug in a file
		// the writer got right. Session-scoped, so every query below sees what was actually written.
		if _, err := sharedSpark.Sql(ctx, "SET spark.sql.parquet.enableVectorizedReader=false"); err != nil {
			t.Logf("WARNING: could not disable Spark's vectorized parquet reader, so parquet assertions may report spurious empty strings for nullable byte-array columns: %v", err)
		}
		if _, err := sharedSpark.Sql(ctx, "SELECT 1"); err != nil {
			t.Logf("Spark session warm-up query failed (non-fatal): %v", err)
		}
	})
	return sharedSpark, sharedSparkErr
}

// dropIcebergTable drops an Iceberg table using Spark SQL
func DropIcebergTable(t *testing.T, tableName, icebergDB string) {
	t.Helper()
	ctx := t.Context()
	spark, err := SparkSession(ctx, t)
	if err != nil {
		t.Logf("Failed to connect to Spark Connect server for dropping table: %v", err)
		return
	}

	fullTableName := fmt.Sprintf("%s.%s.%s", IcebergCatalog, icebergDB, tableName)
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName)
	t.Logf("Dropping Iceberg table: %s", dropQuery)

	_, err = spark.Sql(ctx, dropQuery)
	if err != nil {
		t.Logf("Failed to drop Iceberg table %s: %v", fullTableName, err)
		return
	}
	t.Logf("Successfully dropped Iceberg table: %s", fullTableName)
}

// TODO: Refactor parsing logic into a reusable utility functions
// verifyIcebergSync verifies that data was correctly synchronized to Iceberg
func VerifyIcebergSync(t *testing.T, tableName, icebergDB string, datatypeSchema map[string]string, defaultCDCColumnsSchema map[string]string, schema map[string]interface{}, opSymbol, partitionRegex, driver string, isCDC bool, excludedColumn string) {
	t.Helper()
	ctx := t.Context()
	spark, err := SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	fullTableName := fmt.Sprintf("%s.%s.%s", IcebergCatalog, icebergDB, tableName)
	// The shared session caches table snapshots, so refresh to see the rows the sync just committed.
	// Non-fatal: on a first sync the table may not exist yet, which the retry loop below handles.
	if _, refreshErr := spark.Sql(ctx, fmt.Sprintf("REFRESH TABLE %s", fullTableName)); refreshErr != nil {
		t.Logf("REFRESH TABLE before verify (non-fatal): %v", refreshErr)
	}
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	// In kafka, _op_type is always 'c' and col_included appears only in new rows.
	// To check new record, col_included is used.
	if driver == string(constants.Kafka) {
		if _, ok := schema["col_included"]; ok {
			selectQuery += " AND col_included IS NOT NULL"
		}
	}
	t.Logf("Executing query: %s", selectQuery)

	var selectRows []types.Row
	var queryErr error
	maxRetries := 20
	retryDelay := 5 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
		}
		var selectQueryDf sql.DataFrame
		// This is to check if the table exists in destination, as race condition might cause table to not be created yet
		selectQueryDf, queryErr = spark.Sql(ctx, selectQuery)
		if queryErr != nil {
			t.Logf("Query attempt %d failed: %v", attempt+1, queryErr)
			continue
		}

		// To ensure stale data is not being used for verification
		selectRows, queryErr = selectQueryDf.Collect(ctx)
		if queryErr != nil {
			t.Logf("Query attempt %d failed (Collect error): %v", attempt+1, queryErr)
			continue
		}
		if len(selectRows) > 0 {
			queryErr = nil
			break
		}

		// For delete operations, 0 rows is acceptable - exit immediately without retrying
		if opSymbol == "d" {
			queryErr = nil
			t.Logf("Delete verification passed: found 0 rows for _op_type = 'd' (acceptable)")
			break
		}

		// for every type of operation, op symbol will be different, using that to ensure data is not stale
		queryErr = fmt.Errorf("stale data: query succeeded but returned 0 rows for _op_type = '%s'", opSymbol)
		t.Logf("Query attempt %d/%d failed: %v", attempt+1, maxRetries, queryErr)

		// Force Spark to refresh the table metadata from the Iceberg catalog.
		refreshQuery := fmt.Sprintf("REFRESH TABLE %s", fullTableName)
		if _, refreshErr := spark.Sql(ctx, refreshQuery); refreshErr != nil {
			t.Logf("REFRESH TABLE attempt %d failed (non-fatal): %v", attempt+1, refreshErr)
		}
	}

	// For delete operations, accept both 0 and 1 row (both are valid outcomes)
	if opSymbol == "d" {
		if len(selectRows) > 0 {
			deletedID := selectRows[0].Value("_olake_id")
			require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(selectRows))
		return
	}
	require.NoError(t, queryErr, "Failed to collect data rows from Iceberg after %d attempts: %v", maxRetries, queryErr)
	require.NotEmpty(t, selectRows, "No rows returned for _op_type = '%s'", opSymbol)

	for rowIdx, row := range selectRows {
		icebergMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			icebergMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			icebergValue, ok := icebergMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
			require.Equal(t, expected, icebergValue, "Row %d: mismatch on %q: Iceberg has %#v, expected %#v", rowIdx, key, icebergValue, expected)
		}
		if isCDC {
			for key := range defaultCDCColumnsSchema {
				icebergValue, ok := icebergMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
				// Kafka offset, partition can be 0, NotEmpty fails for 0 so we check for NotNil instead.
				if key == "_kafka_offset" || key == "_kafka_partition" {
					require.NotNil(t, icebergValue, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, icebergValue)
				} else {
					require.NotEmpty(t, icebergValue, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, icebergValue)
				}
				if key == constants.CdcTimestamp {
					ts, ok := normalizeToTime(icebergValue)
					require.Truef(t, ok, "Row %d: expected %q to be a timestamp, got %T (%#v)", rowIdx, key, icebergValue, icebergValue)
					minAllowed := time.Now().Add(-1 * time.Hour)
					require.Falsef(t, ts.Before(time.Now().Add(-1*time.Hour)), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}
		if !isCDC && icebergMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(icebergMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, icebergMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
		}
	}
	t.Logf("Verified Iceberg synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s", fullTableName)
	describeDf, err := spark.Sql(ctx, describeQuery)
	require.NoError(t, err, "Failed to describe Iceberg table")

	describeRows, err := describeDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect describe data from Iceberg")
	icebergSchema := make(map[string]string)
	for _, row := range describeRows {
		colName := row.Value("col_name").(string)
		dataType := row.Value("data_type").(string)
		if !strings.HasPrefix(colName, "#") {
			icebergSchema[colName] = dataType
		}
	}

	if excludedColumn != "" {
		_, ok := icebergSchema[testutils.Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Iceberg schema", excludedColumn)
	}

	for col, dbType := range datatypeSchema {
		iceType, found := icebergSchema[col]
		require.True(t, found, "Column %s not found in Iceberg schema", col)

		expectedIceType, mapped := testutils.GlobalTypeMapping[dbType]
		if !mapped {
			t.Errorf("No mapping defined for driver type %s (column %s)", dbType, col)
		}
		require.Equal(t, expectedIceType, iceType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
	}
	t.Logf("Verified datatypes in Iceberg after sync")
	// Verify datatypes for CDC/default columns as well
	if isCDC {
		for col, expectedIceType := range defaultCDCColumnsSchema {
			iceType, found := icebergSchema[col]
			require.True(t, found, "CDC column %s not found in Iceberg schema", col)

			require.Equal(t, expectedIceType, iceType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
		}
		t.Logf("Verified datatypes for CDC columns in Iceberg after sync")
	}

	// Partition verification using only metadata tables
	if partitionRegex == "" {
		t.Log("No partitionRegex provided, skipping partition verification")
		return
	}
	// Extract partition columns from describe rows
	partitionCols := extractFirstPartitionColFromRows(describeRows)
	require.NotEmpty(t, partitionCols, "Partition columns not found in Iceberg metadata")

	// Parse expected partition columns from pattern like "/{col,identity}"
	// Supports multiple entries like "/{col1,identity}" by taking the first token as the source column
	clean := strings.TrimPrefix(partitionRegex, "/{")
	clean = strings.TrimSuffix(clean, "}")
	toks := strings.Split(clean, ",")
	expectedCol := strings.TrimSpace(toks[0])
	require.Equal(t, expectedCol, partitionCols, "Partition column does not match expected '%s'", expectedCol)
	t.Logf("Verified partition column: %s", expectedCol)
}

// VerifyIcebergNoDuplicates asserts that no duplicate _olake_id values exist for the given
// _op_type in the Iceberg table.
func VerifyIcebergNoDuplicates(ctx context.Context, t *testing.T, tableName, icebergDB, opSymbol string, expectedRowCountByOpType int64) {
	t.Helper()

	spark, err := SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server for duplicate check")

	fullTableName := fmt.Sprintf("%s.%s.%s", IcebergCatalog, icebergDB, tableName)

	// Refresh to get the latest committed Iceberg snapshot.
	refreshQuery := fmt.Sprintf("REFRESH TABLE %s", fullTableName)
	if _, refreshErr := spark.Sql(ctx, refreshQuery); refreshErr != nil {
		t.Logf("REFRESH TABLE (non-fatal): %v", refreshErr)
	}

	countQuery := fmt.Sprintf(
		"SELECT COUNT(*) AS total, COUNT(DISTINCT _olake_id) AS distinct_count FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	t.Logf("Executing duplicate-check query: %s", countQuery)

	df, err := spark.Sql(ctx, countQuery)
	require.NoError(t, err, "Failed to run duplicate-check COUNT query")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect duplicate-check COUNT results")
	require.Len(t, rows, 1, "COUNT query must return exactly one row")

	total, ok := rows[0].Value("total").(int64)
	require.True(t, ok, "COUNT(*) value is not int64: %T", rows[0].Value("total"))

	distinct, ok2 := rows[0].Value("distinct_count").(int64)
	require.True(t, ok2, "COUNT(DISTINCT) value is not int64: %T", rows[0].Value("distinct_count"))

	// 1. No duplicates: every row must have a unique _olake_id.
	require.Equal(t, total, distinct,
		"Duplicate rows detected for _op_type='%s': total=%d, distinct=%d. "+
			"Iceberg MERGE INTO did not deduplicate re-synced records.",
		opSymbol, total, distinct)

	// 2. Exact count: when caller specifies an expected row count, enforce it so that both
	//    over-sync (old rows re-processed and inserted again) and under-sync (new rows missed)
	//    are caught.
	if expectedRowCountByOpType > 0 {
		require.Equal(t, expectedRowCountByOpType, distinct,
			"Row count mismatch for _op_type='%s': expected %d distinct rows, got %d. "+
				"Either old rows were re-synced (over-sync) or new rows were missed (under-sync).",
			opSymbol, expectedRowCountByOpType, distinct)
	}

	t.Logf("Duplicate check passed for _op_type='%s': %d rows, all unique by _olake_id (expected %d)",
		opSymbol, distinct, expectedRowCountByOpType)
}

// VerifyParquetSync verifies that data was correctly synchronized to Parquet files in MinIO
func VerifyParquetSync(t *testing.T, tableName, parquetDB string, datatypeSchema map[string]string, defaultCDCColumnsSchema map[string]string, schema map[string]interface{}, opSymbol, driver string, isCDC bool, excludedColumn string) {
	t.Helper()
	ctx := t.Context()

	spark, err := SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	parquetPath := fmt.Sprintf("s3a://warehouse/%s/%s", parquetDB, tableName)
	viewName := fmt.Sprintf("`%s_view_%d`", tableName, time.Now().UnixNano())

	// create a temporary view for parquet files, allows to run describe query
	createViewQuery := fmt.Sprintf(
		"CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM parquet.`%s/*.parquet`",
		viewName, parquetPath,
	)

	// Retry logic for transient Spark connection issues (e.g., catalog connection pool exhaustion)
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err = spark.Sql(ctx, createViewQuery)
		if err == nil {
			break
		}
		// For delete operations, if path doesn't exist that's acceptable (no data written)
		if opSymbol == "d" && strings.Contains(err.Error(), "PATH_NOT_FOUND") {
			t.Logf("Delete verification passed: Parquet path does not exist (no data written)")
			return
		}
		if attempt < maxRetries {
			t.Logf("Attempt %d/%d: Failed to create view, retrying in 2s: %v", attempt, maxRetries, err)
			time.Sleep(2 * time.Second)
		}
	}
	require.NoError(t, err, "Failed to create temporary view for Parquet files")

	defer func() {
		dropViewQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
		t.Logf("Dropping temporary view: %s", dropViewQuery)
		_, _ = spark.Sql(ctx, dropViewQuery)
	}()

	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE `_op_type` = '%s'",
		viewName, opSymbol,
	)
	// In kafka, _op_type is always 'c' and col_included appears only in new rows.
	// To check new record, col_included is used.
	if driver == string(constants.Kafka) {
		if _, ok := schema["col_included"]; ok {
			selectQuery += " AND `col_included` IS NOT NULL"
		}
	}
	t.Logf("Executing Parquet query: %s", selectQuery)

	df, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to run select query on Parquet files")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect rows from Parquet query")

	// For delete operations, accept both 0 and 1 row (both are valid outcomes)
	if opSymbol == "d" {
		if len(rows) > 0 {
			deletedID := rows[0].Value("_olake_id")
			require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(rows))
		return
	}

	// For non-delete operations, require at least one row
	require.NotEmpty(t, rows, "No rows returned for _op_type = '%s'", opSymbol)

	for rowIdx, row := range rows {
		parquetMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			parquetMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			val, ok := parquetMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Parquet result", rowIdx, key)
			require.Equal(t, expected, val,
				"Row %d: mismatch on %q: Parquet has %#v, expected %#v", rowIdx, key, val, expected)
		}
		if isCDC {
			for key := range defaultCDCColumnsSchema {
				val, ok := parquetMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in Parquet result", rowIdx, key)
				// Kafka offset, partition can be 0, NotEmpty fails for 0 so we check for NotNil instead.
				if key == "_kafka_offset" || key == "_kafka_partition" {
					require.NotNil(t, val, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, val)
				} else {
					require.NotEmpty(t, val, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, val)
				}
				if key == constants.CdcTimestamp {
					ts, ok := normalizeToTime(val)
					require.Truef(t, ok, "Row %d: expected %q to be a timestamp, got %T (%#v)", rowIdx, key, val, val)
					minAllowed := time.Now().Add(-1 * time.Hour)
					require.Falsef(t, ts.Before(time.Now().Add(-1*time.Hour)), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}
		if !isCDC && parquetMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(parquetMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, parquetMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
		}
	}

	t.Logf("Verified Parquet synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s", viewName)
	descDF, err := spark.Sql(ctx, describeQuery)
	require.NoError(t, err, "Failed to describe Parquet view")

	descRows, err := descDF.Collect(ctx)
	require.NoError(t, err, "Failed to collect schema info from Parquet view")

	parquetSchema := make(map[string]string)
	for _, row := range descRows {
		colName := row.Value("col_name").(string)
		dataType := row.Value("data_type").(string)
		if !strings.HasPrefix(colName, "#") {
			parquetSchema[colName] = dataType
		}
	}
	if excludedColumn != "" {
		_, ok := parquetSchema[testutils.Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Parquet schema", excludedColumn)
	}

	for col, dbType := range datatypeSchema {
		pqType, found := parquetSchema[col]
		require.True(t, found, "Column %s not found in Parquet schema", col)

		expectedType, mapped := testutils.GlobalTypeMapping[dbType]
		if !mapped {
			t.Errorf("No mapping defined for driver type %s (column %s)", dbType, col)
		}
		require.Equal(t, expectedType, pqType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedType, pqType)
	}
	t.Logf("Verified datatypes in Parquet after sync")
	// Verify datatypes for CDC/default columns as well
	if isCDC {
		for col, expectedPqType := range defaultCDCColumnsSchema {
			pqType, found := parquetSchema[col]
			require.True(t, found, "CDC column %s not found in Parquet schema", col)
			require.Equal(t, expectedPqType, pqType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedPqType, pqType)
		}
	}
	t.Logf("Verified datatypes for CDC columns in Parquet after sync")
}

// extractFirstPartitionColFromRows extracts the first partition column from DESCRIBE EXTENDED rows
func extractFirstPartitionColFromRows(rows []types.Row) string {
	inPartitionSection := false

	for _, row := range rows {
		// Convert []any -> []string
		vals := row.Values()
		parts := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				parts[i] = ""
			} else {
				parts[i] = fmt.Sprint(v) // safe string conversion
			}
		}
		line := strings.TrimSpace(strings.Join(parts, " "))
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "# Partition Information") {
			inPartitionSection = true
			continue
		}

		if inPartitionSection {
			if strings.HasPrefix(line, "# col_name") {
				continue
			}

			if strings.HasPrefix(line, "#") {
				break
			}

			fields := strings.Fields(line)
			if len(fields) > 0 {
				return fields[0] // return the first partition col
			}
		}
	}

	return ""
}

func normalizeToTime(v interface{}) (time.Time, bool) {
	switch ts := v.(type) {
	case time.Time:
		return ts, true
	case arrow.Timestamp:
		return time.Unix(0, int64(ts)*int64(time.Microsecond)).UTC(), true
	default:
		return time.Time{}, false
	}
}

type syncTestCase struct {
	name                     string
	operation                string
	useState                 bool
	opSymbol                 string
	expected                 map[string]interface{}
	preSetup                 []func(*testutils.TestConfig) error // host-side actions executed before the sync
	verifyNoDuplicates       bool                                // if true, assert COUNT(*) == COUNT(DISTINCT _olake_id) after sync
	expectedRowCountByOpType int64                               // when > 0, assert COUNT(DISTINCT _olake_id) == this value (catches over-sync and under-sync)
}

var (
	sharedSparkOnce sync.Once
	sharedSpark     sql.SparkSession
	sharedSparkErr  error
)
