package abstract

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTablePrefix       = "test_table_olake"
	sparkConnectAddress   = "sc://localhost:15002"
	icebergDatabase       = "olake_iceberg"
	cdcInitializationWait = 2 * time.Second
	cdcProcessingWait     = 60 * time.Second
)

// TODO: redesign integration tests according to new structure
var currentTestTable = fmt.Sprintf("%s_%d", testTablePrefix, time.Now().Unix())

type ExecuteQuery func(ctx context.Context, t *testing.T, conn interface{}, tableName string, operation string)

// TestSetup tests the driver setup and connection check
func (a *AbstractDriver) TestSetup(t *testing.T) {
	t.Helper()
	require.NoError(t, a.Setup(context.Background()), "Connection check failed")
}

// TestDiscover tests the discovery of tables
func (a *AbstractDriver) TestDiscover(t *testing.T, conn interface{}, execQuery ExecuteQuery) {
	t.Helper()
	ctx := context.Background()

	// Setup and cleanup test table
	execQuery(ctx, t, conn, currentTestTable, "create")
	defer execQuery(ctx, t, conn, currentTestTable, "drop")
	execQuery(ctx, t, conn, currentTestTable, "clean")
	execQuery(ctx, t, conn, currentTestTable, "add")

	streams, err := a.Discover(ctx)
	require.NoError(t, err, "Discover failed")
	require.NotEmpty(t, streams, "No streams found")

	found := false
	for _, stream := range streams {
		if stream.Name == currentTestTable {
			found = true
			break
		}
	}
	assert.True(t, found, "Unable to find test table %s", currentTestTable)
}

// TestRead tests full refresh and CDC read operations
func (a *AbstractDriver) TestRead(t *testing.T, conn interface{}, execQuery ExecuteQuery) {
	t.Helper()
	ctx := context.Background()

	// Setup table and initial data
	execQuery(ctx, t, conn, currentTestTable, "create")
	defer execQuery(ctx, t, conn, currentTestTable, "drop")
	execQuery(ctx, t, conn, currentTestTable, "clean")
	execQuery(ctx, t, conn, currentTestTable, "add")

	// Initialize writer pool
	pool := setupWriterPool(ctx, t)

	// Define test cases
	testCases := []struct {
		name          string
		syncMode      types.SyncMode
		operation     string
		expectedCount string
	}{
		{
			name:          "full refresh read",
			syncMode:      types.FULLREFRESH,
			operation:     "",
			expectedCount: "5",
		},
		{
			name:          "cdc read - insert operation",
			syncMode:      types.CDC,
			operation:     "insert",
			expectedCount: "6",
		},
		{
			name:          "cdc read - update operation",
			syncMode:      types.CDC,
			operation:     "update",
			expectedCount: "6",
		},
		{
			name:          "cdc read - delete operation",
			syncMode:      types.CDC,
			operation:     "delete",
			expectedCount: "6",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testStream := a.getTestStream(t, currentTestTable)
			configuredStream := &types.ConfiguredStream{Stream: testStream}
			configuredStream.Stream.SyncMode = tc.syncMode

			if tc.syncMode == types.CDC {
				// Execute the operation for CDC tests
				execQuery(ctx, t, conn, currentTestTable, tc.operation)

				// Wait for CDC initialization
				time.Sleep(cdcInitializationWait)

				require.NoError(t,
					a.Read(ctx, pool, []types.StreamInterface{}, []types.StreamInterface{configuredStream}),
					"CDC read operation failed",
				)

				// Wait for CDC to process
				time.Sleep(cdcProcessingWait)
			} else {
				// Handle full refresh read
				require.NoError(t,
					a.Read(ctx, pool, []types.StreamInterface{configuredStream}, []types.StreamInterface{}),
					"Read operation failed",
				)
				time.Sleep(cdcProcessingWait)
			}

			verifyIcebergSync(t, currentTestTable, tc.expectedCount)
		})
	}
}

// setupWriterPool initializes and returns a new writer pool
func setupWriterPool(ctx context.Context, t *testing.T) *destination.WriterPool {
	t.Helper()

	// Register Parquet writer
	destination.RegisteredWriters[types.Parquet] = func() destination.Writer {
		return &iceberg.Iceberg{}
	}

	pool, err := destination.NewWriter(ctx, &types.WriterConfig{
		Type: "ICEBERG",
		WriterConfig: map[string]any{
			"catalog_type":    "jdbc",
			"jdbc_url":        "jdbc:postgresql://localhost:5432/iceberg",
			"jdbc_username":   "iceberg",
			"jdbc_password":   "password",
			"normalization":   false,
			"iceberg_s3_path": "s3a://warehouse",
			"s3_endpoint":     "http://localhost:9000",
			"s3_use_ssl":      false,
			"s3_path_style":   true,
			"aws_access_key":  "admin",
			"aws_region":      "us-east-1",
			"aws_secret_key":  "password",
			"iceberg_db":      icebergDatabase,
		},
	}, nil)
	require.NoError(t, err, "Failed to create writer pool")

	return pool
}

// getTestStream retrieves the test stream by table name
func (a *AbstractDriver) getTestStream(t *testing.T, tableName string) *types.Stream {
	t.Helper()

	streams, err := a.Discover(context.Background())
	require.NoError(t, err, "Discover failed")
	require.NotEmpty(t, streams, "No streams found")

	for _, stream := range streams {
		if stream.Name == tableName {
			return stream
		}
	}

	require.Fail(t, "Could not find stream for table %s", tableName)
	return nil
}

// verifyIcebergSync verifies that data was correctly synchronized to Iceberg
func verifyIcebergSync(t *testing.T, tableName string, expectedCount string) {
	t.Helper()
	ctx := context.Background()

	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Errorf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	query := fmt.Sprintf(
		"SELECT COUNT(DISTINCT _olake_id) as unique_count FROM %s.%s.%s",
		icebergDatabase, icebergDatabase, tableName,
	)
	t.Logf("Executing query: %s", query)

	countDf, err := spark.Sql(ctx, query)
	require.NoError(t, err, "Failed to query unique count from the table")

	countRows, err := countDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect count data from Iceberg")
	require.NotEmpty(t, countRows, "Count result is empty")

	countValue := countRows[0].Value("unique_count")
	require.NotNil(t, countValue, "Count value is nil")

	actualCount := utils.ConvertToString(countValue)
	require.Equal(t, expectedCount, actualCount,
		"Unique olake_id count mismatch in Iceberg")

	t.Logf("Verified %s unique olake_id records in Iceberg table %s",
		actualCount, tableName)
}
