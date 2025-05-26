package base

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/iceberg"
	"github.com/jmoiron/sqlx"
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

// TestHelper defines database-specific helper functions
type TestHelper struct {
	ExecuteQuery func(ctx context.Context, t *testing.T, conn interface{}, tableName string, operation string)
}

var currentTestTable = fmt.Sprintf("%s_%d", testTablePrefix, time.Now().Unix())

// TestSetup tests the driver setup and connection check
func TestSetup(t *testing.T, driver protocol.Driver, client interface{}) {
	t.Helper()

	assert.NotNil(t, client, "Client should not be nil")
	require.NoError(t, driver.Check(), "Connection check failed")
}

// TestDiscover tests the discovery of tables
func TestDiscover(t *testing.T, driver protocol.Driver, client interface{}, helper TestHelper) {
	t.Helper()
	ctx := context.Background()

	conn, ok := client.(*sqlx.DB)
	require.True(t, ok, "Invalid client type, expected *sqlx.DB")

	// Setup and cleanup test table
	helper.ExecuteQuery(ctx, t, conn, currentTestTable, "create")
	defer helper.ExecuteQuery(ctx, t, conn, currentTestTable, "drop")
	helper.ExecuteQuery(ctx, t, conn, currentTestTable, "clean")
	helper.ExecuteQuery(ctx, t, conn, currentTestTable, "add")

	streams, err := driver.Discover(true)
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
func TestRead(
	t *testing.T,
	_ protocol.Driver,
	client interface{},
	helper TestHelper,
	setupClient func(t *testing.T) (interface{}, protocol.Driver),
) {
	t.Helper()
	ctx := context.Background()

	conn, ok := client.(*sqlx.DB)
	require.True(t, ok, "Invalid client type, expected *sqlx.DB")
	time.Sleep(3 * time.Minute) // Allow time for the minio to stabilize
	// Setup table and initial data
	helper.ExecuteQuery(ctx, t, conn, currentTestTable, "create")
	defer helper.ExecuteQuery(ctx, t, conn, currentTestTable, "drop")
	helper.ExecuteQuery(ctx, t, conn, currentTestTable, "clean")
	helper.ExecuteQuery(ctx, t, conn, currentTestTable, "add")

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
			_, streamDriver := setupClient(t)
			testStream := getTestStream(t, streamDriver, currentTestTable)
			configuredStream := &types.ConfiguredStream{Stream: testStream}
			configuredStream.Stream.SyncMode = tc.syncMode

			if tc.syncMode == types.CDC {
				// Execute the operation for CDC tests
				helper.ExecuteQuery(ctx, t, conn, currentTestTable, tc.operation)

				// Wait for CDC initialization
				time.Sleep(cdcInitializationWait)

				require.NoError(t,
					streamDriver.Read(pool, configuredStream),
					"CDC read operation failed",
				)

				// Wait for CDC to process
				time.Sleep(cdcProcessingWait)
			} else {
				// Handle full refresh read
				require.NoError(t,
					streamDriver.Read(pool, configuredStream),
					"Read operation failed",
				)
				time.Sleep(cdcProcessingWait)
			}

			verifyIcebergSync(t, currentTestTable, tc.expectedCount)
		})
	}
}

// setupWriterPool initializes and returns a new writer pool
func setupWriterPool(ctx context.Context, t *testing.T) *protocol.WriterPool {
	t.Helper()

	// Register Parquet writer
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return &iceberg.Iceberg{}
	}

	pool, err := protocol.NewWriter(ctx, &types.WriterConfig{
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
			"aws_region":      "ap-south-1",
			"aws_secret_key":  "password",
			"iceberg_db":      icebergDatabase,
		},
	})
	require.NoError(t, err, "Failed to create writer pool")

	return pool
}

// getTestStream retrieves the test stream by table name
func getTestStream(t *testing.T, driver protocol.Driver, tableName string) *types.Stream {
	t.Helper()

	streams, err := driver.Discover(true)
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
