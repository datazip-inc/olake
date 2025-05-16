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

// TestHelper defines database-specific helper functions
type TestHelper struct {
	ExecuteQuery func(ctx context.Context, t *testing.T, conn interface{}, tableName string, operation string)
}

// TestSetup tests the driver setup and connection check
var tableName = fmt.Sprintf("%s_%d", "test_table_olake", time.Now().Unix())

func TestSetup(t *testing.T, driver protocol.Driver, client interface{}) {
	t.Helper()
	assert.NotNil(t, client, "Client should not be nil")
	err := driver.Check()
	assert.NoError(t, err, "Connection check failed")
}

// TestDiscover tests the discovery of tables
func TestDiscover(t *testing.T, driver protocol.Driver, client interface{}, helper TestHelper) {
	t.Helper()
	ctx := context.Background()
	conn := client.(*sqlx.DB) // For Postgres; adjust if MySQL uses *sql.DB

	helper.ExecuteQuery(ctx, t, conn, tableName, "create")
	defer helper.ExecuteQuery(ctx, t, conn, tableName, "drop")
	helper.ExecuteQuery(ctx, t, conn, tableName, "clean")
	helper.ExecuteQuery(ctx, t, conn, tableName, "add")

	streams, err := driver.Discover(true)
	assert.NoError(t, err, "Discover failed")
	assert.NotEmpty(t, streams, "No streams found")
	for _, stream := range streams {
		if stream.Name == tableName {
			return
		}
	}
	assert.Fail(t, "Unable to find test table %s", tableName)
}

// TestRead tests full refresh and CDC read operations
func TestRead(t *testing.T, _ protocol.Driver, client interface{}, helper TestHelper, setupClient func(t *testing.T) (interface{}, protocol.Driver)) {
	t.Helper()
	ctx := context.Background()
	conn := client.(*sqlx.DB) // Adjust based on driver

	// Setup table and initial data
	helper.ExecuteQuery(ctx, t, conn, tableName, "create")
	defer helper.ExecuteQuery(ctx, t, conn, tableName, "drop")
	helper.ExecuteQuery(ctx, t, conn, tableName, "clean")
	helper.ExecuteQuery(ctx, t, conn, tableName, "add")

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
			"iceberg_db":      "olake_iceberg",
		},
	})
	require.NoError(t, err, "Failed to create writer pool")
	// Get test stream
	getTestStream := func(d protocol.Driver) *types.Stream {
		streams, err := d.Discover(true)
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
	// Run read test for a given sync mode
	runReadTest := func(t *testing.T, syncMode types.SyncMode, extraTests func(t *testing.T)) {
		_, streamDriver := setupClient(t)
		testStream := getTestStream(streamDriver)
		dummyStream := &types.ConfiguredStream{Stream: testStream}
		dummyStream.Stream.SyncMode = syncMode

		if syncMode == types.CDC {
			// Start CDC read operation
			// Run extra tests if provided
			if extraTests != nil {
				extraTests(t)
			}
			// Wait for CDC initialization
			time.Sleep(2 * time.Second)
			err := streamDriver.Read(pool, dummyStream)
			assert.NoError(t, err, "CDC read operation failed")
			// Wait for CDC to process
			time.Sleep(3 * time.Second)
		} else {
			// Handle full refresh read
			err := streamDriver.Read(pool, dummyStream)
			assert.NoError(t, err, "Read operation failed")
		}
	}
	t.Run("full refresh read", func(t *testing.T) {
		runReadTest(t, types.FULLREFRESH, nil)
		time.Sleep(120 * time.Second)
		VerifyIcebergSync(t, tableName, "5")
	})
	time.Sleep(60 * time.Second)
	t.Run("cdc read", func(t *testing.T) {
		runReadTest(t, types.CDC, func(t *testing.T) {
			t.Run("insert operation", func(t *testing.T) {
				helper.ExecuteQuery(ctx, t, conn, tableName, "insert")
			})
			time.Sleep(180 * time.Second)
			VerifyIcebergSync(t, tableName, "6")
			t.Run("update operation", func(t *testing.T) {
				helper.ExecuteQuery(ctx, t, conn, tableName, "update")
			})
			VerifyIcebergSync(t, tableName, "6")
			t.Run("delete operation", func(t *testing.T) {
				helper.ExecuteQuery(ctx, t, conn, tableName, "delete")
			})
			VerifyIcebergSync(t, tableName, "6")
		})
	})
}
func VerifyIcebergSync(t *testing.T, tableName string, expectedCount string) {
	t.Helper()
	ctx := context.Background()
	var sparkConnectAddress = "sc://localhost:15002" // Default value

	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Errorf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	// Query for unique olake_id records
	query := fmt.Sprintf("SELECT COUNT(DISTINCT _olake_id) as unique_count FROM olake_iceberg.olake_iceberg.%s", tableName)
	t.Logf("Executing query: %s", query)

	// Add retry for query execution
	countDf, err := spark.Sql(ctx, query)
	require.NoError(t, err, "Failed to query unique count from the table")

	// Collect the count result
	countRows, err := countDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect count data from Iceberg")
	require.NotEmpty(t, countRows, "Count result is empty")

	// Extract the count value using the correct method
	countValue := countRows[0].Value("unique_count")
	require.NotNil(t, countValue, "Count value is nil")

	// Verify unique count
	assert.Equal(t, expectedCount, utils.ConvertToString(countValue), "Unique olake_id count mismatch in Iceberg")
	t.Logf("Successfully verified %v unique olake_id records in Iceberg table %s", countValue, tableName)
}
