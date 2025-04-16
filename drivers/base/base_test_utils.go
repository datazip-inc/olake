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
	CreateTable func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	DropTable   func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	CleanTable  func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	AddData     func(ctx context.Context, t *testing.T, conn interface{}, tableName string, numItems int, startAtItem int, cols ...string)

	//CDC operations
	InsertOp func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	UpdateOp func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	DeleteOp func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
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

	helper.CreateTable(ctx, t, conn, tableName)
	defer helper.DropTable(ctx, t, conn, tableName)
	helper.CleanTable(ctx, t, conn, tableName)
	helper.AddData(ctx, t, conn, tableName, 5, 1, "col1", "col2")

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
	helper.CreateTable(ctx, t, conn, tableName)
	defer helper.DropTable(ctx, t, conn, tableName)
	helper.CleanTable(ctx, t, conn, tableName)
	helper.AddData(ctx, t, conn, tableName, 5, 1, "col1", "col2")

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
			readErrCh := make(chan error, 1)
			go func() {
				readErrCh <- streamDriver.Read(pool, dummyStream)
			}()
			time.Sleep(2 * time.Second) // Wait for CDC initialization
			if extraTests != nil {
				extraTests(t)
			}
			time.Sleep(3 * time.Second) // Wait for CDC to process
			// Directly receive from the channel
			err := <-readErrCh
			assert.NoError(t, err, "CDC read operation failed")
		} else {
			err := streamDriver.Read(pool, dummyStream)
			assert.NoError(t, err, "Read operation failed")
		}
		VerifyIcebergSync(t, tableName, "5", "after c/u/d", "olake_id", "col1", "col2")
	}

	t.Run("full refresh read", func(t *testing.T) {
		runReadTest(t, types.FULLREFRESH, nil)
	})
	time.Sleep(60 * time.Second)
	t.Run("cdc read", func(t *testing.T) {
		runReadTest(t, types.CDC, func(t *testing.T) {
			t.Run("insert operation", func(t *testing.T) {
				helper.InsertOp(ctx, t, conn, tableName)
			})
			t.Run("update operation", func(t *testing.T) {
				helper.UpdateOp(ctx, t, conn, tableName)
			})
			t.Run("delete operation", func(t *testing.T) {
				helper.DeleteOp(ctx, t, conn, tableName)
			})
		})
	})

}

func VerifyIcebergSync(t *testing.T, tableName string, expectedCount string, message string, verifyColumns ...string) {
	t.Helper()
	ctx := context.Background()
	var sparkConnectAddress = "sc://localhost:15002" // Default value

	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer spark.Stop()

	// Query for unique olake_id records
	query := fmt.Sprintf("SELECT COUNT(DISTINCT olake_id) as unique_count FROM olake_iceberg.olake_iceberg.%s", tableName)
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
	assert.Equal(t, expectedCount, utils.ConvertToString(countValue), "Unique olake_id count mismatch in Iceberg (%s)", message)
	t.Logf("Successfully verified %v unique olake_id records in Iceberg table %s - %s", countValue, tableName, message)
}
