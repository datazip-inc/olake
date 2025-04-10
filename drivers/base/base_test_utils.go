package base

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/iceberg"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func VerifyIcebergSync(t *testing.T, tableName string, expectedCount int, message string, verifyColumns ...string) {
	t.Helper()
	ctx := context.Background()

	// Allow some time for data to be synced to Iceberg
	time.Sleep(5 * time.Second)

	// Connect to Spark
	var sparkConnectAddress = "sc://localhost:15002" // Default value

	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	if err == nil {
		fmt.Println("Connected to Spark")
	} else {
		t.Logf("Failed to connect to Spark at sc://localhost:15002. Make sure Spark Connect server is running.")
		require.NoError(t, err, "Failed to connect to Spark Connect server")
	}
	require.NoError(t, err, "Failed to connect to Spark")
	defer spark.Stop()

	time.Sleep(15 * time.Second)
	query := "SHOW DATABASES"

	// Execute the SQL query to get the list of databases
	databasesDf, err := spark.Sql(ctx, query)
	require.NoError(t, err, "Failed to execute SHOW DATABASES query")
	databasesDf.Show(ctx, 100, false)
	//Query for unique olake_id records
	query = fmt.Sprintf("SELECT COUNT(DISTINCT olake_id) as unique_count FROM olake_iceberg.olake_iceberg.%s", tableName)
	countDf, err := spark.Sql(ctx, query)
	require.NoError(t, err, "Failed to query unique count from the table")

	//Collect the count result
	countRows, err := countDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect count data from Iceberg")
	require.NotEmpty(t, countRows, "Count result is empty")

	// Extract the count value using the correct method
	countValue := countRows[0].Value("unique_count")
	require.NotNil(t, countValue, "Count value is nil")

	// Convert the value to int (handling different possible types)
	var uniqueCount int
	switch v := countValue.(type) {
	case int64:
		uniqueCount = int(v)
	case int32:
		uniqueCount = int(v)
	case int:
		uniqueCount = v
	case float64:
		uniqueCount = int(v)
	default:
		t.Logf("Unexpected type for count: %T", countValue)
		// Try to convert using fmt.Sprintf and then parsing
		countStr := fmt.Sprintf("%v", countValue)
		parsed, err := strconv.ParseInt(countStr, 10, 64)
		require.NoError(t, err, "Failed to parse count value: %v", countValue)
		uniqueCount = int(parsed)
	}

	// Verify unique count
	assert.Equal(t, expectedCount, uniqueCount, "Unique olake_id count mismatch in Iceberg (%s)", message)
	// Display the actual data for debugging
	dataDf, err := spark.Sql(ctx, fmt.Sprintf("SELECT * FROM olake_iceberg.olake_iceberg.%s", tableName))
	require.NoError(t, err, "Failed to query data from the table")
	dataDf.Show(ctx, 100, false)
	//Log the verification result
	t.Logf("Successfully verified %d unique olake_id records in Iceberg table %s - %s", uniqueCount, tableName, message)
}

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
const tableName = "test_table_olake"

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
	helper.AddData(ctx, t, conn, tableName, 5, 6, "col1", "col2")

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
			VerifyIcebergSync(t, tableName, 6, "after c/u/d", "olake_id", "col1", "col2")
		} else {
			err := streamDriver.Read(pool, dummyStream)
			assert.NoError(t, err, "Read operation failed")
			VerifyIcebergSync(t, tableName, 5, "after full refresh", "olake_id", "col1", "col2")
		}
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
