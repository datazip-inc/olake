package base

import (
	"context"
	"fmt"
	"strings"
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

	GetDBSchema func(ctx context.Context, conn interface{}, tableName string) (map[string]string, error)
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
			"normalization":   true,
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

	dbSchema, err := helper.GetDBSchema(ctx, conn, tableName)
	require.NoError(t, err, "Failed to get database schema")
	t.Logf("Source database schema for %s:", tableName)
	for col, dtype := range dbSchema {
		t.Logf("    %s: %s", col, dtype)
	}

	// Run read test for a given sync mode
	runReadTest := func(t *testing.T, syncMode types.SyncMode, extraTests func(t *testing.T)) {
		_, streamDriver := setupClient(t)
		testStream := getTestStream(streamDriver)
		dummyStream := &types.ConfiguredStream{Stream: testStream}
		dummyStream.StreamMetadata.Normalization = true
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
	}

	t.Run("full refresh read", func(t *testing.T) {
		runReadTest(t, types.FULLREFRESH, nil)
		time.Sleep(120 * time.Second)

		// Call VerifyIcebergSync with the database schema for comparison
		VerifyIcebergSync(
			t, tableName, "5", "after full load", dbSchema,
			"olake_id", "col1", "col2", "col_int", "col_bigint",
			"col_float", "col_double", "col_decimal", "col_boolean",
			"col_timestamp", "col_date", "col_json", "col_uuid", "col_array",
		)
	})
	time.Sleep(60 * time.Second)
	t.Run("cdc read", func(t *testing.T) {
		runReadTest(t, types.CDC, func(t *testing.T) {
			t.Run("insert operation", func(t *testing.T) {
				helper.InsertOp(ctx, t, conn, tableName)
			})
			time.Sleep(40 * time.Second)
			VerifyIcebergSync(t, tableName, "6", "after insert", dbSchema,
				"olake_id", "col1", "col2", "col_int", "col_bigint",
				"col_float", "col_double", "col_decimal", "col_boolean",
				"col_timestamp", "col_date", "col_json", "col_uuid", "col_array")

			t.Run("update operation", func(t *testing.T) {
				helper.UpdateOp(ctx, t, conn, tableName)
			})
			time.Sleep(20 * time.Second)
			VerifyIcebergSync(t, tableName, "6", "after update", dbSchema,
				"olake_id", "col1", "col2", "col_int", "col_bigint",
				"col_float", "col_double", "col_decimal", "col_boolean",
				"col_timestamp", "col_date", "col_json", "col_uuid", "col_array")

			t.Run("delete operation", func(t *testing.T) {
				helper.DeleteOp(ctx, t, conn, tableName)
			})
			time.Sleep(20 * time.Second)
			VerifyIcebergSync(t, tableName, "6", "after delete", dbSchema,
				"olake_id", "col1", "col2", "col_int", "col_bigint",
				"col_float", "col_double", "col_decimal", "col_boolean",
				"col_timestamp", "col_date", "col_json", "col_uuid", "col_array")
		})
	})
}

func VerifyIcebergSync(t *testing.T, tableName string, expectedCount string, message string, sourceDBSchema map[string]string, _ ...string) {
	t.Helper()
	ctx := context.Background()
	var sparkConnectAddress = "sc://localhost:15002" // Default value

	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if err := spark.Stop(); err != nil {
			t.Logf("Error stopping Spark session: %v", err)
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
	assert.Equal(t, expectedCount, utils.ConvertToString(countValue), "Unique olake_id count mismatch in Iceberg (%s)", message)
	t.Logf("Successfully verified %v unique olake_id records in Iceberg table %s - %s", countValue, tableName, message)

	dataTypeQuery := fmt.Sprintf("DESCRIBE TABLE olake_iceberg.olake_iceberg.%s", tableName)
	t.Logf("Executing schema query: %s", dataTypeQuery)

	df, err := spark.Sql(ctx, dataTypeQuery)
	require.NoError(t, err, "Failed to describe the table")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect schema rows")

	icebergSchema := make(map[string]string, len(rows))
	t.Logf("Iceberg schema for table %s:", tableName)
	for _, r := range rows {
		cn := r.Value("col_name")
		dt := r.Value("data_type")
		if cn != nil && dt != nil {
			col := fmt.Sprintf("%v", cn)
			typ := fmt.Sprintf("%v", dt)
			icebergSchema[col] = typ
			t.Logf("    %s: %s", col, typ)
		}
	}

	// Compare the source and Iceberg schemas
	if sourceDBSchema != nil {
		t.Logf("Comparing source database schema with Iceberg schema...")
		for col, pgType := range sourceDBSchema {
			// Check if the column exists in Iceberg schema
			iceType, found := icebergSchema[col]
			if !found {
				t.Logf("WARNING: Column %s from source database not found in Iceberg schema", col)
				continue
			}
			// Verify data type mappings
			if !compareDataTypes(t, pgType, iceType) {
				t.Errorf("Data type mismatch for column %s: source=%s, iceberg=%s", col, pgType, iceType)
			} else {
				t.Logf("Data type match for column %s: source=%s, iceberg=%s", col, pgType, iceType)
			}
		}
	}
}

func compareDataTypes(t *testing.T, sourceType, icebergType string) bool {
	t.Helper()
	// Comprehensive mapping of PostgreSQL to Iceberg data types
	typeMapping := map[string]string{
		// Core numeric types
		"integer":          "int",
		"bigint":           "bigint",
		"smallint":         "int",
		"double precision": "double",
		"real":             "float",
		"numeric":          "float",
		"decimal":          "float",
		"float4":           "float",
		"float8":           "double",

		// Boolean
		"boolean": "boolean",

		// Date/time types
		"date":                        "timestamp",
		"timestamp without time zone": "timestamp",
		"timestamp with time zone":    "timestamp",
		"time without time zone":      "string",
		"time with time zone":         "string",
		"interval":                    "string",

		// String types
		"tid":               "string",
		"character varying": "string",
		"varchar":           "string",
		"text":              "string",
		"character":         "string",
		"bpchar":            "string",
		"name":              "string",
		"uuid":              "string",
		"jsonb":             "string",
		"xml":               "string",
		"inet":              "string",
		"bit":               "string",
		"bit varying":       "string",
		"tsvector":          "string",
		"tsquery":           "string",
		"path":              "string",
		"box":               "string",
		"line":              "string",
		"circle":            "string",
		"int4range":         "string",
		"numrange":          "string",
		"tsrange":           "string",
		"tstzrange":         "string",
		"daterange":         "string",
		"array":             "string",
		"int2vector":        "string",
	}

	// Normalize types for comparison
	sourceTypeLower := strings.ToLower(sourceType)
	icebergTypeLower := strings.ToLower(icebergType)

	// Check direct mapping first
	expectedIcebergType, found := typeMapping[sourceTypeLower]
	if found {
		return strings.HasPrefix(icebergTypeLower, expectedIcebergType)
	}

	// Handle array types (e.g., integer[])
	if strings.HasSuffix(sourceTypeLower, "[]") {
		return strings.HasPrefix(icebergTypeLower, "array") ||
			icebergTypeLower == "string"
	}

	// Handle numeric/decimal with precision
	if strings.HasPrefix(sourceTypeLower, "numeric") ||
		strings.HasPrefix(sourceTypeLower, "decimal") {
		return strings.HasPrefix(icebergTypeLower, "float") ||
			strings.HasPrefix(icebergTypeLower, "decimal")
	}

	// Handle timestamp variants
	if strings.Contains(sourceTypeLower, "timestamp") {
		return strings.Contains(icebergTypeLower, "timestamp")
	}

	t.Logf("No mapping defined for source type: %s", sourceTypeLower)
	return false
}
