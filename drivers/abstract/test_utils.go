package abstract

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/destination/iceberg"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTablePrefix = "test_table_olake"
	// sparkConnectAddress   = "sc://host.docker.internal:15002"
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
func (a *AbstractDriver) TestRead(t *testing.T, conn interface{}, execQuery ExecuteQuery, _ map[string]string) {
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
			configuredStream.StreamMetadata.Normalization = true
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

			// VerifyIcebergSync(t, currentTestTable, tc.expectedCount, datatypeSchema)
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
	})
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
func VerifyIcebergSync(t *testing.T, tableName string, sparkConnectHost string, datatypeSchema map[string]string, schema map[string]interface{}, opSymbol string) {
	t.Helper()
	ctx := context.Background()
	sparkConnectAddress := fmt.Sprintf("sc://%s:15002", sparkConnectHost)

	spark, err := sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
	require.NoError(t, err, "Failed to connect to Spark Connect server")
	defer func() {
		if stopErr := spark.Stop(); stopErr != nil {
			t.Errorf("Failed to stop Spark session: %v", stopErr)
		}
	}()

	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s.%s.%s WHERE _op_type = '%s'",
		icebergDatabase, icebergDatabase, tableName, opSymbol,
	)
	t.Logf("Executing query: %s", selectQuery)

	selectQueryDf, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to select query from the table")

	selectRows, err := selectQueryDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect data rows from Iceberg")

	// delete row checked
	if opSymbol == "d" {
		deletedID := selectRows[0].Value("_olake_id")
		require.Equalf(t, "1", deletedID, "Delete verification failed: expected _olake_id = '1', got %s", deletedID)
		return
	}

	require.NotEmpty(t, selectRows, "No rows returned for _op_type = '%s'", opSymbol)

	for rowIdx, row := range selectRows {
		icebergMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			icebergMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			icebergValue, ok := icebergMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
			require.Equal(t, icebergValue, expected, "Row %d: mismatch on %q: Iceberg has %#v, expected %#v", rowIdx, key, icebergValue, expected)
		}
	}
	t.Logf("Verified synced data in Iceberg")

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s.%s.%s", icebergDatabase, icebergDatabase, tableName)
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

	for col, dbType := range datatypeSchema {
		iceType, found := icebergSchema[col]
		require.True(t, found, "Column %s not found in Iceberg schema", col)

		expectedIceType, mapped := GlobalTypeMapping[dbType]
		if !mapped {
			t.Logf("No mapping defined for PostgreSQL type %s (column %s), skipping check", dbType, col)
			continue
		}
		require.Equal(t, expectedIceType, iceType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
	}
	t.Logf("Verified datatypes in Iceberg after sync")
}
