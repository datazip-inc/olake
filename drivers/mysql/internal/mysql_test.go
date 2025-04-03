package driver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/parquet"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLSetup(t *testing.T) {
	client, _, _ := testMySQLClient(t)
	assert.NotNil(t, client)

	t.Run("successful connection check", func(t *testing.T) {
		_, _, mClient := testMySQLClient(t)
		err := mClient.Check()
		assert.NoError(t, err)
	})
}

// TestMySQLDiscover Update TestMySQLDiscover to use same table name
func TestMySQLDiscover(t *testing.T) {
	client, _, _ := testMySQLClient(t)
	assert.NotNil(t, client)

	ctx := context.Background()
	tableName := "test_table_olake" // Same table name as TestMySQLRead

	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	cleanTestTable(ctx, t, client, tableName) // Clean before adding data
	addTestTableData(ctx, t, client, tableName, 5, 6, "col1", "col2")

	t.Run("discover with tables", func(t *testing.T) {
		_, _, mClient := testMySQLClient(t)
		streams, err := mClient.Discover(true)
		assert.NoError(t, err)
		assert.NotEmpty(t, streams)
		for _, stream := range streams {
			if stream.Name == tableName {
				verifyStreamSchema(t, client, stream, tableName)
				return
			}
		}

		assert.NoError(t, fmt.Errorf("unable to find test table %s", tableName))
	})
}

// TestMySQLRead
func TestMySQLRead(t *testing.T) {
	client, _, _ := testMySQLClient(t)
	if client == nil {
		return
	}

	ctx := context.Background()
	tableName := "test_table_olake"

	// Setup table and initial data
	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	cleanTestTable(ctx, t, client, tableName)
	addTestTableData(ctx, t, client, tableName, 5, 1, "col1", "col2")

	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return &parquet.Parquet{}
	}

	pool, err := protocol.NewWriter(ctx, &types.WriterConfig{
		Type: "PARQUET",
		WriterConfig: map[string]any{
			"normalization": true,
			"local_path":    "/Users/datazip/Desktop/olake-1/drivers/mysql/examples",
		},
	})
	require.NoError(t, err, "Failed to create writer pool")

	// Helper function to get test stream for the table
	getTestStream := func(d *MySQL) *types.Stream {
		streams, err := d.Discover(true)
		require.NoError(t, err, "Discover failed")
		require.NotEmpty(t, streams, "No streams found")

		var testStream *types.Stream
		for _, stream := range streams {
			if stream.Name == tableName {
				testStream = stream
				break
			}
		}
		require.NotNil(t, testStream, "Could not find stream for table %s", tableName)
		return testStream
	}

	// Helper function to setup and run a test with the specified sync mode
	runReadTest := func(t *testing.T, syncMode types.SyncMode, extraTests func(t *testing.T)) {
		// Use a fresh instance of the MySQL driver
		_, _, mClient := testMySQLClient(t)

		// Reset state for this test
		mClient.State.SetGlobalState(&types.State{})

		testStream := getTestStream(mClient)
		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = syncMode

		if syncMode == types.CDC {
			// Start CDC read in a goroutine to capture changes
			readErrCh := make(chan error, 1)
			go func() {
				readErrCh <- mClient.Read(pool, dummyStream)
			}()

			// Give CDC some time to initialize
			time.Sleep(2 * time.Second)

			// Run extra tests if provided
			if extraTests != nil {
				extraTests(t)
			}

			// Wait briefly for CDC to process changes
			time.Sleep(3 * time.Second)

			select {
			case err := <-readErrCh:
				assert.NoError(t, err, "CDC read operation failed")
			case <-time.After(100 * time.Second):
				t.Fatal("CDC read timed out")
			}
		} else {
			// For non-CDC modes, just run the read operation directly
			err = mClient.Read(pool, dummyStream)
			assert.NoError(t, err, "Read operation failed")
		}
	}

	t.Run("full refresh read", func(t *testing.T) {
		runReadTest(t, types.FULLREFRESH, nil)
	})

	t.Run("cdc read", func(t *testing.T) {
		runReadTest(t, types.CDC, func(t *testing.T) {
			// Test database operations
			t.Run("insert operation", func(t *testing.T) {
				_, err := client.ExecContext(ctx,
					fmt.Sprintf("INSERT INTO %s (id, col1, col2) VALUES (6, 'new val 6', 'test 6')", tableName))
				assert.NoError(t, err)
			})

			t.Run("update operation", func(t *testing.T) {
				_, err := client.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET col1 = 'updated val 1' WHERE id = 1", tableName))
				assert.NoError(t, err)
			})

			t.Run("delete operation", func(t *testing.T) {
				_, err := client.ExecContext(ctx,
					fmt.Sprintf("DELETE FROM %s WHERE id = 2", tableName))
				assert.NoError(t, err)
			})
		})
	})
}

// Add this new helper function to clean the table
func cleanTestTable(ctx context.Context, t *testing.T, conn *sql.DB, tableName string) {
	query := fmt.Sprintf("DELETE FROM %s", tableName)
	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to clean test table")
}

// Helper function to create a test table with primary key
func createTestTable(ctx context.Context, t *testing.T, conn *sql.DB, tableName string) {
	query := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        id INTEGER PRIMARY KEY,
        col1 VARCHAR(255),
        col2 VARCHAR(255)
    )`, tableName)

	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to create test table")
}

// Helper function to drop test table
func dropTestTable(ctx context.Context, t *testing.T, conn *sql.DB, tableName string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to drop test table")
}
func addTestTableData(
	_ context.Context,
	t *testing.T,
	conn *sql.DB,
	table string,
	numItems int,
	startAtItem int,
	cols ...string,
) {
	allCols := append([]string{"id"}, cols...)

	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		values := make([]string, len(allCols))
		values[0] = fmt.Sprintf("%d", idx) // Primary key value

		for i, col := range cols {
			values[i+1] = fmt.Sprintf("'%s val %d'", col, idx)
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			table,
			strings.Join(allCols, ", "),
			strings.Join(values, ", "),
		)

		_, err := conn.Exec(query)
		require.NoError(t, err)
	}
}
func verifyStreamSchema(t *testing.T, conn *sql.DB, stream *types.Stream, tableName string) {
	ctx := context.Background()
	assert.Equal(t, stream.Name, tableName, "Stream name does not match table name")
	query := jdbc.MySQLTableSchemaQuery()
	rows, err := conn.QueryContext(ctx, query, tableName, stream.Namespace)
	require.NoError(t, err, "Failed to query table schema")
	defer rows.Close()
	schemaJSON, err := json.Marshal(stream.Schema)
	require.NoError(t, err, "Failed to marshal stream schema")

	// Unmarshal back into a more convenient structure
	var schemaMap struct {
		Properties map[string]map[string]interface{}
	}
	err = json.Unmarshal(schemaJSON, &schemaMap)
	require.NoError(t, err, "Failed to unmarshal stream schema")

	// Now work with the unmarshaled map
	schemaColumns := make(map[string]string)
	for colName, colInfo := range schemaMap.Properties {
		typeField := colInfo["type"]

		var schemaType string
		if typeList, ok := typeField.([]interface{}); ok && len(typeList) > 0 {
			schemaType = typeList[0].(string) // Take first type, e.g., "string" from ["string", "null"]
		} else if typeStr, ok := typeField.(string); ok {
			schemaType = typeStr
		} else {
			assert.Fail(t, "Invalid type field for column %s", colName)
		}

		schemaColumns[colName] = schemaType
	}
	actualColumnCount := 0
	for rows.Next() {
		var colName, colType, dataType, isNullable, columnKey string
		err := rows.Scan(&colName, &colType, &dataType, &isNullable, &columnKey)
		require.NoError(t, err, "Failed to scan column info")

		// Map MySQL data type to schema type
		schemaType := types.Unknown
		if val, found := mysqlTypeToDataTypes[dataType]; found {
			schemaType = val
		} else {
			logger.Warnf("Unsupported MySQL type '%s'for column '%s.%s', defaulting to String", dataType, tableName, colName)
			schemaType = types.String
		}

		// Check column existence and type
		streamType, exists := schemaColumns[colName]
		assert.True(t, exists, "Column %s should exist in stream schema", colName)
		assert.Equal(t, schemaType, streamType, "Data type for column %s should match", colName)

		// Check primary key status
		isPrimary := columnKey == "PRI"
		if isPrimary {
			assert.NotNil(t, stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should not be nil for table with primary key")
			assert.True(t, stream.SourceDefinedPrimaryKey.Exists(colName), "Column %s should be in SourceDefinedPrimaryKey", colName)
		}

		actualColumnCount++
	}

}
