package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/parquet"
	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver with database/sql
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresSetup tests the basic setup and connection check
func TestPostgresSetup(t *testing.T) {
	client, _, _ := testClient(t)
	assert.NotNil(t, client)

	t.Run("successful connexaction check", func(t *testing.T) {
		_, _, pClient := testClient(t)
		err := pClient.Check()
		assert.NoError(t, err)
	})
}

// TestPostgresDiscover tests the discovery of tables
func TestPostgresDiscover(t *testing.T) {
	client, _, _ := testClient(t)
	assert.NotNil(t, client)

	ctx := context.Background()
	tableName := "test_table111"

	//Create and populate test table
	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	cleanTestTable(ctx, t, client, tableName) // Clean before adding data
	addTestTableData(ctx, t, client, tableName, 5, 6, "col1", "col2")

	t.Run("discover with tables", func(t *testing.T) {
		_, _, pClient := testClient(t)
		streams, err := pClient.Discover(true)
		assert.NoError(t, err)
		assert.NotEmpty(t, streams)
		for _, stream := range streams {
			if stream.Name == tableName {
				return
			}
		}
		assert.NoError(t, fmt.Errorf("unable to find test table %s", tableName))
	})
}

// TestPostgresRead tests full refresh and CDC read operations
// TestPostgresRead tests full refresh and CDC read operations
func TestPostgresRead(t *testing.T) {
	client, _, _ := testClient(t)
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

	// Register the Parquet writer
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return &parquet.Parquet{}
	}

	// Create a writer pool
	pool, err := protocol.NewWriter(ctx, &types.WriterConfig{
		Type: "PARQUET",
		WriterConfig: map[string]any{
			"normalization": true,
			"local_path":    "/Users/datazip/Desktop/olake-1/drivers/postgres/examples",
		},
	})
	require.NoError(t, err, "Failed to create writer pool")
	getTestStream := func(d *Postgres) *types.Stream {
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
	runReadTest := func(t *testing.T, syncMode types.SyncMode, extraTests func(t *testing.T)) {
		_, _, pClient := testClient(t)
		pClient.SetupState(types.NewState(types.GlobalType))
		testStream := getTestStream(pClient)
		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = syncMode
		if syncMode == types.CDC {
			// Start CDC read in a goroutine to capture changes
			readErrCh := make(chan error, 1)
			go func() {
				readErrCh <- pClient.Read(pool, dummyStream)
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
			err = pClient.Read(pool, dummyStream)
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
func cleanTestTable(ctx context.Context, t *testing.T, conn *sqlx.DB, tableName string) {
	query := fmt.Sprintf("DELETE FROM %s", tableName)
	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to clean test table")
}

// Helper function to create a test table with primary key
func createTestTable(ctx context.Context, t *testing.T, conn *sqlx.DB, tableName string) {
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
func dropTestTable(ctx context.Context, t *testing.T, conn *sqlx.DB, tableName string) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := conn.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to drop test table")
}
func addTestTableData(
	_ context.Context,
	t *testing.T,
	conn *sqlx.DB,
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
