package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/parquet"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLSetup(t *testing.T) {
	client, config, _ := testMySQLClient(t)
	assert.NotNil(t, client)

	t.Run("successful connection check", func(t *testing.T) {
		mClient := &MySQL{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
		err := mClient.Check()
		assert.NoError(t, err)
	})
}

// TestMySQLDiscover Update TestMySQLDiscover to use same table name
func TestMySQLDiscover(t *testing.T) {
	client, config, _ := testMySQLClient(t)
	assert.NotNil(t, client)

	ctx := context.Background()
	tableName := "test_table_olake" // Same table name as TestMySQLRead

	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	cleanTestTable(ctx, t, client, tableName) // Clean before adding data
	addTestTableData(ctx, t, client, tableName, 5, 6, "col1", "col2")

	t.Run("discover with tables", func(t *testing.T) {
		mClient := &MySQL{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
		streams, err := mClient.Discover(true)
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

// TestMySQLRead
// TestMySQLRead
func TestMySQLRead(t *testing.T) {
	client, config, _ := testMySQLClient(t)
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

	t.Run("full refresh read", func(t *testing.T) {
		mClient := &MySQL{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
		mClient.SetupState(types.NewState(types.GlobalType))

		streams, err := mClient.Discover(true)
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

		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = types.FULLREFRESH
		mClient.State.SetGlobalState(&types.State{})

		err = mClient.Read(pool, dummyStream)
		assert.NoError(t, err, "Read operation failed")
	})

	t.Run("cdc read", func(t *testing.T) {
		mClient := &MySQL{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}

		mClient.CDCSupport = true
		mClient.cdcConfig = CDC{
			InitialWaitTime: 5,
		}
		mClient.SetupState(types.NewState(types.GlobalType))
		mClient.State.SetGlobalState(&types.State{})

		streams, err := mClient.Discover(true)
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

		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = types.CDC

		// Start CDC read in a goroutine to capture changes
		readErrCh := make(chan error, 1)
		go func() {
			readErrCh <- mClient.Read(pool, dummyStream)
		}()

		// Give CDC some time to initialize
		time.Sleep(2 * time.Second)

		// Test INSERT
		t.Run("insert operation", func(t *testing.T) {
			_, err := client.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s (id, col1, col2) VALUES (6, 'new val 6', 'test 6')", tableName))
			assert.NoError(t, err)
		})

		// Test UPDATE
		t.Run("update operation", func(t *testing.T) {
			_, err := client.ExecContext(ctx,
				fmt.Sprintf("UPDATE %s SET col1 = 'updated val 1' WHERE id = 1", tableName))
			assert.NoError(t, err)
		})

		// Test DELETE
		t.Run("delete operation", func(t *testing.T) {
			_, err := client.ExecContext(ctx,
				fmt.Sprintf("DELETE FROM %s WHERE id = 2", tableName))
			assert.NoError(t, err)
		})

		// Wait briefly for CDC to process changes
		time.Sleep(3 * time.Second)

		select {
		case err := <-readErrCh:
			assert.NoError(t, err, "CDC read operation failed")
		case <-time.After(100 * time.Second):
			t.Fatal("CDC read timed out")
		}

		// Give CDC time to process the new data
		time.Sleep(2 * time.Second)
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
