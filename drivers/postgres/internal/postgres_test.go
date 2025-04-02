package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
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
	client, config, _ := testClient(t)
	assert.NotNil(t, client)

	t.Run("successful connexaction check", func(t *testing.T) {
		pClient := &Postgres{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
		err := pClient.Check()
		assert.NoError(t, err)
	})
}

// TestPostgresDiscover tests the discovery of tables
func TestPostgresDiscover(t *testing.T) {
	client, config, _ := testClient(t)
	assert.NotNil(t, client)

	ctx := context.Background()
	tableName := "test_table111"

	//Create and populate test table
	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	cleanTestTable(ctx, t, client, tableName) // Clean before adding data
	addTestTableData(ctx, t, client, tableName, 5, 6, "col1", "col2")

	t.Run("discover with tables", func(t *testing.T) {
		pClient := &Postgres{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
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
	client, config, _ := testClient(t)
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
	assert.NoError(t, err)
	// var walLevel string
	// err = client.QueryRowContext(context.Background(), "SHOW wal_level").Scan(&walLevel)
	// require.NoError(t, err, "Failed to query wal_level")

	// t.Logf("PostgreSQL wal_level is: %s", walLevel)

	// if walLevel != "logical" {
	// 	t.Fatalf("Expected wal_level to be 'logical', but got: %s", walLevel)
	// }

	t.Run("full refresh read", func(t *testing.T) {
		pClient := &Postgres{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
		// Properly initialize State
		pClient.SetupState(types.NewState(types.GlobalType))

		// Discover streams
		streams, err := pClient.Discover(true)
		assert.NoError(t, err)
		assert.NotEmpty(t, streams)

		// Find the correct stream for test_read_table
		var testStream *types.Stream
		for _, stream := range streams {
			if stream.Name == tableName {
				testStream = stream
				break
			}
		}
		assert.NotNil(t, testStream, "Could not find stream for table %s", tableName)

		// Log stream details
		t.Logf("Discovered stream: Name=%s, SyncMode=%s", testStream.Name, testStream.SyncMode)

		// Configure the stream
		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = types.FULLREFRESH
		pClient.State.SetGlobalState(&types.State{})

		// Log configured stream
		t.Logf("Configured stream: %+v", dummyStream)

		// Perform the read
		err = pClient.Read(pool, dummyStream)
		if err != nil {
			t.Logf("Read error: %v", err)
		}
		assert.NoError(t, err, "Read operation failed")
	})

	t.Run("cdc read with crud operations", func(t *testing.T) {
		pClient := &Postgres{
			Driver: base.NewBase(),
			client: client,
			config: &config,
			cdcConfig: CDC{
				InitialWaitTime: 5,
				ReplicationSlot: "olake_slot",
			},
		}
		pClient.CDCSupport = true
		pClient.SetupState(types.NewState(types.GlobalType))

		// Discover streams
		streams, err := pClient.Discover(true)
		require.NoError(t, err)
		require.NotEmpty(t, streams)

		var testStream *types.Stream
		for _, stream := range streams {
			if stream.Name == tableName {
				testStream = stream
				break
			}
		}
		require.NotNil(t, testStream)

		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = types.CDC

		// Start CDC read in a goroutine to capture changes
		errChan := make(chan error, 1)
		go func() {
			errChan <- pClient.Read(pool, dummyStream)
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

		// Check if CDC read completed successfully
		select {
		case err := <-errChan:
			assert.NoError(t, err, "CDC read failed")
		case <-time.After(100 * time.Second):
			t.Fatal("CDC read timed out")
		}
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
