package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

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

	t.Run("successful connection check", func(t *testing.T) {
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
	client, config, d := testClient(t)
	if client == nil {
		return
	}
	ctx := context.Background()
	tableName := "test_d_tab"

	// Create and populate test table
	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	addTestTableData(ctx, t, client, tableName, 5, 1, "col1", "col2")

	// Verify data insertion
	rows, err := client.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", tableName))
	require.NoError(t, err, "Failed to query test table")
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var id int
		var col1, col2 string
		err := rows.Scan(&id, &col1, &col2)
		require.NoError(t, err)
		t.Logf("Row %d: id=%d, col1=%s, col2=%s", count, id, col1, col2)
	}
	assert.Equal(t, 5, count, "Expected 5 rows in test table")

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
		d.State.SetGlobalState(&types.State{})

		// Log configured stream
		t.Logf("Configured stream: %+v", dummyStream)

		// Perform the read
		err = pClient.Read(pool, dummyStream)
		if err != nil {
			t.Logf("Read error: %v", err)
		}
		assert.NoError(t, err, "Read operation failed")
	})
	t.Run("cdc read", func(t *testing.T) {
		pClient := &Postgres{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}

		var walLevel string
		err := client.QueryRowContext(context.Background(), "SHOW wal_level").Scan(&walLevel)
		require.NoError(t, err, "Failed to query wal_level")
		if walLevel != "logical" {
			t.Skip("Skipping CDC test because wal_level is not set to logical")
		}

		pClient.SetupState(types.NewState(types.GlobalType))

		// Discover streams
		streams, err := pClient.Discover(true)
		assert.NoError(t, err)

		dummyStream := &types.ConfiguredStream{
			Stream: streams[0],
		}
		assert.NotEmpty(t, streams)
		dummyStream.Stream.SyncMode = types.CDC
		d.State.SetGlobalState(&types.State{})
		err = pClient.Read(pool, dummyStream)
		assert.NoError(t, err)

	})

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

// Modified addTestTableData with primary key
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
