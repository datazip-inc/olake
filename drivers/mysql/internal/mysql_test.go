package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

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

// TestMySQLDiscover
func TestMySQLDiscover(t *testing.T) {
	client, config, _ := testMySQLClient(t)
	assert.NotNil(t, client)

	ctx := context.Background()
	tableName := "test_table111"

	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
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
func TestMySQLRead(t *testing.T) {
	client, config, d := testMySQLClient(t)
	if client == nil {
		return
	}
	ctx := context.Background()
	tableName := "test_d_tab"

	createTestTable(ctx, t, client, tableName)
	defer dropTestTable(ctx, t, client, tableName)
	addTestTableData(ctx, t, client, tableName, 5, 1, "col1", "col2")

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
	assert.NoError(t, err)

	t.Run("full refresh read", func(t *testing.T) {
		mClient := &MySQL{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}
		mClient.SetupState(types.NewState(types.GlobalType))

		streams, err := mClient.Discover(true)
		assert.NoError(t, err)
		assert.NotEmpty(t, streams)

		var testStream *types.Stream
		for _, stream := range streams {
			if stream.Name == tableName {
				testStream = stream
				break
			}
		}
		assert.NotNil(t, testStream, "Could not find stream for table %s", tableName)

		dummyStream := &types.ConfiguredStream{
			Stream: testStream,
		}
		dummyStream.Stream.SyncMode = types.FULLREFRESH
		d.State.SetGlobalState(&types.State{})

		err = mClient.Read(pool, dummyStream)
		assert.NoError(t, err, "Read operation failed")
	})

	t.Run("cdc read", func(t *testing.T) {
		mClient := &MySQL{
			Driver: base.NewBase(),
			client: client,
			config: &config,
		}

		var logBin string
		err := client.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'log_bin'").Scan(&logBin, &logBin)
		require.NoError(t, err, "Failed to query log_bin")
		if logBin != "ON" {
			t.Skip("Skipping CDC test because binary logging is not enabled")
		} else {
			t.Log("Binary logging is enabled")
		}

		mClient.SetupState(types.NewState(types.GlobalType))

		streams, err := mClient.Discover(true)
		assert.NoError(t, err)
		assert.NotEmpty(t, streams)

		dummyStream := &types.ConfiguredStream{
			Stream: streams[0],
		}
		dummyStream.Stream.SyncMode = types.CDC
		d.State.SetGlobalState(&types.State{})
		err = mClient.Read(pool, dummyStream)
		assert.NoError(t, err)
	})
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

// Modified addTestTableData with primary key
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
