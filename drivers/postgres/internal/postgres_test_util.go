package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	defaultPostgresHost     = "localhost"
	defaultPostgresPort     = 5433
	defaultPostgresUser     = "postgres"
	defaultPostgresPassword = "secret1234"
	defaultPostgresDB       = "postgres"
	defaultBatchSize        = 10000
	defaultCDCWaitTime      = 5
	defaultReplicationSlot  = "olake_slot"
)

// ExecuteQuery executes PostgreSQL queries for testing based on the operation type
func ExecuteQuery(ctx context.Context, t *testing.T, conn interface{}, tableName string, operation string) {
	t.Helper()

	db, ok := conn.(*sqlx.DB)
	require.True(t, ok, "Expected *sqlx.DB connection")

	var (
		query string
		err   error
	)

	switch operation {
	case "create":
		query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INTEGER PRIMARY KEY,
				col1 VARCHAR(255),
				col2 VARCHAR(255)
			)`, tableName)

	case "drop":
		query = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)

	case "clean":
		query = fmt.Sprintf("DELETE FROM %s", tableName)

	case "add":
		insertTestData(t, ctx, db, tableName)
		return // Early return since we handle all inserts in the helper function

	case "insert":
		query = fmt.Sprintf(`
			INSERT INTO %s (id, col1, col2) 
			VALUES (10, 'new val', 'new val')`, tableName)

	case "update":
		query = fmt.Sprintf(`
			UPDATE %s 
			SET col1 = 'updated val' 
			WHERE id = (
				SELECT id FROM (
					SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
				) AS subquery
			)`, tableName, tableName)

	case "delete":
		query = fmt.Sprintf(`
			DELETE FROM %s 
			WHERE id = (
				SELECT id FROM (
					SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
				) AS subquery
			)`, tableName, tableName)

	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}

	_, err = db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to execute %s operation", operation)
}

// insertTestData inserts test data into the specified table
func insertTestData(t *testing.T, ctx context.Context, db *sqlx.DB, tableName string) {
	t.Helper()

	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf(`
			INSERT INTO %s (id, col1, col2) 
			VALUES (%d, 'value%d_col1', 'value%d_col2')`,
			tableName, i, i, i)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

// testPostgresClient initializes and returns a PostgreSQL test client with default configuration
func testPostgresClient(t *testing.T) (*sqlx.DB, Config, *Postgres) {
	t.Helper()

	config := Config{
		Host:     defaultPostgresHost,
		Port:     defaultPostgresPort,
		Username: defaultPostgresUser,
		Password: defaultPostgresPassword,
		Database: defaultPostgresDB,
		SSLConfiguration: &utils.SSLConfig{
			Mode: "disable",
		},
		BatchSize: defaultBatchSize,
	}

	pgDriver := &Postgres{
		Driver: base.NewBase(),
		config: &config,
	}

	// Configure CDC settings
	pgDriver.CDCSupport = true
	pgDriver.cdcConfig = CDC{
		InitialWaitTime: defaultCDCWaitTime,
		ReplicationSlot: defaultReplicationSlot,
	}

	// Initialize state
	state := types.NewState(types.GlobalType)
	pgDriver.SetupState(state)

	_ = protocol.ChangeStreamDriver(pgDriver)
	require.NoError(t, pgDriver.Setup(), "Failed to setup PostgreSQL driver")

	return pgDriver.client, *pgDriver.config, pgDriver
}
