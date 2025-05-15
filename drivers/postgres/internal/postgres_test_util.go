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

// ExecuteQuery for Postgres
func ExecuteQuery(ctx context.Context, t *testing.T, conn interface{}, tableName string, operation string) {
	t.Helper()
	db := conn.(*sqlx.DB)
	var query string

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
		for i := 1; i <= 5; i++ {
			query = fmt.Sprintf("INSERT INTO %s (id,col1, col2) VALUES (%d,'value%d_col1', 'value%d_col2')", tableName, i, i, i)
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err, "Failed to add data")
		}
		return
	case "insert":
		query = fmt.Sprintf("INSERT INTO %s (id, col1, col2) VALUES (10, 'new val', 'new val')", tableName)
	case "update":
		query = fmt.Sprintf(`
		UPDATE %s 
		SET col1 = 'updated val' 
		WHERE id = (
			SELECT * FROM (
				SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
			) AS subquery
		)
	`, tableName, tableName)
	case "delete":
		query = fmt.Sprintf(`
		DELETE FROM %s 
		WHERE id = (
			SELECT * FROM (
				SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
			) AS subquery
		)
	`, tableName, tableName)
	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}

	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to execute query for operation: %s", operation)
}

// Test Client Setup
func testPostgresClient(t *testing.T) (*sqlx.DB, Config, *Postgres) {
	t.Helper()

	config := Config{
		Host:             "localhost",
		Port:             5433,
		Username:         "postgres",
		Password:         "secret1234",
		Database:         "postgres",
		SSLConfiguration: &utils.SSLConfig{Mode: "disable"},
		BatchSize:        10000,
	}

	d := &Postgres{
		Driver: base.NewBase(),
		config: &config,
	}

	// Properly initialize State
	d.CDCSupport = true
	d.cdcConfig = CDC{
		InitialWaitTime: 5,
		ReplicationSlot: "olake_slot",
	}
	state := types.NewState(types.GlobalType)
	d.SetupState(state)

	_ = protocol.ChangeStreamDriver(d)
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}
