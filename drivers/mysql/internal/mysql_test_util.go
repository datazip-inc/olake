// testMySQLClient
package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func testMySQLClient(t *testing.T) (*sqlx.DB, Config, *MySQL) {
	t.Helper()
	config := Config{
		Username:   "mysql",
		Host:       "localhost",
		Port:       3306,
		Password:   "secret1234",
		Database:   "mysql",
		MaxThreads: 4,
		RetryCount: 3,
	}

	// Create MySQL driver instance
	d := &MySQL{
		Driver: base.NewBase(),
		config: &config,
	}

	d.CDCSupport = true
	d.cdcConfig = CDC{
		InitialWaitTime: 5,
	}
	d.CDCSupport = true
	state := types.NewState(types.GlobalType)
	d.SetupState(state)

	_ = protocol.ChangeStreamDriver(d) 
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}

// MySQL-specific helpers
func createTestTable(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY,
            col1 VARCHAR(255),
            col2 VARCHAR(255)
        )`, tableName)
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to create test table")
}

func dropTestTable(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to drop test table")
}

func cleanTestTable(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf("DELETE FROM %s", tableName)
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to clean test table")
}

func addTestTableData(ctx context.Context, t *testing.T, conn interface{}, table string, numItems int, startAtItem int, cols ...string) {
	db := conn.(*sqlx.DB)
	allCols := append([]string{"id"}, cols...)
	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		values := make([]string, len(allCols))
		values[0] = fmt.Sprintf("%d", idx)
		for i, col := range cols {
			values[i+1] = fmt.Sprintf("'%s val %d'", col, idx)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			table, strings.Join(allCols, ", "), strings.Join(values, ", "))
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
}
func insertOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf("INSERT INTO %s (id, col1, col2) VALUES (10, 'new val', 'new val')", tableName)
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err)
}
func updateOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)

	// Use a derived table to select a random ID
	query := fmt.Sprintf(`
		UPDATE %s 
		SET col1 = 'updated val' 
		WHERE id = (
			SELECT * FROM (
				SELECT id FROM %s ORDER BY RAND() LIMIT 1
			) AS subquery
		)
	`, tableName, tableName)

	result, err := db.ExecContext(ctx, query)
	require.NoError(t, err)

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		t.Log("No rows found, skipping update.")
	}
}

func deleteOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)

	// Use a derived table to select a random ID
	query := fmt.Sprintf(`
		DELETE FROM %s 
		WHERE id = (
			SELECT * FROM (
				SELECT id FROM %s ORDER BY RAND() LIMIT 1
			) AS subquery
		)
	`, tableName, tableName)

	result, err := db.ExecContext(ctx, query)
	require.NoError(t, err)

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		t.Log("No rows found, skipping delete.")
	}
}
