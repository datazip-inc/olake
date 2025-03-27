package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // Register pgx driver with database/sql
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// Test Client Setup
func testClient(t *testing.T) (*sqlx.DB, Config, *Postgres) {
	t.Helper()

	config := Config{
		Host:             "localhost",
		Port:             5432,
		Username:         "olake",
		Password:         "olake",
		Database:         "testdb",
		SSLConfiguration: &utils.SSLConfig{Mode: "disable"},
		BatchSize:        10000,
	}

	d := &Postgres{
		Driver: base.NewBase(),
		config: &config,
	}

	// Properly initialize State
	state := types.NewState(types.GlobalType)
	d.SetupState(state) // Assuming SetupState sets d.State = state

	_ = protocol.ChangeStreamDriver(d) // Only if ChangeStreamDriver is needed
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}

// Insert Test Data
// func addTestTableData(
// 	_ context.Context,
// 	t *testing.T,
// 	conn *sqlx.DB,
// 	table string,
// 	numItems int,
// 	startAtItem int,
// 	cols ...string,
// ) {
// 	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
// 		values := make([]string, len(cols))
// 		for i, col := range cols {
// 			values[i] = fmt.Sprintf("'%s val %d'", col, idx)
// 		}

// 		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
// 			table,             // Table name
// 			colsToSQL(cols),   // Column names
// 			colsToSQL(values), // Values as SQL-compatible format
// 		)

// 		logger.Debug("inserting data with query: ", query)
// 		_, err := conn.Exec(query)
// 		require.NoError(t, err)
// 	}
// }

// Helper: Convert column names to SQL format
func colsToSQL(cols []string) string {
	return fmt.Sprintf("%s", stringJoin(cols, ","))
}

// Helper: Join strings for SQL queries
func stringJoin(arr []string, sep string) string {
	result := ""
	for i, str := range arr {
		if i > 0 {
			result += sep
		}
		result += str
	}
	return result
}

// Delete Data from Table
func deleteData(
	ctx context.Context,
	t *testing.T,
	conn *pgx.Conn,
	table string,
	id any,
) {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = %v;", table, id)

	logger.Debug("deleting data with query: ", query)
	_, err := conn.Exec(ctx, query)
	require.NoError(t, err)
}

// Update Data in Table
func updateData(
	ctx context.Context,
	t *testing.T,
	conn *pgx.Conn,
	table string,
	id any,
	cols ...string,
) {
	setClause := ""
	for i, col := range cols {
		if i > 0 {
			setClause += ", "
		}
		setClause += fmt.Sprintf("%s = '%s val %d'", col, col, 0)
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = %v;", table, setClause, id)

	logger.Debug("updating data with query: ", query)
	_, err := conn.Exec(ctx, query)
	require.NoError(t, err)
}
