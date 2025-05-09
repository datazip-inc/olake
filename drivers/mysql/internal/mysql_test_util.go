// testMySQLClient
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
            col2 VARCHAR(255),
			col_tinyint TINYINT,
            col_smallint SMALLINT,
            col_mediumint MEDIUMINT,
            col_int INT,
            col_bigint BIGINT,
            col_float FLOAT,
            col_double DOUBLE,
            col_decimal DECIMAL(10,2),
            col_char CHAR(10),
            col_varchar VARCHAR(255),
            col_text TEXT,
            col_date DATE,
            col_datetime DATETIME,
            col_timestamp TIMESTAMP,
            col_time TIME,
            col_year YEAR,
            col_json JSON,
            col_enum ENUM('enum1', 'enum2'),
            col_set SET('set1', 'set2'),
            col_blob BLOB,
            col_binary BINARY(10),
            col_varbinary VARBINARY(255)
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
	allCols := []string{
		"id", "col_tinyint", "col_smallint", "col_mediumint", "col_int", "col_bigint",
		"col_float", "col_double", "col_decimal", "col_char", "col_varchar",
		"col_text", "col_date", "col_datetime", "col_timestamp", "col_time",
		"col_year", "col_json", "col_enum", "col_set", "col_blob",
		"col_binary", "col_varbinary",
	}
	allCols = append(allCols, cols...)
	placeholders := make([]string, len(allCols))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(allCols, ", "),
		strings.Join(placeholders, ", "),
	)

	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		// Base values for the standard columns
		baseValues := []interface{}{
			idx,
			int8(idx % 128),
			int16(idx),
			int32(idx),
			idx,
			int64(idx) * 1000,
			float32(idx) * 1.5,
			float64(idx) * 2.5,
			fmt.Sprintf("%.2f", float64(idx)*3.5),
			fmt.Sprintf("char%04d", idx),
			fmt.Sprintf("varchar%d", idx),
			fmt.Sprintf("text data %d", idx),
			fmt.Sprintf("2024-03-%02d", (idx%30)+1),
			fmt.Sprintf("2024-03-%02d 12:00:00", (idx%30)+1),
			time.Now().Format("2006-01-02 15:04:05"),
			fmt.Sprintf("%02d:00:00", idx%24),
			2024,
			fmt.Sprintf(`{"key": "value_%d"}`, idx),
			"enum1",
			"set1",
			[]byte(fmt.Sprintf("blob%d", idx)),
			[]byte(fmt.Sprintf("bin%04d", idx)),
			[]byte(fmt.Sprintf("varbin%d", idx)),
		}

		// Add string values for any additional columns provided
		values := append([]interface{}{}, baseValues...)
		for _, col := range cols {
			values = append(values, fmt.Sprintf("additional data for %s - item %d", col, idx))
		}

		_, err := db.ExecContext(ctx, query, values...)
		require.NoError(t, err, fmt.Sprintf("Failed to insert test data item %d", idx))
	}
}
func insertOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, col1, col2, col_tinyint, col_smallint, col_mediumint, col_int, col_bigint,
			col_float, col_double, col_decimal, col_char, col_varchar, col_text,
			col_date, col_datetime, col_timestamp, col_time, col_year,
			col_json, col_enum, col_set, col_blob, col_binary, col_varbinary, col_boolean
		) VALUES (
			10, 'new val', 'new val', 1, 100, 1000, 10000, 100000,
			1.23, 45.67, 123.45, 'char10', 'varchar value', 'text value',
			'2023-01-01', '2023-01-01 12:30:45', CURRENT_TIMESTAMP, '12:30:45', 2023,
			'{"key": "value"}', 'enum1', 'set1,set2', X'0123456789ABCDEF', 
			BINARY('binary'), 'varbinary'
		)`, tableName)
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to insert data into test table")
}

func updateOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)

	// Use a derived table to select a random ID
	query := fmt.Sprintf(`
		UPDATE %s
		SET 
			col1 = 'updated val',
			col2 = 'updated val2',
			col_tinyint = 2,
			col_smallint = 200,
			col_mediumint = 2000,
			col_int = 20000,
			col_bigint = 200000,
			col_float = 2.34,
			col_double = 56.78,
			col_decimal = 234.56,
			col_char = 'updated_ch',
			col_varchar = 'updated varchar value',
			col_text = 'updated text value',
			col_date = '2023-02-02',
			col_datetime = '2023-02-02 13:45:56',
			col_timestamp = CURRENT_TIMESTAMP,
			col_time = '13:45:56',
			col_year = 2024,
			col_json = '{"key": "updated_value"}',
			col_enum = 'enum2',
			col_set = 'set2',
			col_blob = X'ABCDEF0123456789',
			col_binary = BINARY('upd_binary'),
			col_varbinary = 'updated_varbinary'
		WHERE id = (
			SELECT * FROM (
				SELECT id FROM %s ORDER BY RAND() LIMIT 1
			) AS subquery
		)
	`, tableName, tableName)

	result, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to update data in test table")

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

func getMySQLSchema(ctx context.Context, conn interface{}, tableName string) (map[string]string, error) {
	db, ok := conn.(*sqlx.DB)
	if !ok {
		return nil, fmt.Errorf("expected *sqlx.DB, got %T", conn)
	}

	rows, err := db.QueryxContext(ctx, `
        SELECT column_name, column_type
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = ?
    `, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying MySQL schema: %w", err)
	}
	defer rows.Close()

	schema := make(map[string]string)
	for rows.Next() {
		var col, ct string
		if err := rows.Scan(&col, &ct); err != nil {
			return nil, fmt.Errorf("scanning MySQL row: %w", err)
		}
		// Check for BOOLEAN (tinyint(1))
		if ct == "tinyint(1)" {
			schema[col] = "boolean"
		} else {
			schema[col] = strings.ToLower(strings.Split(ct, "(")[0])
		}
	}
	return schema, nil
}
