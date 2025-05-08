package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

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

// MySQL-specific helpers
func createTestTable(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            id INTEGER PRIMARY KEY,
            col1 VARCHAR(255),
            col2 VARCHAR(255),
            col_int INTEGER,
            col_bigint BIGINT,
            col_float FLOAT,
            col_double DOUBLE PRECISION,
            col_decimal DECIMAL(10,2),
            col_boolean BOOLEAN,
            col_timestamp TIMESTAMP,
            col_date DATE,
            col_json JSONB,
            col_uuid UUID,
            col_array INTEGER[]
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
	allCols := append([]string{"id", "col_int", "col_bigint", "col_float", "col_double", "col_decimal", "col_boolean", "col_timestamp", "col_date", "col_json", "col_uuid", "col_array"}, cols...)
	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		values := make([]string, len(allCols))
		values[0] = fmt.Sprintf("%d", idx)                               // id
		values[1] = fmt.Sprintf("%d", idx)                               // col_int
		values[2] = fmt.Sprintf("%d", idx*1000)                          // col_bigint
		values[3] = fmt.Sprintf("%.2f", float64(idx)*1.5)                // col_float
		values[4] = fmt.Sprintf("%.2f", float64(idx)*2.5)                // col_double
		values[5] = fmt.Sprintf("%.2f", float64(idx)*3.5)                // col_decimal
		values[6] = fmt.Sprintf("%v", idx%2 == 0)                        // col_boolean
		values[7] = fmt.Sprintf("'2024-03-%02d 12:00:00'", (idx%30)+1)   // col_timestamp
		values[8] = fmt.Sprintf("'2024-03-%02d'", (idx%30)+1)            // col_date
		values[9] = fmt.Sprintf("'{\"key\": \"value_%d\"}'", idx)        // col_json
		values[10] = fmt.Sprintf("'00000000-0000-0000-0000-%012d'", idx) // col_uuid
		values[11] = fmt.Sprintf("ARRAY[%d, %d, %d]", idx, idx+1, idx+2) // col_array

		for i, col := range cols {
			values[i+12] = fmt.Sprintf("'%s val %d'", col, idx)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
			table, strings.Join(allCols, ", "), strings.Join(values, ", "))
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
}

func insertOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, col1, col2, col_int, col_bigint, col_float, col_double, 
			col_decimal, col_boolean, col_timestamp, col_date, col_json, 
			col_uuid, col_array
		) VALUES (
			10, 'new val', 'new val', 10, 10000, 15.5, 25.5, 35.5, 
			true, '2024-03-15 12:00:00', '2024-03-15', 
			'{"key": "new_value"}', '00000000-0000-0000-0000-000000000010',
			ARRAY[10, 11, 12]
		)`, tableName)
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err)
}

func updateOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)

	query := fmt.Sprintf(`
		UPDATE %s 
		SET 
			col1 = 'updated val',
			col_int = 20,
			col_bigint = 20000,
			col_float = 30.5,
			col_double = 40.5,
			col_decimal = 50.5,
			col_boolean = false,
			col_timestamp = '2024-03-16 12:00:00',
			col_date = '2024-03-16',
			col_json = '{"key": "updated_value"}',
			col_uuid = '00000000-0000-0000-0000-000000000020',
			col_array = ARRAY[20, 21, 22]
		WHERE id = (
			SELECT * FROM (
				SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
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
				SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
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

func getPostgresSchema(ctx context.Context, conn interface{}, tableName string) (map[string]string, error) {
	db, ok := conn.(*sqlx.DB)
	if !ok {
		return nil, fmt.Errorf("expected *sqlx.DB, got %T", conn)
	}

	rows, err := db.QueryxContext(ctx, `
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = $1
    `, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying pg schema: %w", err)
	}
	defer rows.Close()

	pgSchema := make(map[string]string)
	for rows.Next() {
		var col, dt string
		if err := rows.Scan(&col, &dt); err != nil {
			return nil, fmt.Errorf("scanning pg row: %w", err)
		}
		pgSchema[col] = strings.ToLower(dt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pg rows: %w", err)
	}

	return pgSchema, nil
}
