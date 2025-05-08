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
	"github.com/lib/pq"
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
            col_array INTEGER[],
			col_biggerial BIGSERIAL,
            col_bit BIT(1),
            col_bit_varying VARBIT(10),
            col_bool BOOLEAN,
            col_box BOX,
            col_bpchar BPCHAR(10),
            col_char BPCHAR(1),
            col_character BPCHAR(10),
            col_character_varyi VARCHAR(50),
            col_daterange DATERANGE,
            col_double_precisic DOUBLE PRECISION,
            col_float4 REAL,
            col_float8 DOUBLE PRECISION,
            col_inet INET,
            col_int2 BIGINT,
            col_int2vector INT2VECTOR,
            col_int4 INTEGER,
            col_int8 BIGINT,
            col_integer INTEGER,
            col_interval INTERVAL,
            col_jsonb JSONB,
            col_name NAME,
            col_numeric NUMERIC,
            col_numrange NUMRANGE,
            col_path PATH,
            col_real REAL,
            col_text TEXT,
            col_tid TID,
            col_time TIME,
            col_timestamptz TIMESTAMPTZ,
            col_timetz TIMETZ,
            col_tsquery TSQUERY,
            col_tsrange TSRANGE,
            col_tstrrange TSTZRANGE,
            col_varbit VARBIT(20),
            col_varchar VARCHAR(100)
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
	// Define the base columns for all inserts
	allCols := []string{
		"id", "col_int", "col_bigint", "col_float", "col_double", "col_decimal",
		"col_boolean", "col_timestamp", "col_date", "col_json", "col_uuid", "col_array",
		"col_biggerial", "col_bit", "col_bit_varying", "col_bool", "col_box", "col_bpchar",
		"col_char", "col_character", "col_character_varyi", "col_daterange",
		"col_double_precisic", "col_float4", "col_float8", "col_inet", "col_int2",
		"col_int2vector", "col_int4", "col_int8", "col_integer", "col_interval",
		"col_jsonb", "col_name", "col_numeric", "col_numrange", "col_path",
		"col_real", "col_text", "col_tid", "col_time", "col_timestamptz",
		"col_timetz", "col_tsquery", "col_tsrange", "col_tstrrange", "col_varbit",
		"col_varchar",
	}
	// Add any additional columns passed as arguments
	if len(cols) > 0 {
		allCols = append(allCols, cols...)
	}

	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		values := make([]interface{}, len(allCols))
		placeholders := make([]string, len(allCols))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		// Standard columns (from the original function)
		values[0] = idx                                                       // id
		values[1] = idx                                                       // col_int
		values[2] = idx * 1000                                                // col_bigint
		values[3] = float64(idx) * 1.5                                        // col_float
		values[4] = float64(idx) * 2.5                                        // col_double
		values[5] = float64(idx) * 3.5                                        // col_decimal
		values[6] = idx%2 == 0                                                // col_boolean
		values[7] = fmt.Sprintf("2024-03-%02d 12:00:00", (idx%30)+1)          // col_timestamp
		values[8] = fmt.Sprintf("2024-03-%02d", (idx%30)+1)                   // col_date
		values[9] = fmt.Sprintf(`{"key": "value_%d"}`, idx)                   // col_json
		values[10] = fmt.Sprintf("00000000-0000-0000-0000-%012d", idx)        // col_uuid
		values[11] = pq.Array([]int{idx, idx + 1, idx + 2})                   // col_array
		values[12] = 12                                                       // col_biggerial
		values[13] = "1"                                                      // col_bit
		values[14] = fmt.Sprintf("%b", idx%1024)                              // col_bit_varying
		values[15] = idx%2 == 0                                               // col_bool
		values[16] = fmt.Sprintf("((%d,%d),(%d,%d))", idx, idx, idx+1, idx+1) // col_box
		values[17] = fmt.Sprintf("%-10s", fmt.Sprintf("char_%d", idx))        // col_bpchar
		values[18] = string(rune('A' + idx%26))                               // col_char
		values[19] = fmt.Sprintf("%-10s", fmt.Sprintf("char_%d", idx))        // col_character
		values[20] = fmt.Sprintf("varchar_%d", idx)                           // col_character_varyi
		values[21] = fmt.Sprintf("['2024-01-01','2024-01-%02d']", (idx%30)+1) // col_daterange
		values[22] = float64(idx) * 4.5                                       // col_double_precisic
		values[23] = float32(idx) * 5.5                                       // col_float4
		values[24] = float64(idx) * 6.5                                       // col_float8
		values[25] = fmt.Sprintf("192.168.1.%d", idx%255)                     // col_inet
		values[26] = int16(idx)                                               // col_int2
		values[27] = fmt.Sprintf("1 %d", idx)                                 // col_int2vector
		values[28] = int32(idx)                                               // col_int4
		values[29] = int64(idx)                                               // col_int8
		values[30] = idx                                                      // col_integer
		values[31] = fmt.Sprintf("%d days", idx)                              // col_interval
		values[32] = fmt.Sprintf(`{"id": %d, "name": "test_%d"}`, idx, idx)   // col_jsonb
		values[33] = fmt.Sprintf("name_%d", idx)                              // col_name
		values[34] = float64(idx) * 7.5                                       // col_numeric
		values[35] = fmt.Sprintf("[%d,%d]", idx, idx+10)                      // col_numrange
		values[36] = fmt.Sprintf("((%d,%d),(%d,%d))", idx, idx, idx+1, idx+1) // col_path
		values[37] = float32(idx) * 8.5                                       // col_real
		values[38] = fmt.Sprintf("text_%d", idx)                              // col_text
		values[39] = fmt.Sprintf("(%d,%d)", idx, idx)                         // col_tid
		values[40] = fmt.Sprintf("%02d:%02d:%02d", idx%24, idx%60, idx%60)    // col_time
		values[41] = fmt.Sprintf("2024-03-%02d 12:00:00+00", (idx%30)+1)      // col_timestamptz
		values[42] = fmt.Sprintf("%02d:%02d:%02d+00", idx%24, idx%60, idx%60) // col_timetz
		values[43] = fmt.Sprintf("'word%d' & 'word%d'", idx, idx+1)           // col_tsquery
		values[44] = fmt.Sprintf("['2024-03-%02d 12:00:00','2024-03-%02d 13:00:00']",
			(idx%30)+1, (idx%30)+1) // col_tsrange
		values[45] = fmt.Sprintf("['2024-03-%02d 12:00:00+00','2024-03-%02d 13:00:00+00']",
			(idx%30)+1, (idx%30)+1) // col_tstrrange
		values[46] = fmt.Sprintf("%b", idx%1048576)     // col_varbit
		values[47] = fmt.Sprintf("varchar_val_%d", idx) // col_varchar

		// Handle any additional columns passed as arguments
		for i, col := range cols {
			values[len(allCols)-len(cols)+i] = fmt.Sprintf("%s val %d", col, idx)
		}
		// Build parameterized query
		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			table,
			strings.Join(allCols, ", "),
			strings.Join(placeholders, ", "),
		)

		_, err := db.ExecContext(ctx, query, values...)
		require.NoError(t, err, "Failed to insert test data")
	}
}

func insertOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, col1, col2, col_int, col_bigint, col_float, col_double, 
			col_decimal, col_boolean, col_timestamp, col_date, col_json, 
			col_uuid, col_array, col_biggerial, col_bit, col_bit_varying, col_bool, 
			col_box, col_bpchar, col_char, col_character, col_character_varyi,
			col_daterange, col_double_precisic, col_float4, col_float8, 
			col_inet, col_int2, col_int2vector, col_int4, col_int8, 
			col_integer, col_interval, col_jsonb, col_name, col_numeric, 
			col_numrange, col_path, col_real, col_text, col_tid, 
			col_time, col_timestamptz, col_timetz, col_tsquery, 
			col_tsrange, col_tstrrange, col_varbit, col_varchar
		) VALUES (
			10, 'new val', 'new val', 10, 10000, 15.5, 25.5, 35.5, 
			true, '2024-03-15 12:00:00', '2024-03-15', 
			'{"key": "new_value"}', '00000000-0000-0000-0000-000000000010',
			ARRAY[10, 11, 12], 20,'1', '1010', true, 
			'((10,10),(11,11))', 'char_10   ', 'A', 'char_10   ', 'varchar_10',
			'[2024-01-01,2024-01-10]', 45.0, 55.0, 65.0, 
			'192.168.1.10', 10, '1 10', 10, 10, 
			10, '10 days', '{"id": 10, "name": "test_10"}', 'name_10', 75.0, 
			'[10,20]', '((10,10),(11,11))', 85.0, 'text_10', '(10,10)', 
			'10:10:10', '2024-03-10 12:00:00+00', '10:10:10+00', '''word10'' & ''word11''', 
			'[2024-03-10 12:00:00,2024-03-10 13:00:00]', 
			'[2024-03-10 12:00:00+00,2024-03-10 13:00:00+00]', 
			'1010', 'varchar_val_10'
		)`, tableName)

	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to insert test data")
}

func updateOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	db := conn.(*sqlx.DB)
	query := fmt.Sprintf(`
		UPDATE %s 
		SET 
			col1 = 'updated val',
			col2 = 'updated val2',
			col_int = 69,
			col_bigint = 690000,
			col_float = 30.5,
			col_double = 40.5,
			col_decimal = 50.5,
			col_boolean = false,
			col_timestamp = '2024-03-16 12:00:00',
			col_date = '2024-03-16',
			col_json = '{"key": "updated_value"}',
			col_uuid = '00000000-0000-0000-0000-000000000020',
			col_array = ARRAY[20, 21, 22],
			col_bool = false,
			col_bpchar = 'updated   ',
			col_char = 'U',
			col_character = 'updated   ',
			col_character_varyi = 'updated_varchar',
			col_daterange = '[2024-02-01,2024-02-20]',
			col_double_precisic = 55.5,
			col_float4 = 65.5,
			col_float8 = 75.5,
			col_inet = '192.168.2.20',
			col_jsonb = '{"id": 20, "name": "updated_test"}',
			col_name = 'updated_name',
			col_numeric = 85.5,
			col_timestamptz = '2024-03-20 12:00:00+00',
			col_timetz = '20:20:20+00',
			col_tsquery = '''updated'' & ''word''',
			col_tsrange = '[2024-03-20 12:00:00,2024-03-20 13:00:00]'
		WHERE id = (
			SELECT id FROM (
				SELECT id FROM %s ORDER BY RANDOM() LIMIT 1
			) AS subquery
		)
	`, tableName, tableName)

	result, err := db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to update test data")

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
