package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func ExecuteQuery(ctx context.Context, t *testing.T, tableName string, operation string) {
	t.Helper()
	db, ok := sqlx.ConnectContext(ctx, "postgres",
		"postgres://postgres@localhost:5433/postgres?sslmode=disable",
	)
	require.NoError(t, ok, "failed to connect to postgres")

	var (
		query string
		err   error
	)

	switch operation {
	case "create":
		query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				col_bigint BIGINT,
				col_bigserial BIGSERIAL PRIMARY KEY,
				col_bool BOOLEAN,
				col_char CHAR(1),
				col_character CHAR(10),
				col_character_varying VARCHAR(50),
				col_date DATE,
				col_decimal NUMERIC,
				col_double_precision DOUBLE PRECISION,
				col_float4 REAL,
				col_int INT,
				col_int2 SMALLINT,
				col_integer INTEGER,
				col_interval INTERVAL,
				col_json JSON,
				col_jsonb JSONB,
				col_name NAME,
				col_numeric NUMERIC,
				col_real REAL,
				col_text TEXT,
				col_timestamp TIMESTAMP,
				col_timestamptz TIMESTAMPTZ,
				col_uuid UUID,
				col_varbit VARBIT(20),
				col_xml XML,
				CONSTRAINT unique_custom_key UNIQUE (col_bigserial)
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
			INSERT INTO %s (
				col_bigint, col_bool, col_char, col_character,
				col_character_varying, col_date, col_decimal,
				col_double_precision, col_float4, col_int, col_int2,
				col_integer, col_interval, col_json, col_jsonb,
				col_name, col_numeric, col_real, col_text,
				col_timestamp, col_timestamptz, col_uuid, col_varbit, col_xml
			) VALUES (
				123456789012345, TRUE, 'c', 'char_val',
				'varchar_val', '2023-01-01', 123.45,
				123.456789, 123.45, 123, 123, 12345,
				'1 hour', '{"key": "value"}', '{"key": "value"}',
				'test_name', 123.45, 123.45, 'sample text',
				'2023-01-01 12:00:00', '2023-01-01 12:00:00+00',
				'123e4567-e89b-12d3-a456-426614174000', B'101010',
				'<tag>value</tag>'
			)`, tableName)

	case "update":
		query = fmt.Sprintf(`
			UPDATE %s SET
				col_bigint = 123456789012340,
				col_bool = FALSE,
				col_char = 'd',
				col_character = 'updated__',
				col_character_varying = 'updated val',
				col_date = '2024-07-01',
				col_decimal = 543.21,
				col_double_precision = 987.654321,
				col_float4 = 543.21,
				col_int = 321,
				col_int2 = 321,
				col_integer = 54321,
				col_interval = '2 hours',
				col_json = '{"new": "json"}',
				col_jsonb = '{"new": "jsonb"}',
				col_name = 'updated_name',
				col_numeric = 321.00,
				col_real = 321.00,
				col_text = 'updated text',
				col_timestamp = '2024-07-01 15:30:00',
				col_timestamptz = '2024-07-01 15:30:00+00',
				col_uuid = '00000000-0000-0000-0000-000000000000',
				col_varbit = B'111000',
				col_xml = '<updated>value</updated>'
			WHERE col_bigserial = 1`, tableName)

	case "delete":
		query = fmt.Sprintf("DELETE FROM %s WHERE col_bigserial = 1", tableName)

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
		INSERT INTO %s (
			col_bigint, col_bigserial, col_bool, col_char, col_character,
			col_character_varying, col_date, col_decimal,
			col_double_precision, col_float4, col_int, col_int2, col_integer,
			col_interval, col_json, col_jsonb, col_name, col_numeric,
			col_real, col_text, col_timestamp, col_timestamptz,
			col_uuid, col_varbit, col_xml
		) VALUES (
			123456789012345, DEFAULT, TRUE, 'c', 'char_val',
			'varchar_val', '2023-01-01', 123.45,
			123.456789, 123.45, 123, 123, 12345, '1 hour', '{"key": "value"}',
			'{"key": "value"}', 'test_name', 123.45, 123.45,
			'sample text', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00+00',
			'123e4567-e89b-12d3-a456-426614174000', B'101010',
			'<tag>value</tag>'
		)`, tableName)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data")
	}
}

var ExpectedPostgresData = map[string]interface{}{
	"col_bigint":            int64(123456789012345),
	"col_bool":              true,
	"col_char":              "c",
	"col_character":         "char_val  ",
	"col_character_varying": "varchar_val",
	"col_date":              arrow.Timestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_decimal":           float32(123.45),
	"col_double_precision":  123.456789,
	"col_float4":            float32(123.45),
	"col_int":               int32(123),
	"col_int2":              int32(123),
	"col_integer":           int32(12345),
	"col_interval":          "01:00:00",
	"col_json":              `{"key": "value"}`,
	"col_jsonb":             `{"key": "value"}`,
	"col_name":              "test_name",
	"col_numeric":           float32(123.45),
	"col_real":              float32(123.45),
	"col_text":              "sample text",
	"col_timestamp":         arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_timestamptz":       arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_uuid":              "123e4567-e89b-12d3-a456-426614174000",
	"col_varbit":            "101010",
	"col_xml":               "<tag>value</tag>",
}

var ExpectedUpdatedPostgresData = map[string]interface{}{
	"col_bigint":            int64(123456789012340),
	"col_bool":              false,
	"col_char":              "d",
	"col_character":         "updated__ ",
	"col_character_varying": "updated val",
	"col_date":              arrow.Timestamp(time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_decimal":           float32(543.21),
	"col_double_precision":  987.654321,
	"col_float4":            float32(543.21),
	"col_int":               int32(321),
	"col_int2":              int32(321),
	"col_integer":           int32(54321),
	"col_interval":          "02:00:00",
	"col_json":              `{"new": "json"}`,
	"col_jsonb":             `{"new": "jsonb"}`,
	"col_name":              "updated_name",
	"col_numeric":           float32(321.00),
	"col_real":              float32(321.00),
	"col_text":              "updated text",
	"col_timestamp":         arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_timestamptz":       arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_uuid":              "00000000-0000-0000-0000-000000000000",
	"col_varbit":            "111000",
	"col_xml":               "<updated>value</updated>",
}

var PostgresToIcebergSchema = map[string]string{
	"col_bigint":            "bigint",
	"col_bigserial":         "bigserial",
	"col_bool":              "boolean",
	"col_char":              "char",
	"col_character":         "character",
	"col_character_varying": "varchar",
	"col_date":              "date",
	"col_decimal":           "numeric",
	"col_double_precision":  "double precision",
	"col_float4":            "real",
	"col_int":               "int",
	"col_int2":              "smallint",
	"col_integer":           "integer",
	"col_interval":          "interval",
	"col_json":              "json",
	"col_jsonb":             "jsonb",
	"col_name":              "name",
	"col_numeric":           "numeric",
	"col_real":              "real",
	"col_text":              "text",
	"col_timestamp":         "timestamp",
	"col_timestamptz":       "timestamptz",
	"col_uuid":              "uuid",
	"col_varbit":            "varbit",
	"col_xml":               "xml",
}
