package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
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
				col_bigint BIGINT,
				col_bigserial BIGSERIAL PRIMARY KEY,
				col_bool BOOLEAN,
				col_char CHAR(1),
				col_character CHAR(10),
				col_character_varying VARCHAR(50),
				col_date DATE,
				col_daterange DATERANGE,
				col_decimal NUMERIC,
				col_double_precision DOUBLE PRECISION,
				col_float4 REAL,
				col_int INT,
				col_int2 SMALLINT,
				col_integer INTEGER,
				col_interval INTERVAL,
				col_json JSON,
				col_jsonb JSONB,
				col_jsonpath JSONPATH,
				col_name NAME,
				col_numeric NUMERIC,
				col_real REAL,
				col_text TEXT,
				col_time TIME,
				col_timestamp TIMESTAMP,
				col_timestamptz TIMESTAMPTZ,
				col_timetz TIMETZ,
				col_uuid UUID,
				col_varbit VARBIT(20),
				col_xml XML,
				CONSTRAINT unique_custom_key_3 UNIQUE (col_bigserial)
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
			INSERT INTO %s (col_int, col_character_varying, col_text) 
			VALUES (10, 'new val', 'new val')`, tableName)

	case "update":
		query = fmt.Sprintf(`
			UPDATE %s 
			SET col_character_varying = 'updated val' 
			WHERE col_bigserial = (
				SELECT col_bigserial FROM (
					SELECT col_bigserial FROM %s ORDER BY RANDOM() LIMIT 1
				) AS subquery
			)`, tableName, tableName)

	case "delete":
		query = fmt.Sprintf(`
			DELETE FROM %s 
			WHERE col_bigserial = (
				SELECT col_bigserial FROM (
					SELECT col_bigserial FROM %s ORDER BY RANDOM() LIMIT 1
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
		INSERT INTO %s (
			col_bigint, col_bigserial, col_bool, col_char, col_character,
			col_character_varying, col_date, col_daterange, col_decimal,
			col_double_precision, col_float4, col_int, col_int2, col_integer,
			col_interval, col_json, col_jsonb, col_jsonpath, col_name, col_numeric,
			col_real, col_text, col_time, col_timestamp, col_timestamptz, col_timetz,
			col_uuid, col_varbit, col_xml
		) VALUES (
			1234567890123456789, DEFAULT, TRUE, 'c', 'char_val',
			'varchar_val', '2023-01-01', '[2023-01-01,2023-01-02)', 123.45,
			123.456789, 123.45, 123, 123, 12345, '1 hour', '{"key": "value"}',
			'{"key": "value"}', '$.key', 'test_name', 123.45, 123.45,
			'sample text', '12:00:00', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00+00', '12:00:00+00',
			'123e4567-e89b-12d3-a456-426614174000', B'101010',
			'<tag>value</tag>'
		)`, tableName)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data")
	}
}

// testPostgresClient initializes and returns a PostgreSQL test client with default configuration
func testPostgresClient(t *testing.T) (*sqlx.DB, *abstract.AbstractDriver) {
	t.Helper()
	logger.Init()

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
		config: &config,
	}

	// Configure CDC settings
	pgDriver.CDCSupport = true
	pgDriver.cdcConfig = CDC{
		InitialWaitTime: defaultCDCWaitTime,
		ReplicationSlot: defaultReplicationSlot,
	}

	absDriver := abstract.NewAbstractDriver(context.Background(), pgDriver)

	state := &types.State{
		Type:    types.StreamType,
		RWMutex: &sync.RWMutex{},
	}
	absDriver.SetupState(state)
	require.NoError(t, absDriver.Setup(context.Background()), "Failed to setup PostgreSQL driver")

	return pgDriver.client, absDriver
}
