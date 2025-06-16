package driver

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	defaultMySQLUser     = "mysql"
	defaultMySQLHost     = "localhost"
	defaultMySQLPort     = 3306
	defaultMySQLPassword = "secret1234"
	defaultMySQLDatabase = "mysql"
	defaultMaxThreads    = 4
	defaultRetryCount    = 3
	initialCDCWaitTime   = 5
)

// ExecuteQuery executes MySQL queries for testing based on the operation type
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
				id INT UNSIGNED NOT NULL AUTO_INCREMENT,
				id_bigint BIGINT,
				id_bigint_unsigned BIGINT UNSIGNED,
				id_int INT,
				id_int_unsigned INT UNSIGNED,
				id_integer INT,
				id_integer_unsigned INT UNSIGNED,
				id_mediumint MEDIUMINT,
				id_mediumint_unsigned MEDIUMINT UNSIGNED,
				id_smallint SMALLINT,
				id_smallint_unsigned SMALLINT UNSIGNED,
				id_tinyint TINYINT,
				id_tinyint_unsigned TINYINT UNSIGNED,
				price_decimal DECIMAL(10,2),
				price_double DOUBLE,
				price_double_precision DOUBLE,
				price_float FLOAT,
				price_numeric DECIMAL(10,2),
				price_real DOUBLE,
				name_char CHAR(50),
				name_varchar VARCHAR(100),
				name_text TEXT,
				name_tinytext TINYTEXT,
				name_mediumtext MEDIUMTEXT,
				name_longtext LONGTEXT,
				name_enum ENUM('Small','Medium','Large'),
				created_date DATETIME,
				created_time TIME,
				created_timestamp TIMESTAMP NULL,
				is_active TINYINT(1),
				json_data JSON,
				long_varchar MEDIUMTEXT,
				name_bool TINYINT(1) DEFAULT '1',
				PRIMARY KEY (id)
			)`, tableName)

	case "drop":
		query = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)

	case "clean":
		query = fmt.Sprintf("DELETE FROM %s", tableName)

	case "add":
		insertTestData(t, ctx, db, tableName)
		return // Early return since we handle all inserts in the helper function

	case "insert":
		query = fmt.Sprintf(
			"INSERT INTO %s (id, name_varchar, name_text) VALUES (10, 'new val', 'new val')",
			tableName,
		)

	case "update":
		query = fmt.Sprintf(
			`UPDATE %s
			 SET name_varchar = 'updated val'
			 WHERE id = (
				SELECT id FROM (
					SELECT id FROM %s ORDER BY RAND() LIMIT 1
				) AS subquery
			)`,
			tableName, tableName,
		)

	case "delete":
		query = fmt.Sprintf(`
			DELETE FROM %s 
			WHERE id = (
				SELECT id FROM (
					SELECT id FROM %s ORDER BY RAND() LIMIT 1
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
			id, id_bigint, id_bigint_unsigned,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned, price_decimal, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, name_enum, created_date,
			created_time, created_timestamp, is_active, json_data,
			long_varchar, name_bool
		) VALUES (
			%d, 1234567890, 1234567891,
			100, 101, 102, 103,
			5001, 5002, 101, 102,
			50, 51,
			%.2f, %.3f,
			%.3f, %.2f, %.2f, %.3f,
			'char_val_1', 'varchar_val_2', 'text_val_1', 'tinytext_val_2',
			'mediumtext_val_1', 'longtext_val_2', 'Medium', '2023-01-01 12:00:00',
			'12:00:00', '2023-01-01 12:00:00', 1, '{"key": "value_1"}',
			'long_varchar_val_2', 1
		)`, tableName, i, 123.45, 123.456, 123.456, 123.45, 123.45, 123.456)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

// testAndBuildAbstractDriver initializes and returns an AbstractDriver with default configuration
func testAndBuildAbstractDriver(t *testing.T) (*sqlx.DB, *abstract.AbstractDriver) {
	t.Helper()
	logger.Init()

	config := Config{
		Username:   defaultMySQLUser,
		Host:       defaultMySQLHost,
		Port:       defaultMySQLPort,
		Password:   defaultMySQLPassword,
		Database:   defaultMySQLDatabase,
		MaxThreads: defaultMaxThreads,
		RetryCount: defaultRetryCount,
	}

	mysqlDriver := &MySQL{
		config: &config,
		cdcConfig: CDC{
			InitialWaitTime: initialCDCWaitTime,
		},
	}
	mysqlDriver.CDCSupport = true
	absDriver := abstract.NewAbstractDriver(context.Background(), mysqlDriver)

	state := &types.State{
		Type:    types.StreamType,
		RWMutex: &sync.RWMutex{},
	}
	absDriver.SetupState(state)
	require.NoError(t, absDriver.Setup(context.Background()), "Failed to setup MySQL driver")

	return mysqlDriver.client, absDriver
}
