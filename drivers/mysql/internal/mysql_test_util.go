package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// ExecuteQuery executes MySQL queries for testing based on the operation type
func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var connStr string
	if fileConfig {
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			config.Username,
			config.Password,
			config.Host,
			config.Port,
			config.Database)
	} else {
		connStr = "mysql:secret1234@tcp(localhost:3306)/olake_mysql_test?parseTime=true"
	}
	db, err := sqlx.ConnectContext(ctx, "mysql", connStr)
	require.NoError(t, err, "failed to connect to  mysql")
	defer func() {
		require.NoError(t, db.Close())
	}()

	// integration test uses only one stream for testing
	integrationTestTable := streams[0]
	var query string

	switch operation {
	case "create":
		query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT UNSIGNED NOT NULL AUTO_INCREMENT,
				id_bigint BIGINT,
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
				created_date DATETIME,
				created_timestamp TIMESTAMP NULL,
				is_active TINYINT(1),
				long_varchar MEDIUMTEXT,
				name_bool TINYINT(1) DEFAULT '1',
				PRIMARY KEY (id)
			)`, integrationTestTable)

	case "drop":
		query = fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTestTable)

	case "clean":
		query = fmt.Sprintf("DELETE FROM %s", integrationTestTable)

	case "add":
		insertTestData(t, ctx, db, integrationTestTable)
		return // Early return since we handle all inserts in the helper function

	case "insert":
		query = fmt.Sprintf(`
			INSERT INTO %s (
			id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned, price_decimal, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active,
			long_varchar, name_bool
		) VALUES (
			6, 123456789012345,
			100, 101, 102, 103,
			5001, 5002, 101, 102,
			50, 51,
			123.45, 123.456,
			123.456,  123.45, 123.45, 123.456,
			'c', 'varchar_val', 'text_val', 'tinytext_val',
			'mediumtext_val', 'longtext_val', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00', 1,
			'long_varchar_val', 1
		)`, integrationTestTable)

	case "update":
		query = fmt.Sprintf(`
			UPDATE %s SET
				id_bigint = 987654321098765,
				id_int = 200, id_int_unsigned = 201,
				id_integer = 202, id_integer_unsigned = 203,
				id_mediumint = 6001, id_mediumint_unsigned = 6002,
				id_smallint = 201, id_smallint_unsigned = 202,
				id_tinyint = 60, id_tinyint_unsigned = 61,
				price_decimal = 543.21, price_double = 654.321,
				price_double_precision = 654.321, price_float = 543.21,
				price_numeric = 543.21, price_real = 654.321,
				name_char = 'X', name_varchar = 'updated varchar',
				name_text = 'updated text', name_tinytext = 'upd tiny',
				name_mediumtext = 'upd medium', name_longtext = 'upd long',
				created_date = '2024-07-01 15:30:00',
				created_timestamp = '2024-07-01 15:30:00', is_active = 0,
				long_varchar = 'updated long...', name_bool = 0
			WHERE id = 1`, integrationTestTable)

	case "delete":
		query = fmt.Sprintf("DELETE FROM %s WHERE id = 1", integrationTestTable)

	case "setup_cdc":
		backfillStreams := testutils.GetBackfillStreamsFromCDC(streams)
		// truncate the cdc tables
		for idx, cdcStream := range streams {
			_, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", cdcStream))
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
			// mysql chunking strategy does not support 0 record sync
			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE id > 15000000 LIMIT 1", cdcStream, backfillStreams[idx]))
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		}
		return

	case "bulk_cdc_data_insert":
		backfillStreams := testutils.GetBackfillStreamsFromCDC(streams)
		// insert the data into the cdc tables concurrently
		err := utils.Concurrent(ctx, streams, len(streams), func(ctx context.Context, cdcStream string, executionNumber int) error {
			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s LIMIT 15000000", cdcStream, backfillStreams[executionNumber-1]))
			if err != nil {
				return err
			}
			return nil
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		return

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
			id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned, price_decimal, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active, long_varchar, name_bool
		) VALUES (
			%d, 123456789012345,
			100, 101, 102, 103,
			5001, 5002, 101, 102,
			50, 51,
			123.45, 123.456,
			123.456,  123.45, 123.45, 123.456,
			'c', 'varchar_val', 'text_val', 'tinytext_val',
			'mediumtext_val', 'longtext_val', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00', 1, 'long_varchar_val', 1
		)`, tableName, i)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

var ExpectedMySQLData = map[string]interface{}{
	"id_bigint":              int64(123456789012345),
	"id_int":                 int32(100),
	"id_int_unsigned":        int32(101),
	"id_integer":             int32(102),
	"id_integer_unsigned":    int32(103),
	"id_mediumint":           int32(5001),
	"id_mediumint_unsigned":  int32(5002),
	"id_smallint":            int32(101),
	"id_smallint_unsigned":   int32(102),
	"id_tinyint":             int32(50),
	"id_tinyint_unsigned":    int32(51),
	"price_decimal":          float32(123.45),
	"price_double":           float64(123.456),
	"price_double_precision": float64(123.456),
	"price_float":            float32(123.45),
	"price_numeric":          float32(123.45),
	"price_real":             float64(123.456),
	"name_char":              "c",
	"name_varchar":           "varchar_val",
	"name_text":              "text_val",
	"name_tinytext":          "tinytext_val",
	"name_mediumtext":        "mediumtext_val",
	"name_longtext":          "longtext_val",
	"created_date":           arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_timestamp":      arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"is_active":              int32(1),
	"long_varchar":           "long_varchar_val",
	"name_bool":              int32(1),
}

var ExpectedUpdatedMySQLData = map[string]interface{}{
	"id_bigint":              int64(987654321098765),
	"id_int":                 int32(200),
	"id_int_unsigned":        int32(201),
	"id_integer":             int32(202),
	"id_integer_unsigned":    int32(203),
	"id_mediumint":           int32(6001),
	"id_mediumint_unsigned":  int32(6002),
	"id_smallint":            int32(201),
	"id_smallint_unsigned":   int32(202),
	"id_tinyint":             int32(60),
	"id_tinyint_unsigned":    int32(61),
	"price_decimal":          float32(543.21),
	"price_double":           float64(654.321),
	"price_double_precision": float64(654.321),
	"price_float":            float32(543.21),
	"price_numeric":          float32(543.21),
	"price_real":             float64(654.321),
	"name_char":              "X",
	"name_varchar":           "updated varchar",
	"name_text":              "updated text",
	"name_tinytext":          "upd tiny",
	"name_mediumtext":        "upd medium",
	"name_longtext":          "upd long",
	"created_date":           arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_timestamp":      arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"is_active":              int32(0),
	"long_varchar":           "updated long...",
	"name_bool":              int32(0),
}

var MySQLToIcebergSchema = map[string]string{
	"id":                     "unsigned int",
	"id_bigint":              "bigint",
	"id_int":                 "int",
	"id_int_unsigned":        "unsigned int",
	"id_integer":             "int",
	"id_integer_unsigned":    "unsigned int",
	"id_mediumint":           "mediumint",
	"id_mediumint_unsigned":  "unsigned mediumint",
	"id_smallint":            "smallint",
	"id_smallint_unsigned":   "unsigned smallint",
	"id_tinyint":             "tinyint",
	"id_tinyint_unsigned":    "unsigned tinyint",
	"price_decimal":          "decimal",
	"price_double":           "double",
	"price_double_precision": "double",
	"price_float":            "float",
	"price_numeric":          "decimal",
	"price_real":             "double",
	"name_char":              "char",
	"name_varchar":           "varchar",
	"name_text":              "text",
	"name_tinytext":          "tinytext",
	"name_mediumtext":        "mediumtext",
	"name_longtext":          "longtext",
	"created_date":           "datetime",
	"created_timestamp":      "timestamp",
	"is_active":              "tinyint",
	"long_varchar":           "mediumtext",
	"name_bool":              "tinyint",
}
