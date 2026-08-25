package mysql

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// performanceCDCStreams is the CDC stream set the performance suite drives, shared between the
// PerformanceTest config and the perf operations below.
var performanceCDCStreams = []string{"trips_cdc", "fhv_trips_cdc"}

// ExecuteQuery executes MySQL queries for testing based on the operation type
func ExecuteQuery(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
	t.Helper()
	ExecuteQueryExcluding(ctx, t, conf, operation, nil)
}

// versionedSeedColumns are the columns TestMySQLCompatibility can leave out of the seed data for old
// baselines (CompatibilityColumnRule.ExcludeBelow); every other suite seeds all of them.
var versionedSeedColumns = []struct {
	name, ddl, value, filteredValue, updateExpr string
}{
	{"name_ucs2", "name_ucs2 VARCHAR(100) CHARACTER SET ucs2", "'ucs2_val'", "'filtered ucs2'", "name_ucs2 = 'updated ucs2'"},
	{"name_utf16le", "name_utf16le VARCHAR(100) CHARACTER SET utf16le", "'utf16le_val'", "'filtered utf16le'", "name_utf16le = 'updated utf16le'"},
	{"grade", "grade ENUM('naïve','café','résumé') CHARACTER SET latin1", "'naïve'", "'naïve'", "grade = 'café'"},
}

// seedColumnFragments renders the versioned columns NOT being excluded as the fragments each seed
// statement splices in after name_latin1; excluding nothing reproduces the full fixture.
func seedColumnFragments(t *testing.T, excluded []string) (ddl, cols, vals, filteredVals, updates string) {
	t.Helper()
	supported := make([]string, 0, len(versionedSeedColumns))
	for _, col := range versionedSeedColumns {
		supported = append(supported, col.name)
	}
	drop, err := testutils.SeedColumnsExcluded(excluded, supported)
	require.NoError(t, err, "mysql seed exclusion")

	var names, values, filtered, sets []string
	for _, col := range versionedSeedColumns {
		if drop[col.name] {
			continue
		}
		ddl += "\n\t\t" + col.ddl + ","
		names = append(names, col.name)
		values = append(values, col.value)
		filtered = append(filtered, col.filteredValue)
		sets = append(sets, col.updateExpr)
	}
	if len(names) == 0 {
		return "", "", "", "", ""
	}
	join := func(parts []string) string { return " " + strings.Join(parts, ", ") + "," }
	return ddl, join(names), join(values), join(filtered), join(sets)
}

// ExecuteQueryExcluding is ExecuteQuery with columns left out of the seed DDL and DML entirely --
// the compatibility suite's seed exclusion for columns an old baseline cannot sync at any price.
func ExecuteQueryExcluding(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string, excludedColumns []string) {
	t.Helper()

	seedDDL, seedCols, seedVals, seedFilteredVals, seedUpdates := seedColumnFragments(t, excludedColumns)

	var connStr, database string
	config := conf.SourceBaseConfig
	database = config.String("database")
	// the mysql driver spells its single host "hosts"
	connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.String("username"),
		config.String("password"),
		config.Host("hosts"),
		config.Int("port"),
		database)
	db, err := sqlx.ConnectContext(ctx, "mysql", connStr)
	require.NoError(t, err, "failed to connect to  mysql")
	defer func() {
		require.NoError(t, db.Close())
	}()

	// integration test uses only one stream for testing
	integrationTestTable := conf.GetTableName()
	var query string

	switch operation {
	case "create":
		query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT UNSIGNED NOT NULL AUTO_INCREMENT,
				id_bigint BIGINT,
				id_int INT,
				id_cursor INT,
				id_int_unsigned INT UNSIGNED,
				id_integer INT,
				id_integer_unsigned INT UNSIGNED,
				id_mediumint MEDIUMINT,
				id_mediumint_unsigned MEDIUMINT UNSIGNED,
				id_smallint SMALLINT,
				id_smallint_unsigned SMALLINT UNSIGNED,
				id_tinyint TINYINT,
				id_tinyint_unsigned TINYINT UNSIGNED,
				id_tinyint_unsigned_max TINYINT UNSIGNED,
				id_smallint_unsigned_max SMALLINT UNSIGNED,
				id_mediumint_unsigned_max MEDIUMINT UNSIGNED,
				id_mediumint_unsigned_signbit MEDIUMINT UNSIGNED,
				id_int_unsigned_max INT UNSIGNED,
				id_bigint_unsigned BIGINT UNSIGNED,
				id_bigint_unsigned_signbit BIGINT UNSIGNED,
				id_bigint_unsigned_max BIGINT UNSIGNED,
				price_decimal DECIMAL(10,2),
				amount_decimal_9_2 DECIMAL(9,2),
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
		status ENUM('active','inactive','pending') DEFAULT NULL,
		priority ENUM('low','medium','high') DEFAULT 'low',
		name_latin1 VARCHAR(100) CHARACTER SET latin1,%s
		tags SET('sports','music','gaming','reading') DEFAULT NULL,
		permissions SET('read','write','execute') CHARACTER SET latin1 DEFAULT NULL,
		PRIMARY KEY (id),
		excludedColumn INT
	)`, integrationTestTable, seedDDL)

	case "drop":
		query = fmt.Sprintf("DROP TABLE IF EXISTS %s", integrationTestTable)

	case "drop-all":
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", database))
		require.NoError(t, err, "failed to drop database %s", database)
		query = fmt.Sprintf("CREATE DATABASE `%s`", database)

	case "clean":
		query = fmt.Sprintf("DELETE FROM %s", integrationTestTable)

	case "add":
		insertTestData(ctx, t, db, integrationTestTable, excludedColumns)
		return // Early return since we handle all inserts in the helper function

	case "insert":
		query = fmt.Sprintf(`
			INSERT INTO %s (
			id_cursor, id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned,
			id_tinyint_unsigned_max, id_smallint_unsigned_max,
			id_mediumint_unsigned_max, id_mediumint_unsigned_signbit, id_int_unsigned_max,
			id_bigint_unsigned, id_bigint_unsigned_signbit, id_bigint_unsigned_max,
			price_decimal, amount_decimal_9_2, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active,
			long_varchar, name_bool, status, priority,
			name_latin1,%s
			tags, permissions,
			excludedColumn
		) VALUES (
			6, 6, 123456789012345,
			100, 4294967295, 102, 4294967294,
			5001, 5002, 101, 102,
			50, 51,
			255, 65535,
			16777215, 8388608, 4294967295,
			5003, 9223372036854775808, 18446744073709551615,
			123.45, 5330197.27, 123.456,
			123.456,  123.45, 123.45, 123.456,
			'c', 'varchar_val', 'text_val', 'tinytext_val',
			'mediumtext_val', 'longtext_val', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00', 1,
			'long_varchar_val', 1, 'active', 'high',
			'latin1_val',%s
			'sports,reading', 'read,write',
			101
		)`, integrationTestTable, seedCols, seedVals)
		_, err = db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to execute %s operation", operation)
		// insert a filtered doc, it would be filtered out by the filter, won't be synced into the destination
		filteredQuery := fmt.Sprintf(`
			INSERT INTO %s (
			id_cursor, id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned,
			id_tinyint_unsigned_max, id_smallint_unsigned_max,
			id_mediumint_unsigned_max, id_mediumint_unsigned_signbit, id_int_unsigned_max,
			id_bigint_unsigned, id_bigint_unsigned_signbit, id_bigint_unsigned_max,
			price_decimal, amount_decimal_9_2, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active,
			long_varchar, name_bool, status, priority,
			name_latin1,%s
			tags, permissions,
			excludedColumn
		) VALUES (
			-1, 999, 111111111111111,
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0,
			0, 0,
			0, 0, 0,
			0, 0, 0,
			50.123, 50.12, 50.123,
			50.123, 50.0, 50.123, 50.123,
			'x', 'filtered_val', 'filtered text', 'filtered tiny',
			'filtered medium', 'filtered long', '2022-06-15 10:00:00',
			'2021-06-15 10:00:00', 0,
			'filtered long varchar', 0, 'inactive', 'low',
			'filtered latin1',%s
			'music', 'execute',
			200
		)`, integrationTestTable, seedCols, seedFilteredVals)
		_, err = db.ExecContext(ctx, filteredQuery)
		require.NoError(t, err, "Failed to insert filtered test data row")
		return

	case "insert_2pc":
		query = fmt.Sprintf(`
			INSERT INTO %s (
			id_cursor, id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned,
			id_tinyint_unsigned_max, id_smallint_unsigned_max,
			id_mediumint_unsigned_max, id_mediumint_unsigned_signbit, id_int_unsigned_max,
			id_bigint_unsigned, id_bigint_unsigned_signbit, id_bigint_unsigned_max,
			price_decimal, amount_decimal_9_2, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active,
			long_varchar, name_bool, status, priority,
			name_latin1,%s
			tags, permissions
		) VALUES (
			7, 7, 123456789012345,
			100, 4294967295, 102, 4294967294,
			5001, 5002, 101, 102,
			50, 51,
			255, 65535,
			16777215, 8388608, 4294967295,
			5003, 9223372036854775808, 18446744073709551615,
			123.45, 5330197.27, 123.456,
			123.456,  123.45, 123.45, 123.456,
			'c', 'varchar_val', 'text_val', 'tinytext_val',
			'mediumtext_val', 'longtext_val', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00', 1,
			'long_varchar_val', 1, 'active', 'high',
			'latin1_val',%s
			'sports,reading', 'read,write'
		)`, integrationTestTable, seedCols, seedVals)

	case "update":
		query = fmt.Sprintf(`
			UPDATE %s SET
				id_cursor = NULL,
				id_bigint = 987654321098765,
				id_int = 200, id_int_unsigned = 4294967293,
				id_integer = 202, id_integer_unsigned = 4294967292,
				id_mediumint = 6001, id_mediumint_unsigned = 6002,
				id_smallint = 201, id_smallint_unsigned = 202,
				id_tinyint = 60, id_tinyint_unsigned = 61,
				id_tinyint_unsigned_max = 254, id_smallint_unsigned_max = 65534,
				id_mediumint_unsigned_max = 16777214, id_mediumint_unsigned_signbit = 8388609,
				id_int_unsigned_max = 4294967294,
				id_bigint_unsigned = 6003,
				id_bigint_unsigned_signbit = 9223372036854775809,
				id_bigint_unsigned_max = 18446744073709551614,
				price_decimal = 543.21, amount_decimal_9_2 = 1234567.89, price_double = 654.321,
				price_double_precision = 654.321, price_float = 543.21,
				price_numeric = 543.21, price_real = 654.321,
				name_char = 'X', name_varchar = 'updated varchar',
				name_text = 'updated text', name_tinytext = 'upd tiny',
				name_mediumtext = 'upd medium', name_longtext = 'upd long',
				created_date = '2024-07-01 15:30:00',
				created_timestamp = '2024-07-01 15:30:00', is_active = 0,
				long_varchar = 'updated long...', name_bool = 0,
			status = 'pending', priority = 'low',
			name_latin1 = 'updated latin1',%s
			tags = 'gaming,reading', permissions = 'read,write,execute',
			excludedColumn = 102,
			includedColumn = 202
		WHERE id = 1`, integrationTestTable, seedUpdates)

	case "delete":
		query = fmt.Sprintf("DELETE FROM %s WHERE id = 1", integrationTestTable)

	case "setup_cdc":
		backfillStreams := performance.GetBackfillStreamsFromCDC(performanceCDCStreams)
		// truncate the cdc tables
		for idx, cdcStream := range performanceCDCStreams {
			_, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", cdcStream))
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
			// mysql chunking strategy does not support 0 record sync
			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE id > 15000000 LIMIT 1", cdcStream, backfillStreams[idx]))
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		}
		return

	case "reset_cdc_config":
		cdcSettings := map[string]string{
			"binlog_format":       "ROW",
			"binlog_row_image":    "FULL",
			"binlog_row_metadata": "FULL",
		}
		for variable, value := range cdcSettings {
			_, err := db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL %s = '%s'", variable, value))
			require.NoError(t, err, fmt.Sprintf("failed to SET GLOBAL %s = %s", variable, value))
		}
		return

	case "bulk_cdc_data_insert":
		backfillStreams := performance.GetBackfillStreamsFromCDC(performanceCDCStreams)
		// insert the data into the cdc tables concurrently
		err := testutils.Concurrent(ctx, performanceCDCStreams, len(performanceCDCStreams), func(ctx context.Context, cdcStream string, executionNumber int) error {
			_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s LIMIT 15000000", cdcStream, backfillStreams[executionNumber]))
			return err
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		return

	case "evolve-schema":
		query = fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN id_int BIGINT, MODIFY COLUMN price_float DOUBLE, ADD COLUMN includedColumn INT;", integrationTestTable)

	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}

	_, err = db.ExecContext(ctx, query)
	require.NoError(t, err, "Failed to execute %s operation", operation)
}

// insertTestData inserts test data into the specified table
func insertTestData(ctx context.Context, t *testing.T, db *sqlx.DB, tableName string, excludedColumns []string) {
	t.Helper()

	_, seedCols, seedVals, seedFilteredVals, _ := seedColumnFragments(t, excludedColumns)
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf(`
		INSERT INTO %s (
			id_cursor, id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned,
			id_tinyint_unsigned_max, id_smallint_unsigned_max,
			id_mediumint_unsigned_max, id_mediumint_unsigned_signbit, id_int_unsigned_max,
			id_bigint_unsigned, id_bigint_unsigned_signbit, id_bigint_unsigned_max,
			price_decimal, amount_decimal_9_2, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active, long_varchar, name_bool, status, priority,
			name_latin1,%s
			tags, permissions,
			excludedColumn
		) VALUES (
			%d, %d, 123456789012345,
			100, 4294967295, 102, 4294967294,
			5001, 5002, 101, 102,
			50, 51,
			255, 65535,
			16777215, 8388608, 4294967295,
			5003, 9223372036854775808, 18446744073709551615,
			123.45, 5330197.27, 123.456,
			123.456,  123.45, 123.45, 123.456,
			'c', 'varchar_val', 'text_val', 'tinytext_val',
			'mediumtext_val', 'longtext_val', '2023-01-01 12:00:00',
			'2023-01-01 12:00:00', 1, 'long_varchar_val', 1, 'active', 'high',
			'latin1_val',%s
			'sports,reading', 'read,write',
			100
		)`, tableName, seedCols, i, i, seedVals)

		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
	// insert a filtered doc, it would be filtered out by the filter, won't be synced into the destination
	filteredQuery := fmt.Sprintf(`
		INSERT INTO %s (
			id_cursor, id, id_bigint,
			id_int, id_int_unsigned, id_integer, id_integer_unsigned,
			id_mediumint, id_mediumint_unsigned, id_smallint, id_smallint_unsigned,
			id_tinyint, id_tinyint_unsigned,
			id_tinyint_unsigned_max, id_smallint_unsigned_max,
			id_mediumint_unsigned_max, id_mediumint_unsigned_signbit, id_int_unsigned_max,
			id_bigint_unsigned, id_bigint_unsigned_signbit, id_bigint_unsigned_max,
			price_decimal, amount_decimal_9_2, price_double,
			price_double_precision, price_float, price_numeric, price_real,
			name_char, name_varchar, name_text, name_tinytext,
			name_mediumtext, name_longtext, created_date,
			created_timestamp, is_active, long_varchar, name_bool, status, priority,
			name_latin1,%s
			tags, permissions,
			excludedColumn
		) VALUES (
			-1, 998, 111111111111111,
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0,
			0, 0,
			0, 0, 0,
			0, 0, 0,
			500234.123, 500234.12, 500234.123,
			500234.123, 500234.0, 500234.123, 500234.123,
			'x', 'filtered_val', 'filtered text', 'filtered tiny',
			'filtered medium', 'filtered long', '2021-06-15 10:00:00',
			'2021-06-15 10:00:00', 0, 'filtered long varchar', 0, 'inactive', 'low',
			'filtered latin1',%s
			'music', 'execute',
			200
		)`, tableName, seedCols, seedFilteredVals)
	_, err := db.ExecContext(ctx, filteredQuery)
	require.NoError(t, err, "Failed to insert filtered test data row")
}

// The id_int_unsigned / id_integer_unsigned values are deliberately ABOVE int32 range (max
// INT UNSIGNED is 4294967295). That is what makes state version 4 observable: at v>=4 the driver
// reinterprets the raw bits as uint32 and the value survives as int64, while at v<=3 it strips the
// "unsigned " prefix, maps to Int32, and the same bits read back as -1 / -2 -- the overflow
// constants/state_version.go's version 4 note describes. Keep them above 2^31-1; small values
// make the gate invisible because they fit in both types.
// TODO: olake has no uint64 data type, so the id_bigint_unsigned_* values past MaxInt64 pin what
// olake writes today, not what MySQL stored.
var ExpectedMySQLData = map[string]interface{}{
	"id_bigint":             int64(123456789012345),
	"id_int":                int32(100),
	"id_int_unsigned":       int64(4294967295),
	"id_integer":            int32(102),
	"id_integer_unsigned":   int64(4294967294),
	"id_mediumint":          int32(5001),
	"id_mediumint_unsigned": int32(5002),
	"id_smallint":           int32(101),
	"id_smallint_unsigned":  int32(102),
	"id_tinyint":            int32(50),
	"id_tinyint_unsigned":   int32(51),
	// unsigned maxima: the binlog reports these as negative signed ints (255 -> int8(-1)), so they
	// only survive if the driver masks the sign extension back off. mediumint is the odd one out:
	// it is 3 bytes wide inside a 4-byte int32, so everything from 8388608 up is sign-extended too
	"id_tinyint_unsigned_max":       int32(255),
	"id_smallint_unsigned_max":      int32(65535),
	"id_mediumint_unsigned_max":     int32(16777215),
	"id_mediumint_unsigned_signbit": int32(8388608),
	"id_int_unsigned_max":           int64(4294967295),
	"id_bigint_unsigned":            int64(5003),
	"id_bigint_unsigned_signbit":    int64(math.MinInt64), // should be 9223372036854775808 (2^63)
	"id_bigint_unsigned_max":        int64(-1),            // should be 18446744073709551615 (2^64-1)
	"price_decimal":                 float64(123.45),
	"amount_decimal_9_2":            float64(5330197.27),
	"price_double":                  float64(123.456),
	"price_double_precision":        float64(123.456),
	"price_float":                   float32(123.45),
	"price_numeric":                 float64(123.45),
	"price_real":                    float64(123.456),
	"name_char":                     "c",
	"name_varchar":                  "varchar_val",
	"name_text":                     "text_val",
	"name_tinytext":                 "tinytext_val",
	"name_mediumtext":               "mediumtext_val",
	"name_longtext":                 "longtext_val",
	"created_date":                  arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_timestamp":             arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"is_active":                     int32(1),
	"long_varchar":                  "long_varchar_val",
	"name_bool":                     int32(1),
	"status":                        "active",
	"priority":                      "high",
	"name_latin1":                   "latin1_val",
	"name_ucs2":                     "ucs2_val",
	"name_utf16le":                  "utf16le_val",
	"grade":                         "naïve",
	"tags":                          "sports,reading",
	"permissions":                   "read,write",
}

// TODO: olake has no uint64 data type, so the id_bigint_unsigned_* values past MaxInt64 pin what
// olake writes today, not what MySQL stored.
var ExpectedUpdatedData = map[string]interface{}{
	"id_bigint":                     int64(987654321098765),
	"id_int":                        int64(200),
	"id_int_unsigned":               int64(4294967293),
	"id_integer":                    int32(202),
	"id_integer_unsigned":           int64(4294967292),
	"id_mediumint":                  int32(6001),
	"id_mediumint_unsigned":         int32(6002),
	"id_smallint":                   int32(201),
	"id_smallint_unsigned":          int32(202),
	"id_tinyint":                    int32(60),
	"id_tinyint_unsigned":           int32(61),
	"id_tinyint_unsigned_max":       int32(254),
	"id_smallint_unsigned_max":      int32(65534),
	"id_mediumint_unsigned_max":     int32(16777214),
	"id_mediumint_unsigned_signbit": int32(8388609),
	"id_int_unsigned_max":           int64(4294967294),
	"id_bigint_unsigned":            int64(6003),
	"id_bigint_unsigned_signbit":    int64(math.MinInt64 + 1), // should be 9223372036854775809 (2^63+1)
	"id_bigint_unsigned_max":        int64(-2),                // should be 18446744073709551614 (2^64-2)
	"price_decimal":                 float64(543.21),
	"amount_decimal_9_2":            float64(1234567.89),
	"price_double":                  float64(654.321),
	"price_double_precision":        float64(654.321),
	"price_float":                   float64(543.21),
	"price_numeric":                 float64(543.21),
	"price_real":                    float64(654.321),
	"name_char":                     "X",
	"name_varchar":                  "updated varchar",
	"name_text":                     "updated text",
	"name_tinytext":                 "upd tiny",
	"name_mediumtext":               "upd medium",
	"name_longtext":                 "upd long",
	"created_date":                  arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_timestamp":             arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"is_active":                     int32(0),
	"long_varchar":                  "updated long...",
	"name_bool":                     int32(0),
	"status":                        "pending",
	"priority":                      "low",
	"name_latin1":                   "updated latin1",
	"name_ucs2":                     "updated ucs2",
	"name_utf16le":                  "updated utf16le",
	"grade":                         "café",
	"tags":                          "gaming,reading",
	"permissions":                   "read,write,execute",
	"includedcolumn":                int32(202),
}

var MySQLToDestinationSchema = map[string]string{
	"id":                    "bigint",
	"id_bigint":             "bigint",
	"id_int":                "int",
	"id_int_unsigned":       "bigint",
	"id_integer":            "int",
	"id_integer_unsigned":   "bigint",
	"id_mediumint":          "mediumint",
	"id_mediumint_unsigned": "unsigned mediumint",
	"id_smallint":           "smallint",
	"id_smallint_unsigned":  "unsigned smallint",
	"id_tinyint":            "tinyint",
	"id_tinyint_unsigned":   "unsigned tinyint",
	// the unsigned maxima only fit their mapped type once the sign extension is masked off
	"id_tinyint_unsigned_max":       "unsigned tinyint",
	"id_smallint_unsigned_max":      "unsigned smallint",
	"id_mediumint_unsigned_max":     "unsigned mediumint",
	"id_mediumint_unsigned_signbit": "unsigned mediumint",
	"id_int_unsigned_max":           "bigint",
	"id_bigint_unsigned":            "bigint",
	"id_bigint_unsigned_signbit":    "bigint",
	"id_bigint_unsigned_max":        "bigint",
	"price_decimal":                 "decimal",
	"price_double":                  "double",
	"price_double_precision":        "double",
	"price_float":                   "float",
	"price_numeric":                 "decimal",
	"price_real":                    "double",
	"name_char":                     "char",
	"name_varchar":                  "varchar",
	"name_text":                     "text",
	"name_tinytext":                 "tinytext",
	"name_mediumtext":               "mediumtext",
	"name_longtext":                 "longtext",
	"created_date":                  "datetime",
	"created_timestamp":             "timestamp",
	"is_active":                     "tinyint",
	"long_varchar":                  "mediumtext",
	"name_bool":                     "tinyint",
	"status":                        "enum",
	"priority":                      "enum",
	"name_latin1":                   "varchar",
	"name_ucs2":                     "varchar",
	"name_utf16le":                  "varchar",
	"grade":                         "enum",
	"tags":                          "set",
	"permissions":                   "set",
}

var EvolvedMySQLToDestinationSchema = map[string]string{
	"id":                    "bigint",
	"id_bigint":             "bigint",
	"id_int":                "bigint",
	"id_int_unsigned":       "bigint",
	"id_integer":            "int",
	"id_integer_unsigned":   "bigint",
	"id_mediumint":          "mediumint",
	"id_mediumint_unsigned": "unsigned mediumint",
	"id_smallint":           "smallint",
	"id_smallint_unsigned":  "unsigned smallint",
	"id_tinyint":            "tinyint",
	"id_tinyint_unsigned":   "unsigned tinyint",
	// the unsigned maxima only fit their mapped type once the sign extension is masked off
	"id_tinyint_unsigned_max":       "unsigned tinyint",
	"id_smallint_unsigned_max":      "unsigned smallint",
	"id_mediumint_unsigned_max":     "unsigned mediumint",
	"id_mediumint_unsigned_signbit": "unsigned mediumint",
	"id_int_unsigned_max":           "bigint",
	"id_bigint_unsigned":            "bigint",
	"id_bigint_unsigned_signbit":    "bigint",
	"id_bigint_unsigned_max":        "bigint",
	"price_decimal":                 "decimal",
	"amount_decimal_9_2":            "decimal",
	"price_double":                  "double",
	"price_double_precision":        "double",
	"price_float":                   "double",
	"price_numeric":                 "decimal",
	"price_real":                    "double",
	"name_char":                     "char",
	"name_varchar":                  "varchar",
	"name_text":                     "text",
	"name_tinytext":                 "tinytext",
	"name_mediumtext":               "mediumtext",
	"name_longtext":                 "longtext",
	"created_date":                  "datetime",
	"created_timestamp":             "timestamp",
	"is_active":                     "tinyint",
	"long_varchar":                  "mediumtext",
	"name_bool":                     "tinyint",
	"status":                        "enum",
	"priority":                      "enum",
	"name_latin1":                   "varchar",
	"name_ucs2":                     "varchar",
	"name_utf16le":                  "varchar",
	"grade":                         "enum",
	"tags":                          "set",
	"permissions":                   "set",
	"includedcolumn":                "int",
}
var ExpectedMySQLDefaultCDCColumnsSchema = map[string]string{
	"_cdc_timestamp":        "timestamp",
	"_cdc_binlog_file_name": "string",
	"_cdc_binlog_file_pos":  "bigint",
}
