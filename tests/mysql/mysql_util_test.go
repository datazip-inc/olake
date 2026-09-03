package mysql

import (
	"context"
	"fmt"
	"math"
	"strconv"
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

type seedColumn struct {
	name     string
	datatype string
	value    string
	filtered string
	updated  string
}

func (c seedColumn) definition() string { return c.name + " " + c.datatype }

var seedColumns = []seedColumn{
	{name: "id", datatype: "INT UNSIGNED NOT NULL AUTO_INCREMENT", value: "", filtered: "", updated: ""},
	{name: "id_bigint", datatype: "BIGINT", value: "123456789012345", filtered: "111111111111111", updated: "987654321098765"},
	{name: "id_int", datatype: "INT", value: "100", filtered: "0", updated: "200"},
	{name: "id_cursor", datatype: "INT", value: "", filtered: "-1", updated: "NULL"},
	{name: "id_int_unsigned", datatype: "INT UNSIGNED", value: "4294967295", filtered: "0", updated: "4294967293"},
	{name: "id_integer", datatype: "INT", value: "102", filtered: "0", updated: "202"},
	{name: "id_integer_unsigned", datatype: "INT UNSIGNED", value: "4294967294", filtered: "0", updated: "4294967292"},
	{name: "id_mediumint", datatype: "MEDIUMINT", value: "5001", filtered: "0", updated: "6001"},
	{name: "id_mediumint_unsigned", datatype: "MEDIUMINT UNSIGNED", value: "5002", filtered: "0", updated: "6002"},
	{name: "id_smallint", datatype: "SMALLINT", value: "101", filtered: "0", updated: "201"},
	{name: "id_smallint_unsigned", datatype: "SMALLINT UNSIGNED", value: "102", filtered: "0", updated: "202"},
	{name: "id_tinyint", datatype: "TINYINT", value: "50", filtered: "0", updated: "60"},
	{name: "id_tinyint_unsigned", datatype: "TINYINT UNSIGNED", value: "51", filtered: "0", updated: "61"},
	{name: "id_tinyint_unsigned_max", datatype: "TINYINT UNSIGNED", value: "255", filtered: "0", updated: "254"},
	{name: "id_smallint_unsigned_max", datatype: "SMALLINT UNSIGNED", value: "65535", filtered: "0", updated: "65534"},
	{name: "id_mediumint_unsigned_max", datatype: "MEDIUMINT UNSIGNED", value: "16777215", filtered: "0", updated: "16777214"},
	{name: "id_mediumint_unsigned_signbit", datatype: "MEDIUMINT UNSIGNED", value: "8388608", filtered: "0", updated: "8388609"},
	{name: "id_int_unsigned_max", datatype: "INT UNSIGNED", value: "4294967295", filtered: "0", updated: "4294967294"},
	{name: "price_decimal", datatype: "DECIMAL(10,2)", value: "123.45", filtered: "50.123", updated: "543.21"},
	{name: "amount_decimal_9_2", datatype: "DECIMAL(9,2)", value: "5330197.27", filtered: "50.12", updated: "1234567.89"},
	{name: "price_double", datatype: "DOUBLE", value: "123.456", filtered: "50.123", updated: "654.321"},
	{name: "price_double_precision", datatype: "DOUBLE", value: "123.456", filtered: "50.123", updated: "654.321"},
	{name: "price_float", datatype: "FLOAT", value: "123.45", filtered: "50.0", updated: "543.21"},
	{name: "price_numeric", datatype: "DECIMAL(10,2)", value: "123.45", filtered: "50.123", updated: "543.21"},
	{name: "price_real", datatype: "DOUBLE", value: "123.456", filtered: "50.123", updated: "654.321"},
	{name: "name_char", datatype: "CHAR(50)", value: "'c'", filtered: "'x'", updated: "'X'"},
	{name: "name_varchar", datatype: "VARCHAR(100)", value: "'varchar_val'", filtered: "'filtered_val'", updated: "'updated varchar'"},
	{name: "name_text", datatype: "TEXT", value: "'text_val'", filtered: "'filtered text'", updated: "'updated text'"},
	{name: "name_tinytext", datatype: "TINYTEXT", value: "'tinytext_val'", filtered: "'filtered tiny'", updated: "'upd tiny'"},
	{name: "name_mediumtext", datatype: "MEDIUMTEXT", value: "'mediumtext_val'", filtered: "'filtered medium'", updated: "'upd medium'"},
	{name: "name_longtext", datatype: "LONGTEXT", value: "'longtext_val'", filtered: "'filtered long'", updated: "'upd long'"},
	{name: "created_date", datatype: "DATETIME", value: "'2023-01-01 12:00:00'", filtered: "'2022-06-15 10:00:00'", updated: "'2024-07-01 15:30:00'"},
	{name: "created_timestamp", datatype: "TIMESTAMP NULL", value: "'2023-01-01 12:00:00'", filtered: "'2021-06-15 10:00:00'", updated: "'2024-07-01 15:30:00'"},
	{name: "is_active", datatype: "TINYINT(1)", value: "1", filtered: "0", updated: "0"},
	{name: "long_varchar", datatype: "MEDIUMTEXT", value: "'long_varchar_val'", filtered: "'filtered long varchar'", updated: "'updated long...'"},
	{name: "name_bool", datatype: "TINYINT(1) DEFAULT '1'", value: "1", filtered: "0", updated: "0"},
	{name: "status", datatype: "ENUM('active','inactive','pending') DEFAULT NULL", value: "'active'", filtered: "'inactive'", updated: "'pending'"},
	{name: "priority", datatype: "ENUM('low','medium','high') DEFAULT 'low'", value: "'high'", filtered: "'low'", updated: "'low'"},
	{name: "name_ucs2", datatype: "VARCHAR(100) CHARACTER SET ucs2", value: "'ucs2_val'", filtered: "'filtered ucs2'", updated: "'updated ucs2'"},
	{name: "name_utf16le", datatype: "VARCHAR(100) CHARACTER SET utf16le", value: "'utf16le_val'", filtered: "'filtered utf16le'", updated: "'updated utf16le'"},
	{name: "grade", datatype: "ENUM('naïve','café','résumé') CHARACTER SET latin1", value: "'naïve'", filtered: "'naïve'", updated: "'café'"},
	{name: "name_latin1", datatype: "VARCHAR(100) CHARACTER SET latin1", value: "'latin1_val'", filtered: "'filtered latin1'", updated: "'updated latin1'"},
	{name: "permissions", datatype: "SET('read','write','execute') CHARACTER SET latin1 DEFAULT NULL", value: "'read,write'", filtered: "'execute'", updated: "'read,write,execute'"},
	{name: "id_bigint_unsigned", datatype: "BIGINT UNSIGNED", value: "5003", filtered: "0", updated: "6003"},
	{name: "id_bigint_unsigned_signbit", datatype: "BIGINT UNSIGNED", value: "9223372036854775808", filtered: "0", updated: "9223372036854775809"},
	{name: "id_bigint_unsigned_max", datatype: "BIGINT UNSIGNED", value: "18446744073709551615", filtered: "0", updated: "18446744073709551614"},
	{name: "tags", datatype: "SET('sports','music','gaming','reading') DEFAULT NULL", value: "'sports,reading'", filtered: "'music'", updated: "'gaming,reading'"},
	// binary columns carry bytes that are not valid UTF-8 on purpose; the update writes a short
	// BINARY(16) value so the binlog path has to restore MySQL's 0x00 padding
	{name: "data_binary", datatype: "BINARY(16)", value: "X'123E4567E89B12D3A456426614174000'", filtered: "X'00'", updated: "X'FFFE'"},
	{name: "data_varbinary", datatype: "VARBINARY(64)", value: "X'00FF10FE'", filtered: "X'01'", updated: "X'0102FF'"},
	{name: "data_blob", datatype: "BLOB", value: "X'89504E470D0A1A0A'", filtered: "X'00'", updated: "X'E28228'"},
	{name: "excludedColumn", datatype: "INT", value: "", filtered: "", updated: "102"},
}

// seedColumnTypes derives every seed column's type tags, excluded ones included, so a data_types
// rule in compatibility_rules.json follows a seed edit with nothing to declare.
func seedColumnTypes() map[string][]string {
	types := make(map[string][]string, len(seedColumns))
	for _, col := range seedColumns {
		types[col.name] = testutils.DataTypeTags(col.datatype)
	}
	return types
}

func filterSeedColumns(t *testing.T, excluded []string) []seedColumn {
	t.Helper()
	names := make([]string, 0, len(seedColumns))
	for _, col := range seedColumns {
		names = append(names, col.name)
	}
	drop, err := testutils.SeedColumnsExcluded(excluded, names)
	require.NoError(t, err, "mysql seed exclusion")

	kept := make([]seedColumn, 0, len(seedColumns))
	for _, col := range seedColumns {
		if !drop[col.name] {
			kept = append(kept, col)
		}
	}
	return kept
}

func createTableQuery(table string, cols []seedColumn) string {
	defs := make([]string, 0, len(cols)+1)
	for _, col := range cols {
		defs = append(defs, col.definition())
	}
	defs = append(defs, "PRIMARY KEY (id)")
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)", table, strings.Join(defs, ",\n\t"))
}

func insertRowQuery(table string, cols []seedColumn, filtered bool, overrides map[string]string) string {
	names := make([]string, 0, len(cols))
	values := make([]string, 0, len(cols))
	for _, col := range cols {
		value := col.value
		if filtered {
			value = col.filtered
		}
		if override, ok := overrides[col.name]; ok {
			value = override
		}
		if value == "" {
			continue
		}
		names = append(names, col.name)
		values = append(values, value)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(values, ", "))
}

func updateRowQuery(table string, cols []seedColumn) string {
	sets := make([]string, 0, len(cols)+1)
	for _, col := range cols {
		if col.updated == "" {
			continue
		}
		sets = append(sets, col.name+" = "+col.updated)
	}
	sets = append(sets, "includedColumn = 202")
	return fmt.Sprintf("UPDATE %s SET %s WHERE id = 1", table, strings.Join(sets, ", "))
}

// ExecuteQuery executes MySQL queries for testing based on the operation type. Columns named in
// conf.SeedExcludedColumns are left out of the seed DDL and DML entirely.
func ExecuteQuery(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
	t.Helper()

	excludedColumns := conf.SeedExcludedColumns
	seedCols := filterSeedColumns(t, excludedColumns)

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
		query = createTableQuery(integrationTestTable, seedCols)

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
		_, err = db.ExecContext(ctx, insertRowQuery(integrationTestTable, seedCols, false,
			map[string]string{"id": "6", "id_cursor": "6", "excludedColumn": "101"}))
		require.NoError(t, err, "Failed to execute %s operation", operation)
		// insert a filtered doc, it would be filtered out by the filter, won't be synced into the destination
		_, err = db.ExecContext(ctx, insertRowQuery(integrationTestTable, seedCols, true,
			map[string]string{"id": "999", "excludedColumn": "200"}))
		require.NoError(t, err, "Failed to insert filtered test data row")
		return

	case "insert_2pc":
		query = insertRowQuery(integrationTestTable, seedCols, false,
			map[string]string{"id": "7", "id_cursor": "7"})

	case "update":
		query = updateRowQuery(integrationTestTable, seedCols)

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

	seedCols := filterSeedColumns(t, excludedColumns)
	for i := 1; i <= 5; i++ {
		_, err := db.ExecContext(ctx, insertRowQuery(tableName, seedCols, false,
			map[string]string{"id": strconv.Itoa(i), "id_cursor": strconv.Itoa(i), "excludedColumn": "100"}))
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
	// insert a filtered doc, it would be filtered out by the filter, won't be synced into the destination
	_, err := db.ExecContext(ctx, insertRowQuery(tableName, seedCols, true, map[string]string{
		"id":                     "998",
		"excludedColumn":         "200",
		"price_decimal":          "500234.123",
		"amount_decimal_9_2":     "500234.12",
		"price_double":           "500234.123",
		"price_double_precision": "500234.123",
		"price_float":            "500234.0",
		"price_numeric":          "500234.123",
		"price_real":             "500234.123",
		"created_date":           "'2021-06-15 10:00:00'",
	}))
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
	"data_binary":                   []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00},
	"data_varbinary":                []byte{0x00, 0xff, 0x10, 0xfe},
	"data_blob":                     []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a},
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
	"data_binary":                   []byte{0xff, 0xfe, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // MySQL pads BINARY(16)
	"data_varbinary":                []byte{0x01, 0x02, 0xff},
	"data_blob":                     []byte{0xe2, 0x82, 0x28},
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
	"data_binary":                   "binary",
	"data_varbinary":                "varbinary",
	"data_blob":                     "blob",
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
	"data_binary":                   "binary",
	"data_varbinary":                "varbinary",
	"data_blob":                     "blob",
	"includedcolumn":                "int",
}
var ExpectedMySQLDefaultCDCColumnsSchema = map[string]string{
	"_cdc_timestamp":        "timestamp",
	"_cdc_binlog_file_name": "string",
	"_cdc_binlog_file_pos":  "bigint",
}
