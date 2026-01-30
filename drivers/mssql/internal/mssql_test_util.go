package driver

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var connStr string
	if fileConfig {
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)

		config.Host = fmt.Sprintf("%s:%d", config.Host, config.Port)

		connStr = fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&encrypt=disable",
			config.Username,
			config.Password,
			config.Host,
			config.Database,
		)
	} else {
		connStr = "sqlserver://sa:Password!123@localhost:1433?database=olake_mssql_test&encrypt=disable"
	}

	db, err := sqlx.ConnectContext(ctx, "sqlserver", connStr)
	require.NoError(t, err, "failed to connect to mssql")
	defer func() {
		require.NoError(t, db.Close())
	}()

	// integration test uses only one stream for testing
	integrationTestTable := streams[0]

	// A capture instance is SQL Server’s logical CDC stream for a table.
	captureInstance := fmt.Sprintf("dbo_%s", integrationTestTable)

	switch operation {
	case "create":
		// Create the table in dbo schema
		createTable := fmt.Sprintf(`
			IF OBJECT_ID('dbo.%s', 'U') IS NULL
			BEGIN
				CREATE TABLE dbo.%s (
					id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
					id_cursor INT NOT NULL,

					col_tinyint TINYINT NOT NULL,
					col_smallint SMALLINT NOT NULL,
					col_int INT NOT NULL,
					col_bigint BIGINT NOT NULL,
					col_decimal DECIMAL(18,2) NOT NULL,
					col_numeric NUMERIC(10,5) NOT NULL,
					col_smallmoney SMALLMONEY NOT NULL,
					col_money MONEY NOT NULL,
					col_float FLOAT NOT NULL,
					col_real REAL NOT NULL,
					col_bit BIT NOT NULL,

					col_char CHAR(10) NOT NULL,
					col_varchar VARCHAR(255) NOT NULL,
					col_text TEXT NOT NULL,
					col_nchar NCHAR(10) NOT NULL,
					col_nvarchar NVARCHAR(255) NOT NULL,
					col_ntext NTEXT NOT NULL,

					col_date DATE NOT NULL,
					col_time TIME NOT NULL,
					col_smalldatetime SMALLDATETIME NOT NULL,
					col_datetime DATETIME NOT NULL,
					col_datetime2 DATETIME2(6) NOT NULL,
					col_datetimeoffset DATETIMEOFFSET(6) NOT NULL,
					col_uniqueidentifier UNIQUEIDENTIFIER NOT NULL,

					col_xml XML NOT NULL,
					col_sysname SYSNAME NOT NULL,

					col_image IMAGE NOT NULL,
					col_hierarchyid HIERARCHYID NOT NULL,
					col_sql_variant SQL_VARIANT NOT NULL,

					col_int_nullable INT NULL,
					col_varchar_nullable VARCHAR(255) NULL,
					col_datetime2_nullable DATETIME2(6) NULL,

					created_at DATETIME2(6) NOT NULL
				);
			END;
		`, integrationTestTable, integrationTestTable)
		_, err = db.ExecContext(ctx, createTable)
		require.NoError(t, err, "failed to create integration test table")

		// Always drop existing capture instance first to ensure fresh start_lsn
		// This handles cases where the capture instance wasn't properly cleaned up
		dropExistingCDC := fmt.Sprintf(`
			IF EXISTS (
				SELECT 1
				FROM cdc.change_tables
				WHERE capture_instance = N'%s'
			)
			BEGIN
				EXEC sys.sp_cdc_disable_table
					@source_schema = N'dbo',
					@source_name   = N'%s',
					@capture_instance = N'%s';
			END;
		`, captureInstance, integrationTestTable, captureInstance)
		_, _ = db.ExecContext(ctx, dropExistingCDC)

		// Enable CDC for table - always create fresh capture instance
		enableTableCDC := fmt.Sprintf(`
			EXEC sys.sp_cdc_enable_table
				@source_schema = N'dbo',
				@source_name   = N'%s',
				@capture_instance = N'%s',
				@role_name     = NULL
		`, integrationTestTable, captureInstance)
		_, err = db.ExecContext(ctx, enableTableCDC)
		require.NoError(t, err, "failed to enable CDC on integration test table")

		// Wait until current_max_lsn >= start_lsn of the capture instance so CDC is ready for sync
		verifyCDCEnabled(t, ctx, db, captureInstance)

	case "drop":
		// Disable CDC before dropping table to ensure capture instance is cleaned up
		// This prevents "capture instance already exists" errors in subsequent test runs
		disableTableCDC := fmt.Sprintf(`
			IF EXISTS (
				SELECT 1
				FROM cdc.change_tables
				WHERE capture_instance = N'%s'
			)
			BEGIN
				EXEC sys.sp_cdc_disable_table
					@source_schema = N'dbo',
					@source_name   = N'%s',
					@capture_instance = N'%s';
			END;
		`, captureInstance, integrationTestTable, captureInstance)
		if _, err = db.ExecContext(ctx, disableTableCDC); err != nil {
			t.Logf("failed to disable CDC on integration test table: %s", err)
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID('dbo.%s','U') IS NOT NULL DROP TABLE dbo.%s;`, integrationTestTable, integrationTestTable))
		require.NoError(t, err, "failed to drop integration test table")

	case "clean":
		_, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM dbo.%s;`, integrationTestTable))
		require.NoError(t, err, "failed to clean integration test table")

	case "add":
		insertTestData(t, ctx, db, integrationTestTable)
		return

	case "insert":
		insertOne := fmt.Sprintf(`
			INSERT INTO dbo.%s (
				id_cursor,
				col_tinyint, col_smallint, col_int, col_bigint,
				col_decimal, col_numeric, col_smallmoney, col_money,
				col_float, col_real, col_bit,
				col_char, col_varchar, col_text, col_nchar, col_nvarchar, col_ntext,
				col_date, col_time, col_smalldatetime, col_datetime, col_datetime2, col_datetimeoffset,
				col_uniqueidentifier,
				col_xml, col_sysname,
				col_image, col_hierarchyid, col_sql_variant,
				col_int_nullable, col_varchar_nullable, col_datetime2_nullable,
				created_at
			) VALUES (
				6,
				3, 5, 10, 19,
				123.50, 10.12500, 1.2500, 2.5000,
				123.50, 12.50, 1,
				'char_val__', 'varchar_val', 'text_val', N'nchar_val_', N'nvarchar_val', N'ntext_val',
				'2023-01-01', '12:00:00', '2023-01-01 12:00:00', '2023-01-01 12:00:00',
				'2023-01-01 12:00:00', '2023-01-01 12:00:00 +00:00',
				'123e4567-e89b-12d3-a456-426614174000',
				'<xml>test</xml>', 'sysname_val',
				0x43434343,
				hierarchyid::Parse('/1/1/'), CAST('variant_base' AS sql_variant),
				NULL, NULL, NULL,
				'2023-01-01 12:00:00'
			);
		`, integrationTestTable)
		_, err := db.ExecContext(ctx, insertOne)
		require.NoError(t, err, "failed to insert CDC row")

	case "update":
		updateRow := fmt.Sprintf(`
			UPDATE dbo.%s SET
				id_cursor = 7,
				col_bigint = 20,
				col_decimal = 543.25,
				col_money = 9.7500,
				col_real = 321.0,
				col_bit = 0,
				col_varchar = 'updated varchar',
				col_datetime2 = '2024-07-01 15:30:00',
				col_datetimeoffset = '2024-07-01 15:30:00 +00:00',
				col_uniqueidentifier = '00000000-0000-0000-0000-000000000000',
				col_xml = '<xml>updated</xml>',
				col_sysname = 'updated_sysname',
				col_int_nullable = 123,
				col_varchar_nullable = 'nullable updated',
				col_datetime2_nullable = '2024-07-01 15:30:00',
				created_at = '2024-07-01 15:30:00'
			WHERE id = 6;
		`, integrationTestTable)
		_, err := db.ExecContext(ctx, updateRow)
		require.NoError(t, err, "failed to update CDC row")

	case "delete":
		_, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM dbo.%s WHERE id = 6;`, integrationTestTable))
		require.NoError(t, err, "failed to delete CDC row")

	case "evolve-schema":
		// Schema evolution: widen col_int from int -> bigint
		stmt := fmt.Sprintf(`ALTER TABLE dbo.%s ALTER COLUMN col_int BIGINT NOT NULL;`, integrationTestTable)
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "failed to evolve schema")

	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}
}

// verifyCDCEnabled polls until sys.fn_cdc_get_max_lsn() >= start_lsn of the
// given capture instance, so the capture instance is ready for CDC sync.
func verifyCDCEnabled(t *testing.T, ctx context.Context, db *sqlx.DB, captureInstance string) {
	t.Helper()
	const (
		pollInterval = 500 * time.Millisecond
		timeout      = 30 * time.Second
	)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var startLSN []byte
		qStart := fmt.Sprintf(`
			SELECT start_lsn FROM cdc.change_tables WHERE capture_instance = N'%s'
		`, captureInstance)
		if err := db.QueryRowContext(ctx, qStart).Scan(&startLSN); err != nil {
			t.Logf("verifyCDCEnabled: get start_lsn: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		var currentMaxLSN []byte
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&currentMaxLSN); err != nil {
			t.Logf("verifyCDCEnabled: get max_lsn: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		startHex := hex.EncodeToString(startLSN)
		currentHex := hex.EncodeToString(currentMaxLSN)
		if currentHex >= startHex {
			return
		}
		time.Sleep(pollInterval)
	}

	t.Fatalf("CDC capture instance %s not ready within %v (current_max_lsn never reached start_lsn)", captureInstance, timeout)
}

func insertTestData(t *testing.T, ctx context.Context, db *sqlx.DB, tableName string) {
	t.Helper()
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf(`
			INSERT INTO dbo.%s (
				id_cursor,
				col_tinyint, col_smallint, col_int, col_bigint,
				col_decimal, col_numeric, col_smallmoney, col_money,
				col_float, col_real, col_bit,
				col_char, col_varchar, col_text, col_nchar, col_nvarchar, col_ntext,
				col_date, col_time, col_smalldatetime, col_datetime, col_datetime2, col_datetimeoffset,
				col_uniqueidentifier,
				col_xml, col_sysname,
				col_image, col_hierarchyid, col_sql_variant,
				col_int_nullable, col_varchar_nullable, col_datetime2_nullable,
				created_at
			) VALUES (
				%d,
				3, 5, 10, 19,
				123.50, 10.12500, 1.2500, 2.5000,
				123.50, 12.50, 1,
				'char_val__', 'varchar_val', 'text_val', N'nchar_val_', N'nvarchar_val', N'ntext_val',
				'2023-01-01', '12:00:00', '2023-01-01 12:00:00', '2023-01-01 12:00:00',
				'2023-01-01 12:00:00', '2023-01-01 12:00:00 +00:00',
				'123e4567-e89b-12d3-a456-426614174000',
				'<xml>test</xml>', 'sysname_val',
				0x43434343,
				hierarchyid::Parse('/1/1/'), CAST('variant_base' AS sql_variant),
				NULL, NULL, NULL,
				'2023-01-01 12:00:00'
			);
		`, tableName, i)
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

var ExpectedMSSQLData = map[string]interface{}{
	// ints
	"col_tinyint":  int32(3),
	"col_smallint": int32(5),
	"col_int":      int32(10),
	"col_bigint":   int64(19),

	// numerics
	"col_decimal": float64(123.5),
	"col_numeric": float64(10.125),
	"col_float":   float64(123.5),
	"col_real":    float32(12.5),

	"col_bit": true,

	// money
	"col_smallmoney": float64(1.25),
	"col_money":      float64(2.5),

	// strings
	"col_char":     "char_val__",
	"col_varchar":  "varchar_val",
	"col_text":     "text_val",
	"col_nchar":    "nchar_val_",
	"col_nvarchar": "nvarchar_val",
	"col_ntext":    "ntext_val",

	// date/time
	"col_date":           arrow.Timestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_time":           "12:00:00",
	"col_smalldatetime":  arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_datetime":       arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_datetime2":      arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_datetimeoffset": arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_at":         arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),

	"col_uniqueidentifier": "123e4567-e89b-12d3-a456-426614174000",
	"col_xml":              "<xml>test</xml>",
	"col_sysname":          "sysname_val",

	"col_image":       "CCCC",
	"col_hierarchyid": "5ac0",
	"col_sql_variant": "variant_base",
}

var ExpectedUpdatedMSSQLData = map[string]interface{}{
	// ints
	"col_tinyint":  int32(3),
	"col_smallint": int32(5),
	"col_int":      int32(10),
	"col_bigint":   int64(20),

	// numerics
	"col_decimal": float64(543.25),
	"col_numeric": float64(10.125),
	"col_float":   float64(123.5),
	"col_real":    float32(321.0),

	// misc primitives
	"col_bit": false,

	// money
	"col_smallmoney": float64(1.25),
	"col_money":      float64(9.75),

	// strings
	"col_char":     "char_val__",
	"col_varchar":  "updated varchar",
	"col_text":     "text_val",
	"col_nchar":    "nchar_val_",
	"col_nvarchar": "nvarchar_val",
	"col_ntext":    "ntext_val",

	// date/time
	"col_date":           arrow.Timestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_time":           "12:00:00",
	"col_smalldatetime":  arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_datetime":       arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_datetime2":      arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"col_datetimeoffset": arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),

	"col_uniqueidentifier": "00000000-0000-0000-0000-000000000000",
	"col_xml":              "<xml>updated</xml>",
	"col_sysname":          "updated_sysname",

	"col_image":       "CCCC",
	"col_hierarchyid": "5ac0",
	"col_sql_variant": "variant_base",

	"col_int_nullable":       int32(123),
	"col_varchar_nullable":   "nullable updated",
	"col_datetime2_nullable": arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),

	"created_at": arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
}

// MSSQLToDestinationSchema
var MSSQLToDestinationSchema = map[string]string{
	"id":                     "int",
	"col_tinyint":            "tinyint",
	"col_smallint":           "smallint",
	"col_int":                "int",
	"col_bigint":             "bigint",
	"col_decimal":            "double",
	"col_numeric":            "double",
	"col_smallmoney":         "double",
	"col_money":              "double",
	"col_float":              "double",
	"col_real":               "real",
	"col_bit":                "boolean",
	"col_char":               "string",
	"col_varchar":            "string",
	"col_text":               "string",
	"col_nchar":              "string",
	"col_nvarchar":           "string",
	"col_ntext":              "string",
	"col_date":               "timestamp",
	"col_time":               "string",
	"col_smalldatetime":      "timestamp",
	"col_datetime":           "timestamp",
	"col_datetime2":          "timestamp",
	"col_datetimeoffset":     "timestamp",
	"col_uniqueidentifier":   "string",
	"col_xml":                "string",
	"col_sysname":            "string",
	"col_image":              "string",
	"col_hierarchyid":        "string",
	"col_sql_variant":        "string",
	"col_int_nullable":       "int",
	"col_varchar_nullable":   "string",
	"col_datetime2_nullable": "timestamp",
	"created_at":             "timestamp",
}
