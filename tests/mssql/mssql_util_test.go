package mssql

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

// cdcMetadataMu serializes CDC enable/disable: both write the server-wide msdb.dbo.cdc_jobs, so two
// suites doing it at once deadlock there (error 1205).
var cdcMetadataMu sync.Mutex

// execCDCMetadata runs a CDC metadata statement, retrying while SQL Server reports it as the
// deadlock victim (error 1205, which asks the caller to rerun). Returns the last error otherwise.
func execCDCMetadata(ctx context.Context, t *testing.T, db *sqlx.DB, query string) error {
	t.Helper()
	cdcMetadataMu.Lock()
	defer cdcMetadataMu.Unlock()

	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		if _, err = db.ExecContext(ctx, query); err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "was deadlocked on lock resources") {
			return err
		}
		t.Logf("CDC metadata statement lost a deadlock (attempt %d/5), retrying: %s", attempt, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	return err
}

func ExecuteQuery(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
	t.Helper()

	// The suite picks the database, not just the table: separate tables still race on DDL like
	// DROP/CREATE TABLE, which modify database-scoped shared metadata (system catalog, cdc schema)
	// and fail the loser as the deadlock victim. This has to resolve to the same name
	// variantSourceOverride writes into the suite's source.json, or olake and these queries end up
	// in different databases. 01-init.sql provisions each with CDC enabled.
	var connStr string
	if config := conf.SourceBaseConfig; config != nil {
		connStr = fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
			config.String("username"),
			config.String("password"),
			config.String("host"),
			config.Int("port"),
			testutils.SuiteDatabase(config.String("database"), conf.Suite),
		)
	} else {
		connStr = fmt.Sprintf("sqlserver://sa:Password!123@localhost:1433?database=%s&encrypt=disable",
			testutils.SuiteDatabase("olake_mssql_test", conf.Suite))
	}

	db, err := sqlx.ConnectContext(ctx, "sqlserver", connStr)
	require.NoError(t, err, "failed to connect to mssql")
	defer func() {
		require.NoError(t, db.Close())
	}()

	// integration test uses only one stream for testing
	integrationTestTable := testutils.TestTableName(conf)

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

					created_at DATETIME2(6) NOT NULL,
					excludedColumn INT NULL,
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
		_ = execCDCMetadata(ctx, t, db, dropExistingCDC)

		// Enable CDC for table - always create fresh capture instance
		enableTableCDC := fmt.Sprintf(`
			EXEC sys.sp_cdc_enable_table
				@source_schema = N'dbo',
				@source_name   = N'%s',
				@capture_instance = N'%s',
				@role_name     = NULL
		`, integrationTestTable, captureInstance)
		require.NoError(t, execCDCMetadata(ctx, t, db, enableTableCDC), "failed to enable CDC on integration test table")

		// Wait until current_max_lsn >= start_lsn of the capture instance so CDC is ready for sync
		verifyCDCEnabled(ctx, t, db, captureInstance)

	case "drop-all":
		require.NoError(t, execCDCMetadata(ctx, t, db, `
			DECLARE @schema SYSNAME, @table SYSNAME, @capture SYSNAME, @drop NVARCHAR(MAX);
			DECLARE tables CURSOR LOCAL FAST_FORWARD FOR
				SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
				WHERE TABLE_TYPE = 'BASE TABLE'
				AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys', 'cdc')
				AND NOT (TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'systranschemas');
			OPEN tables;
			FETCH NEXT FROM tables INTO @schema, @table;
			WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @capture = NULL;
				IF OBJECT_ID('cdc.change_tables') IS NOT NULL
					SELECT @capture = capture_instance FROM cdc.change_tables
					WHERE source_object_id = OBJECT_ID(QUOTENAME(@schema) + '.' + QUOTENAME(@table));
				IF @capture IS NOT NULL
					EXEC sys.sp_cdc_disable_table
						@source_schema = @schema, @source_name = @table, @capture_instance = @capture;
				SET @drop = N'DROP TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table);
				EXEC sys.sp_executesql @drop;
				FETCH NEXT FROM tables INTO @schema, @table;
			END
			CLOSE tables;
			DEALLOCATE tables;`), "failed to drop all tables")

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
		if err = execCDCMetadata(ctx, t, db, disableTableCDC); err != nil {
			t.Logf("failed to disable CDC on integration test table: %s", err)
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID('dbo.%s','U') IS NOT NULL DROP TABLE dbo.%s;`, integrationTestTable, integrationTestTable))
		require.NoError(t, err, "failed to drop integration test table")

	case "clean":
		_, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM dbo.%s;`, integrationTestTable))
		require.NoError(t, err, "failed to clean integration test table")

	case "add":
		insertTestData(ctx, t, db, integrationTestTable)
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
				created_at,
				excludedColumn
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
				'2023-01-01 12:00:00',
				101
			);
		`, integrationTestTable)
		_, err := db.ExecContext(ctx, insertOne)
		require.NoError(t, err, "failed to insert CDC row")

		filteredQuery := fmt.Sprintf(`
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
					created_at,
					excludedColumn
				) VALUES (
					6,
					3, 5, 10, 19,
					239835.89, 10.12500, 1.2500, 2.5000,
					123.50, 12.50, 1,
					'char_val__', 'varchar_val', 'text_val', N'nchar_val_', N'nvarchar_val', N'ntext_val',
					'2023-01-01', '12:00:00', '2023-01-01 12:00:00', '2023-01-01 12:00:00',
					'2023-01-01 12:00:00', '2023-01-01 12:00:00 +00:00',
					'123e4567-e89b-12d3-a456-426614174000',
					'<xml>test</xml>', 'sysname_val',
					0x43434343,
					hierarchyid::Parse('/1/1/'), CAST('variant_base' AS sql_variant),
					NULL, NULL, NULL,
					'2023-01-01 12:00:00',
					101
			);
		`, integrationTestTable)
		_, err = db.ExecContext(ctx, filteredQuery)
		require.NoError(t, err, "failed to insert filtered CDC row")

	case "insert_2pc":
		insertTwo := fmt.Sprintf(`
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
				7,
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
		_, err2 := db.ExecContext(ctx, insertTwo)
		require.NoError(t, err2, "failed to insert CDC row (insert_2pc)")

	case "update":
		updateRow := fmt.Sprintf(`
			UPDATE dbo.%s SET
				id_cursor = 100,
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
				created_at = '2024-07-01 15:30:00',
				excludedColumn = 102
			WHERE id = 1;
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

	case "wait-cdc-catchup":
		// The caller just committed DML it expects the next CDC sync to pick up; wait for the
		// asynchronous capture job to scan it.
		waitForCDCCapture(ctx, t, db)

	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}
}

// verifyCDCEnabled polls until sys.fn_cdc_get_max_lsn() >= start_lsn of the
// given capture instance, so the capture instance is ready for CDC sync.
func verifyCDCEnabled(ctx context.Context, t *testing.T, db *sqlx.DB, captureInstance string) {
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
		if bytes.Compare(currentMaxLSN, startLSN) >= 0 {
			return
		}
		time.Sleep(pollInterval)
	}

	t.Fatalf("CDC capture instance %s not ready within %v (current_max_lsn never reached start_lsn)", captureInstance, timeout)
}

// waitForCDCCapture blocks until the capture job's processed high-water mark
// (sys.fn_cdc_get_max_lsn()) advances past its current value, i.e. until the job has completed a
// transaction-log scan that includes the DML the caller just committed (this test database has no
// other writers). A CDC sync only reads changes up to that mark, so syncing earlier would see no
// rows. Timing out is not an error: if the capture job processed the DML before the baseline was
// read, the mark only moves on future activity — proceeding then costs no more than the blind
// 20-second sleep this wait replaced.
func waitForCDCCapture(ctx context.Context, t *testing.T, db *sqlx.DB) {
	t.Helper()
	const (
		pollInterval = 500 * time.Millisecond
		timeout      = 20 * time.Second
	)

	var baseline []byte
	err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&baseline)
	require.NoError(t, err, "failed to read CDC max LSN baseline")

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var current []byte
		if err := db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&current); err != nil {
			t.Logf("waitForCDCCapture: get max_lsn: %v", err)
		} else if bytes.Compare(current, baseline) > 0 {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Logf("waitForCDCCapture: max LSN did not advance within %v (capture job may have already scanned the change); proceeding", timeout)
}

func insertTestData(ctx context.Context, t *testing.T, db *sqlx.DB, tableName string) {
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
				created_at,
				excludedColumn
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
				'2023-01-01 12:00:00',
				100
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

var ExpectedMSSQLDefaultCDCColumnsSchema = map[string]string{
	"_cdc_timestamp": "timestamp",
	"_cdc_start_lsn": "string",
	"_cdc_seqval":    "string",
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
