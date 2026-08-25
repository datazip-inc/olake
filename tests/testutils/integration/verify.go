package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// TODO: Refactor parsing logic into a reusable utility functions
// verifyIcebergSync verifies that data was correctly synchronized to Iceberg
func VerifyIcebergSync(t *testing.T, tableName, icebergDB string, datatypeSchema map[string]string, defaultCDCColumnsSchema map[string]string, schema map[string]interface{}, opSymbol, partitionRegex, driver string, isCDC bool, excludedColumn string) {
	t.Helper()
	ctx := t.Context()
	spark, err := SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	fullTableName := fmt.Sprintf("%s.%s.%s", IcebergCatalog, icebergDB, tableName)
	// The shared session caches table snapshots, so refresh to see the rows the sync just committed.
	// Non-fatal: on a first sync the table may not exist yet, which the retry loop below handles.
	if _, refreshErr := spark.Sql(ctx, fmt.Sprintf("REFRESH TABLE %s", fullTableName)); refreshErr != nil {
		t.Logf("REFRESH TABLE before verify (non-fatal): %v", refreshErr)
	}
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	// In kafka, _op_type is always 'c' and col_included appears only in new rows.
	// To check new record, col_included is used.
	if driver == string(constants.Kafka) {
		if _, ok := schema["col_included"]; ok {
			selectQuery += " AND col_included IS NOT NULL"
		}
	}
	t.Logf("Executing query: %s", selectQuery)

	var selectRows []types.Row
	var queryErr error
	maxRetries := 20
	retryDelay := 5 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(retryDelay)
		}
		var selectQueryDf sql.DataFrame
		// This is to check if the table exists in destination, as race condition might cause table to not be created yet
		selectQueryDf, queryErr = spark.Sql(ctx, selectQuery)
		if queryErr != nil {
			t.Logf("Query attempt %d failed: %v", attempt+1, queryErr)
			continue
		}

		// To ensure stale data is not being used for verification
		selectRows, queryErr = selectQueryDf.Collect(ctx)
		if queryErr != nil {
			t.Logf("Query attempt %d failed (Collect error): %v", attempt+1, queryErr)
			continue
		}
		if len(selectRows) > 0 {
			queryErr = nil
			break
		}

		// For delete operations, 0 rows is acceptable - exit immediately without retrying
		if opSymbol == "d" {
			queryErr = nil
			t.Logf("Delete verification passed: found 0 rows for _op_type = 'd' (acceptable)")
			break
		}

		// for every type of operation, op symbol will be different, using that to ensure data is not stale
		queryErr = fmt.Errorf("stale data: query succeeded but returned 0 rows for _op_type = '%s'", opSymbol)
		t.Logf("Query attempt %d/%d failed: %v", attempt+1, maxRetries, queryErr)

		// Force Spark to refresh the table metadata from the Iceberg catalog.
		refreshQuery := fmt.Sprintf("REFRESH TABLE %s", fullTableName)
		if _, refreshErr := spark.Sql(ctx, refreshQuery); refreshErr != nil {
			t.Logf("REFRESH TABLE attempt %d failed (non-fatal): %v", attempt+1, refreshErr)
		}
	}

	// For delete operations, accept both 0 and 1 row (both are valid outcomes)
	if opSymbol == "d" {
		if len(selectRows) > 0 {
			deletedID := selectRows[0].Value("_olake_id")
			require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(selectRows))
		return
	}
	require.NoError(t, queryErr, "Failed to collect data rows from Iceberg after %d attempts: %v", maxRetries, queryErr)
	require.NotEmpty(t, selectRows, "No rows returned for _op_type = '%s'", opSymbol)

	for rowIdx, row := range selectRows {
		icebergMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			icebergMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			icebergValue, ok := icebergMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
			require.Equal(t, expected, icebergValue, "Row %d: mismatch on %q: Iceberg has %#v, expected %#v", rowIdx, key, icebergValue, expected)
		}
		if isCDC {
			for key := range defaultCDCColumnsSchema {
				icebergValue, ok := icebergMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in Iceberg result", rowIdx, key)
				// Kafka offset, partition can be 0, NotEmpty fails for 0 so we check for NotNil instead.
				if key == "_kafka_offset" || key == "_kafka_partition" {
					require.NotNil(t, icebergValue, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, icebergValue)
				} else {
					require.NotEmpty(t, icebergValue, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, icebergValue)
				}
				if key == constants.CdcTimestamp {
					ts, ok := normalizeToTime(icebergValue)
					require.Truef(t, ok, "Row %d: expected %q to be a timestamp, got %T (%#v)", rowIdx, key, icebergValue, icebergValue)
					minAllowed := time.Now().Add(-1 * time.Hour)
					require.Falsef(t, ts.Before(time.Now().Add(-1*time.Hour)), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}
		if !isCDC && icebergMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(icebergMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, icebergMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
		}
	}
	t.Logf("Verified Iceberg synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s", fullTableName)
	describeDf, err := spark.Sql(ctx, describeQuery)
	require.NoError(t, err, "Failed to describe Iceberg table")

	describeRows, err := describeDf.Collect(ctx)
	require.NoError(t, err, "Failed to collect describe data from Iceberg")
	icebergSchema := make(map[string]string)
	for _, row := range describeRows {
		colName := row.Value("col_name").(string)
		dataType := row.Value("data_type").(string)
		if !strings.HasPrefix(colName, "#") {
			icebergSchema[colName] = dataType
		}
	}

	if excludedColumn != "" {
		_, ok := icebergSchema[testutils.Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Iceberg schema", excludedColumn)
	}

	for col, dbType := range datatypeSchema {
		iceType, found := icebergSchema[col]
		require.True(t, found, "Column %s not found in Iceberg schema", col)

		expectedIceType, mapped := testutils.GlobalTypeMapping[dbType]
		if !mapped {
			t.Errorf("No mapping defined for driver type %s (column %s)", dbType, col)
		}
		require.Equal(t, expectedIceType, iceType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
	}
	t.Logf("Verified datatypes in Iceberg after sync")
	// Verify datatypes for CDC/default columns as well
	if isCDC {
		for col, expectedIceType := range defaultCDCColumnsSchema {
			iceType, found := icebergSchema[col]
			require.True(t, found, "CDC column %s not found in Iceberg schema", col)

			require.Equal(t, expectedIceType, iceType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
		}
		t.Logf("Verified datatypes for CDC columns in Iceberg after sync")
	}

	// Partition verification using only metadata tables
	if partitionRegex == "" {
		t.Log("No partitionRegex provided, skipping partition verification")
		return
	}
	// Extract partition columns from describe rows
	partitionCols := extractFirstPartitionColFromRows(describeRows)
	require.NotEmpty(t, partitionCols, "Partition columns not found in Iceberg metadata")

	// Parse expected partition columns from pattern like "/{col,identity}"
	// Supports multiple entries like "/{col1,identity}" by taking the first token as the source column
	clean := strings.TrimPrefix(partitionRegex, "/{")
	clean = strings.TrimSuffix(clean, "}")
	toks := strings.Split(clean, ",")
	expectedCol := strings.TrimSpace(toks[0])
	require.Equal(t, expectedCol, partitionCols, "Partition column does not match expected '%s'", expectedCol)
	t.Logf("Verified partition column: %s", expectedCol)
}

// VerifyIcebergNoDuplicates asserts that no duplicate _olake_id values exist for the given
// _op_type in the Iceberg table.
func VerifyIcebergNoDuplicates(ctx context.Context, t *testing.T, tableName, icebergDB, opSymbol string, expectedRowCountByOpType int64) {
	t.Helper()

	spark, err := SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server for duplicate check")

	fullTableName := fmt.Sprintf("%s.%s.%s", IcebergCatalog, icebergDB, tableName)

	// Refresh to get the latest committed Iceberg snapshot.
	refreshQuery := fmt.Sprintf("REFRESH TABLE %s", fullTableName)
	if _, refreshErr := spark.Sql(ctx, refreshQuery); refreshErr != nil {
		t.Logf("REFRESH TABLE (non-fatal): %v", refreshErr)
	}

	countQuery := fmt.Sprintf(
		"SELECT COUNT(*) AS total, COUNT(DISTINCT _olake_id) AS distinct_count FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	t.Logf("Executing duplicate-check query: %s", countQuery)

	df, err := spark.Sql(ctx, countQuery)
	require.NoError(t, err, "Failed to run duplicate-check COUNT query")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect duplicate-check COUNT results")
	require.Len(t, rows, 1, "COUNT query must return exactly one row")

	total, ok := rows[0].Value("total").(int64)
	require.True(t, ok, "COUNT(*) value is not int64: %T", rows[0].Value("total"))

	distinct, ok2 := rows[0].Value("distinct_count").(int64)
	require.True(t, ok2, "COUNT(DISTINCT) value is not int64: %T", rows[0].Value("distinct_count"))

	// 1. No duplicates: every row must have a unique _olake_id.
	require.Equal(t, total, distinct,
		"Duplicate rows detected for _op_type='%s': total=%d, distinct=%d. "+
			"Iceberg MERGE INTO did not deduplicate re-synced records.",
		opSymbol, total, distinct)

	// 2. Exact count: when caller specifies an expected row count, enforce it so that both
	//    over-sync (old rows re-processed and inserted again) and under-sync (new rows missed)
	//    are caught.
	if expectedRowCountByOpType > 0 {
		require.Equal(t, expectedRowCountByOpType, distinct,
			"Row count mismatch for _op_type='%s': expected %d distinct rows, got %d. "+
				"Either old rows were re-synced (over-sync) or new rows were missed (under-sync).",
			opSymbol, expectedRowCountByOpType, distinct)
	}

	t.Logf("Duplicate check passed for _op_type='%s': %d rows, all unique by _olake_id (expected %d)",
		opSymbol, distinct, expectedRowCountByOpType)
}

// VerifyParquetSync verifies that data was correctly synchronized to Parquet files in MinIO
func VerifyParquetSync(t *testing.T, tableName, parquetDB string, datatypeSchema map[string]string, defaultCDCColumnsSchema map[string]string, schema map[string]interface{}, opSymbol, driver string, isCDC bool, excludedColumn string) {
	t.Helper()
	ctx := t.Context()

	spark, err := SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	parquetPath := fmt.Sprintf("s3a://warehouse/%s/%s", parquetDB, tableName)
	viewName := fmt.Sprintf("`%s_view_%d`", tableName, time.Now().UnixNano())

	// create a temporary view for parquet files, allows to run describe query
	createViewQuery := fmt.Sprintf(
		"CREATE OR REPLACE TEMP VIEW %s AS SELECT * FROM parquet.`%s/*.parquet`",
		viewName, parquetPath,
	)

	// Retry logic for transient Spark connection issues (e.g., catalog connection pool exhaustion)
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err = spark.Sql(ctx, createViewQuery)
		if err == nil {
			break
		}
		// For delete operations, if path doesn't exist that's acceptable (no data written)
		if opSymbol == "d" && strings.Contains(err.Error(), "PATH_NOT_FOUND") {
			t.Logf("Delete verification passed: Parquet path does not exist (no data written)")
			return
		}
		if attempt < maxRetries {
			t.Logf("Attempt %d/%d: Failed to create view, retrying in 2s: %v", attempt, maxRetries, err)
			time.Sleep(2 * time.Second)
		}
	}
	require.NoError(t, err, "Failed to create temporary view for Parquet files")

	defer func() {
		dropViewQuery := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
		t.Logf("Dropping temporary view: %s", dropViewQuery)
		_, _ = spark.Sql(ctx, dropViewQuery)
	}()

	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE `_op_type` = '%s'",
		viewName, opSymbol,
	)
	// In kafka, _op_type is always 'c' and col_included appears only in new rows.
	// To check new record, col_included is used.
	if driver == string(constants.Kafka) {
		if _, ok := schema["col_included"]; ok {
			selectQuery += " AND `col_included` IS NOT NULL"
		}
	}
	t.Logf("Executing Parquet query: %s", selectQuery)

	df, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to run select query on Parquet files")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect rows from Parquet query")

	// For delete operations, accept both 0 and 1 row (both are valid outcomes)
	if opSymbol == "d" {
		if len(rows) > 0 {
			deletedID := rows[0].Value("_olake_id")
			require.NotEmpty(t, deletedID, "Delete verification failed: _olake_id should not be empty")
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(rows))
		return
	}

	// For non-delete operations, require at least one row
	require.NotEmpty(t, rows, "No rows returned for _op_type = '%s'", opSymbol)

	for rowIdx, row := range rows {
		parquetMap := make(map[string]interface{}, len(schema)+1)
		for _, col := range row.FieldNames() {
			parquetMap[col] = row.Value(col)
		}
		for key, expected := range schema {
			val, ok := parquetMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in Parquet result", rowIdx, key)
			require.Equal(t, expected, val,
				"Row %d: mismatch on %q: Parquet has %#v, expected %#v", rowIdx, key, val, expected)
		}
		if isCDC {
			for key := range defaultCDCColumnsSchema {
				val, ok := parquetMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in Parquet result", rowIdx, key)
				// Kafka offset, partition can be 0, NotEmpty fails for 0 so we check for NotNil instead.
				if key == "_kafka_offset" || key == "_kafka_partition" {
					require.NotNil(t, val, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, val)
				} else {
					require.NotEmpty(t, val, "Row %d: expected column %q to be non-empty, got %#v", rowIdx, key, val)
				}
				if key == constants.CdcTimestamp {
					ts, ok := normalizeToTime(val)
					require.Truef(t, ok, "Row %d: expected %q to be a timestamp, got %T (%#v)", rowIdx, key, val, val)
					minAllowed := time.Now().Add(-1 * time.Hour)
					require.Falsef(t, ts.Before(time.Now().Add(-1*time.Hour)), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}
		if !isCDC && parquetMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(parquetMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, parquetMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
		}
	}

	t.Logf("Verified Parquet synced data with respect to data synced from source[%s] found equal", driver)

	describeQuery := fmt.Sprintf("DESCRIBE TABLE %s", viewName)
	descDF, err := spark.Sql(ctx, describeQuery)
	require.NoError(t, err, "Failed to describe Parquet view")

	descRows, err := descDF.Collect(ctx)
	require.NoError(t, err, "Failed to collect schema info from Parquet view")

	parquetSchema := make(map[string]string)
	for _, row := range descRows {
		colName := row.Value("col_name").(string)
		dataType := row.Value("data_type").(string)
		if !strings.HasPrefix(colName, "#") {
			parquetSchema[colName] = dataType
		}
	}
	if excludedColumn != "" {
		_, ok := parquetSchema[testutils.Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Parquet schema", excludedColumn)
	}

	for col, dbType := range datatypeSchema {
		pqType, found := parquetSchema[col]
		require.True(t, found, "Column %s not found in Parquet schema", col)

		expectedType, mapped := testutils.GlobalTypeMapping[dbType]
		if !mapped {
			t.Errorf("No mapping defined for driver type %s (column %s)", dbType, col)
		}
		require.Equal(t, expectedType, pqType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedType, pqType)
	}
	t.Logf("Verified datatypes in Parquet after sync")
	// Verify datatypes for CDC/default columns as well
	if isCDC {
		for col, expectedPqType := range defaultCDCColumnsSchema {
			pqType, found := parquetSchema[col]
			require.True(t, found, "CDC column %s not found in Parquet schema", col)
			require.Equal(t, expectedPqType, pqType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedPqType, pqType)
		}
	}
	t.Logf("Verified datatypes for CDC columns in Parquet after sync")
}

// extractFirstPartitionColFromRows extracts the first partition column from DESCRIBE EXTENDED rows
func extractFirstPartitionColFromRows(rows []types.Row) string {
	inPartitionSection := false

	for _, row := range rows {
		// Convert []any -> []string
		vals := row.Values()
		parts := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				parts[i] = ""
			} else {
				parts[i] = fmt.Sprint(v) // safe string conversion
			}
		}
		line := strings.TrimSpace(strings.Join(parts, " "))
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "# Partition Information") {
			inPartitionSection = true
			continue
		}

		if inPartitionSection {
			if strings.HasPrefix(line, "# col_name") {
				continue
			}

			if strings.HasPrefix(line, "#") {
				break
			}

			fields := strings.Fields(line)
			if len(fields) > 0 {
				return fields[0] // return the first partition col
			}
		}
	}

	return ""
}

func normalizeToTime(v interface{}) (time.Time, bool) {
	switch ts := v.(type) {
	case time.Time:
		return ts, true
	case arrow.Timestamp:
		return time.Unix(0, int64(ts)*int64(time.Microsecond)).UTC(), true
	default:
		return time.Time{}, false
	}
}
