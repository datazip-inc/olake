package integration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// ExpectedRows bundles cardinality, content, and schema expectations for destination verification.
type ExpectedRows struct {
	Rows                    map[string]map[string]interface{} // expected rows indexed by key (e.g. "1", "2")
	KeyColumn               string                            // column to match destination rows to Rows (default: "id")
	FallbackTemplate        map[string]interface{}            // single-row template when Rows is not populated
	Count                   *int                              // nil = skip count check; 0 = empty; >0 = exact count
	SkipCountCheck          bool                              // forces ResolveCount() to return nil
	DefaultCDCColumnsSchema map[string]string                 // CDC column types to verify when IsCDC is true
	IsCDC                   bool                              // whether to validate CDC columns
}

// ResolveCount returns the effective expected row count.
func (e ExpectedRows) ResolveCount() *int {
	if e.SkipCountCheck {
		return nil
	}
	if e.Count != nil {
		return e.Count
	}
	if len(e.Rows) > 0 {
		c := len(e.Rows)
		return &c
	}
	return nil
}

// ptr returns a pointer to any value.
func ptr[T any](v T) *T {
	return &v
}

// requireExpectations fails a verification that has nothing to assert.
func requireExpectations(t *testing.T, datatypeSchema map[string]string, expected ExpectedRows, opSymbol, driver string) {
	t.Helper()
	require.NotEmpty(t, datatypeSchema, "the %s suite passed no destination datatype schema; set DestinationDataTypeSchema, or UpdatedDestinationDataTypeSchema for a sync whose schema evolved", driver)
	if opSymbol != "d" {
		hasExpectedData := len(expected.Rows) > 0 || len(expected.FallbackTemplate) > 0
		require.True(t, hasExpectedData, "the %s suite passed no expected row data for op %q; set ExpectedData, or ExpectedUpdatedData for a sync whose schema evolved", driver, opSymbol)
	}
	if expected.IsCDC {
		require.NotEmpty(t, expected.DefaultCDCColumnsSchema, "the %s suite passed no CDC columns schema; set DefaultCDCColumnsSchema", driver)
	}
}

// TODO: Refactor parsing logic into a reusable utility functions
// verifyIcebergSync verifies that data was correctly synchronized to Iceberg
func VerifyIcebergSync(t *testing.T, tableName, icebergDB string, datatypeSchema, typeMapping map[string]string, expected ExpectedRows, opSymbol, partitionRegex, driver string, excludedColumn string) {
	t.Helper()
	requireExpectations(t, datatypeSchema, expected, opSymbol, driver)
	ctx := t.Context()
	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, icebergDB, tableName)
	// The shared session caches table snapshots, so refresh to see the rows the sync just committed.
	// Non-fatal: on a first sync the table may not exist yet, which the retry loop below handles.
	refreshTable(ctx, t, spark, fullTableName)
	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE _op_type = '%s'",
		fullTableName, opSymbol,
	)
	// Kafka append keeps _op_type 'c'. Upsert writes c (first insert, Iceberg rewrite), u, or d.
	// col_included appears only on evolved/update rows.
	if driver == string(constants.Kafka) {
		hasColIncluded := false
		if expected.FallbackTemplate != nil {
			_, hasColIncluded = expected.FallbackTemplate["col_included"]
		}
		if !hasColIncluded {
			for _, r := range expected.Rows {
				if _, ok := r["col_included"]; ok {
					hasColIncluded = true
					break
				}
			}
		}
		if hasColIncluded {
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
		refreshTable(ctx, t, spark, fullTableName)
	}

	require.NoError(t, queryErr, "Failed to collect data rows from Iceberg after %d attempts: %v", maxRetries, queryErr)

	verifySyncedRows(t, selectRows, expected, opSymbol, "Iceberg", driver)
	if opSymbol == "d" {
		return
	}

	icebergSchema, describeRows := testutils.DescribeSchema(ctx, t, spark, fullTableName)

	if excludedColumn != "" {
		_, ok := icebergSchema[testutils.Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Iceberg schema", excludedColumn)
	}

	require.NotEmpty(t, typeMapping, "the %s suite declares no type mapping; set Test.TypeMapping", driver)
	for col, dbType := range datatypeSchema {
		iceType, found := icebergSchema[col]
		require.True(t, found, "Column %s not found in Iceberg schema", col)

		expectedIceType, mapped := typeMapping[dbType]
		require.Truef(t, mapped, "column %s is declared as %q, which the %s suite's TypeMapping does not define; the declared types and the mapping must agree", col, dbType, driver)
		require.Equal(t, sparkTypeOf(expectedIceType), iceType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedIceType, iceType)
	}
	t.Logf("Verified datatypes in Iceberg after sync")
	verifyIcebergBinaryTypes(ctx, t, spark, fullTableName, datatypeSchema, typeMapping)
	// Verify datatypes for CDC/default columns as well
	if expected.IsCDC {
		for col, expectedIceType := range expected.DefaultCDCColumnsSchema {
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

	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server for duplicate check")

	fullTableName := fmt.Sprintf("%s.%s.%s", testutils.IcebergCatalog, icebergDB, tableName)

	// Refresh to get the latest committed Iceberg snapshot.
	refreshTable(ctx, t, spark, fullTableName)

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
func VerifyParquetSync(t *testing.T, tableName, parquetDB string, datatypeSchema, typeMapping map[string]string, expected ExpectedRows, opSymbol, driver string, excludedColumn string) {
	t.Helper()
	requireExpectations(t, datatypeSchema, expected, opSymbol, driver)
	ctx := t.Context()

	spark, err := testutils.SparkSession(ctx, t)
	require.NoError(t, err, "Failed to connect to Spark Connect server")

	viewName := fmt.Sprintf("`%s_view_%d`", tableName, time.Now().UnixNano())

	// Retry logic for transient Spark connection issues (e.g., catalog connection pool exhaustion)
	const maxRetries = 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = testutils.CreateParquetView(ctx, t, spark, viewName, parquetDB, tableName)
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

	selectQuery := fmt.Sprintf(
		"SELECT * FROM %s WHERE `_op_type` = '%s'",
		viewName, opSymbol,
	)
	// Kafka append keeps _op_type 'c'. Upsert writes c (first insert, Iceberg rewrite), u, or d.
	// col_included appears only on evolved/update rows.
	if driver == string(constants.Kafka) {
		hasColIncluded := false
		if expected.FallbackTemplate != nil {
			_, hasColIncluded = expected.FallbackTemplate["col_included"]
		}
		if !hasColIncluded {
			for _, r := range expected.Rows {
				if _, ok := r["col_included"]; ok {
					hasColIncluded = true
					break
				}
			}
		}
		if hasColIncluded {
			selectQuery += " AND `col_included` IS NOT NULL"
		}
	}
	t.Logf("Executing Parquet query: %s", selectQuery)

	df, err := spark.Sql(ctx, selectQuery)
	require.NoError(t, err, "Failed to run select query on Parquet files")

	rows, err := df.Collect(ctx)
	require.NoError(t, err, "Failed to collect rows from Parquet query")

	verifySyncedRows(t, rows, expected, opSymbol, "Parquet", driver)
	if opSymbol == "d" {
		return
	}

	parquetSchema, _ := testutils.DescribeSchema(ctx, t, spark, viewName)
	if excludedColumn != "" {
		_, ok := parquetSchema[testutils.Reformat(excludedColumn)]
		require.Falsef(t, ok, "Excluded column %q should not exist in Parquet schema", excludedColumn)
	}

	require.NotEmpty(t, typeMapping, "the %s suite declares no type mapping; set Test.TypeMapping", driver)
	for col, dbType := range datatypeSchema {
		pqType, found := parquetSchema[col]
		require.True(t, found, "Column %s not found in Parquet schema", col)

		expectedType, mapped := typeMapping[dbType]
		require.Truef(t, mapped, "column %s is declared as %q, which the %s suite's TypeMapping does not define; the declared types and the mapping must agree", col, dbType, driver)
		require.Equal(t, sparkTypeOf(expectedType), pqType,
			"Data type mismatch for column %s: expected %s, got %s", col, expectedType, pqType)
	}
	t.Logf("Verified datatypes in Parquet after sync")
	verifyParquetBinaryTypes(ctx, t, parquetDB, tableName, datatypeSchema, typeMapping)
	// Verify datatypes for CDC/default columns as well
	if expected.IsCDC {
		for col, expectedPqType := range expected.DefaultCDCColumnsSchema {
			pqType, found := parquetSchema[col]
			require.True(t, found, "CDC column %s not found in Parquet schema", col)
			require.Equal(t, expectedPqType, pqType,
				"CDC data type mismatch for column %s: expected %s, got %s", col, expectedPqType, pqType)
		}
	}
	t.Logf("Verified datatypes for CDC columns in Parquet after sync")
}

// verifySyncedRows asserts cardinality, unique _olake_id, column values, and CDC metadata
// on rows collected from an Iceberg or Parquet sync.
func verifySyncedRows(
	t *testing.T,
	rows []types.Row,
	expected ExpectedRows,
	opSymbol, destination, driver string,
) {
	t.Helper()

	if opSymbol == "d" {
		maxAllowed := 1
		targetCount := expected.ResolveCount()
		if targetCount != nil {
			maxAllowed = *targetCount
		}
		require.LessOrEqualf(t, len(rows), maxAllowed, "Delete verification failed: got %d rows for _op_type = 'd', expected at most %d", len(rows), maxAllowed)
		for rowIdx, row := range rows {
			deletedID := row.Value("_olake_id")
			require.NotEmpty(t, deletedID, "Row %d: Delete verification failed: _olake_id should not be empty", rowIdx)
		}
		t.Logf("Delete verification passed: found %d row(s) for _op_type = 'd'", len(rows))
		return
	}

	targetCount := expected.ResolveCount()
	if targetCount != nil {
		if *targetCount == 0 {
			require.Empty(t, rows, "Expected 0 rows for _op_type = '%s', got %d", opSymbol, len(rows))
		} else {
			require.Len(t, rows, *targetCount, "Row count mismatch for _op_type = '%s'", opSymbol)
		}
	} else {
		require.NotEmpty(t, rows, "No rows returned for _op_type = '%s'", opSymbol)
	}

	seenIDs := make(map[interface{}]struct{}, len(rows))
	for rowIdx, row := range rows {
		olakeID := row.Value("_olake_id")
		if olakeID != nil {
			require.NotEmpty(t, olakeID, "Row %d: expected _olake_id to be non-empty", rowIdx)
			_, exists := seenIDs[olakeID]
			require.Falsef(t, exists, "Duplicate row detected: _olake_id %#v appeared more than once", olakeID)
			seenIDs[olakeID] = struct{}{}
		}
	}

	keyCol := expected.KeyColumn
	if keyCol == "" {
		keyCol = "id"
	}

	for rowIdx, row := range rows {
		rowMap := make(map[string]interface{}, len(row.FieldNames()))
		for _, col := range row.FieldNames() {
			rowMap[col] = row.Value(col)
		}

		// Look up expected row by key if distinct row expectations exist; else use fallback template
		var expectedSchema map[string]interface{}
		if len(expected.Rows) > 0 {
			keyVal := row.Value(keyCol)
			if keyVal != nil {
				expectedSchema = expected.Rows[fmt.Sprint(keyVal)]
			}
		} else {
			expectedSchema = expected.FallbackTemplate
		}

		require.NotEmpty(t, expectedSchema, "Row %d: no expected data found for key %v in %s result", rowIdx, row.Value(keyCol), destination)

		for key, expectedVal := range expectedSchema {
			actualVal, ok := rowMap[key]
			require.Truef(t, ok, "Row %d: missing column %q in %s result", rowIdx, key, destination)
			require.Equal(t, expectedVal, sparkValueOf(expectedVal, actualVal), "Row %d: mismatch on %q: %s has %#v, expected %#v", rowIdx, key, destination, actualVal, expectedVal)
		}

		if expected.IsCDC {
			for key := range expected.DefaultCDCColumnsSchema {
				val, ok := rowMap[key]
				require.Truef(t, ok, "Row %d: missing column %q in %s result", rowIdx, key, destination)
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
					require.Falsef(t, ts.Before(minAllowed), "Row %d: %q is too old: %v, should not be earlier than %v", rowIdx, key, ts, minAllowed)
				}
			}
		}

		if !expected.IsCDC && rowMap[constants.CdcTimestamp] != nil {
			ts, ok := normalizeToTime(rowMap[constants.CdcTimestamp])
			require.Truef(t, ok, "expected %q to be a timestamp, got %T", constants.CdcTimestamp, rowMap[constants.CdcTimestamp])
			// Normalize to UTC to keep tests stable across environments (Local vs UTC).
			require.Equal(t, time.Unix(0, 0).UTC(), ts.UTC())
		}
	}
	t.Logf("Verified %s synced data with respect to data synced from source[%s] found equal", destination, driver)
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

// sparkTypeOf is the type Spark reports for an expected destination type. Iceberg's fixed[n] and
// parquet's FIXED_LEN_BYTE_ARRAY(n) both surface in Spark as plain binary, so a fixed width is
// asserted separately, from the destination's own metadata.
func sparkTypeOf(expected string) string {
	if _, fixed := fixedBinaryWidth(expected); fixed {
		return "binary"
	}
	return expected
}

// sparkValueOf is a destination value in the form its expectation states it. A Decimal expectation is exact
// text at the column's scale; Spark returns a decimal as its unscaled integer.
func sparkValueOf(expected, actual any) any {
	if _, isDecimal := expected.(testutils.Decimal); !isDecimal {
		return actual
	}
	if v, isNum := actual.(decimal128.Num); isNum {
		_, fraction, _ := strings.Cut(string(expected.(testutils.Decimal)), ".")
		return testutils.Decimal(decimalText(v, len(fraction)))
	}
	return actual
}

// decimalText prints an unscaled integer at scale exactly, with every digit of the scale.
func decimalText(unscaled decimal128.Num, scale int) string {
	digits := unscaled.Abs().BigInt().String()
	if scale > 0 {
		digits = strings.Repeat("0", max(scale+1-len(digits), 0)) + digits
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if unscaled.Sign() < 0 {
		return "-" + digits
	}
	return digits
}

// fixedBinaryWidth parses the n of an expected fixed[n].
func fixedBinaryWidth(expected string) (int, bool) {
	inner, ok := strings.CutPrefix(expected, "fixed[")
	if !ok || !strings.HasSuffix(inner, "]") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(inner, "]"))
	return n, err == nil && n > 0
}

// binaryExpectations picks the columns expected to land as bytes, with the precise type each
// should have: fixed[n] or binary.
func binaryExpectations(datatypeSchema, typeMapping map[string]string) map[string]string {
	precise := map[string]string{}
	for col, dbType := range datatypeSchema {
		expected := typeMapping[dbType]
		if _, fixed := fixedBinaryWidth(expected); fixed || expected == "binary" {
			precise[col] = expected
		}
	}
	return precise
}

// parquetKindOf is the parquet physical type an expected byte type must be written as.
func parquetKindOf(expected string) string {
	if n, fixed := fixedBinaryWidth(expected); fixed {
		return fmt.Sprintf("FIXED_LEN_BYTE_ARRAY(%d)", n)
	}
	return "BYTE_ARRAY"
}

// verifyIcebergBinaryTypes asserts the byte columns' types from the table's own metadata, where
// fixed[16] and binary are distinct; in Spark's view of the table they are not.
func verifyIcebergBinaryTypes(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, datatypeSchema, typeMapping map[string]string) {
	t.Helper()
	expected := binaryExpectations(datatypeSchema, typeMapping)
	if len(expected) == 0 {
		return
	}
	schema, err := testutils.IcebergSchemaTypes(ctx, spark, fullTableName)
	require.NoError(t, err, "failed to read the Iceberg schema of %s", fullTableName)
	actual := make(map[string]string, len(expected))
	for col := range expected {
		actual[col] = schema[col]
	}
	require.Equal(t, expected, actual, "binary columns of %s land with a different Iceberg type than expected (Spark shows fixed[n] as binary, so this reads the table metadata)", fullTableName)
	t.Logf("Verified fixed and variable-length binary types in the Iceberg schema of %s", fullTableName)
}

// verifyParquetBinaryTypes asserts the byte columns' physical types from every parquet file's
// footer, where FIXED_LEN_BYTE_ARRAY(16) and BYTE_ARRAY are distinct; in Spark's view of the
// files they are not.
func verifyParquetBinaryTypes(ctx context.Context, t *testing.T, parquetDB, tableName string, datatypeSchema, typeMapping map[string]string) {
	t.Helper()
	precise := binaryExpectations(datatypeSchema, typeMapping)
	if len(precise) == 0 {
		return
	}
	expected := make(map[string]string, len(precise))
	for col, typ := range precise {
		expected[col] = parquetKindOf(typ)
	}
	client, err := testutils.NewMinIOClient()
	require.NoError(t, err)
	files, err := testutils.ParquetColumnKinds(ctx, client, parquetDB, tableName)
	require.NoError(t, err, "failed to read the parquet footers of %s/%s", parquetDB, tableName)
	require.NotEmpty(t, files, "no parquet files found for %s/%s", parquetDB, tableName)
	for file, columns := range files {
		actual := make(map[string]string, len(expected))
		for col := range expected {
			actual[col] = columns[col]
		}
		require.Equal(t, expected, actual, "binary columns in %s carry a different parquet physical type than expected (Spark shows both as binary, so this reads the footer)", file)
	}
	t.Logf("Verified fixed and variable-length binary types in %d parquet footer(s) of %s/%s", len(files), parquetDB, tableName)
}
