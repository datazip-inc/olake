package testutils

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/stretchr/testify/require"
)

const (
	IcebergCatalog = "olake_iceberg"
	// IP literal, not "localhost": a hostname sends grpc-go through a DNS resolver that stalls
	// every new connection ~20s when the DNS servers are slow (measured 20.22s vs 42ms).
	sparkConnectAddress = "sc://127.0.0.1:15002"
)

// The Spark Connect session is shared: each one costs ~175ms to build and every verification needs it.
var (
	sharedSparkOnce sync.Once
	sharedSpark     sql.SparkSession
	sharedSparkErr  error
)

// sparkSession returns the shared Spark Connect session, building it on first use and warming it
// so the one-off server bootstrap is timed here instead of inflating whichever verify runs first.
func SparkSession(ctx context.Context, t *testing.T) (sql.SparkSession, error) {
	sharedSparkOnce.Do(func() {
		// The shared session outlives whichever test builds it, so its construction must not be
		// tied to that test's context (t.Context cancels when the test ends).
		ctx := context.WithoutCancel(ctx)
		defer TrackPhaseTiming(t, "spark", "session build")()
		for attempt := 1; ; attempt++ {
			sharedSpark, sharedSparkErr = sql.NewSessionBuilder().Remote(sparkConnectAddress).Build(ctx)
			if sharedSparkErr == nil || attempt == 3 {
				break
			}
			t.Logf("Attempt %d/3: Failed to connect to Spark, retrying in 2s: %v", attempt, sharedSparkErr)
			time.Sleep(2 * time.Second)
		}
		if sharedSparkErr != nil {
			return
		}
		// Spark's vectorized parquet reader mis-decodes DELTA_LENGTH_BYTE_ARRAY columns that hold
		// nulls, reading every value after a null back as "" -- which reads as a data bug in a file
		// the writer got right. Session-scoped, so every query below sees what was actually written.
		if _, err := sharedSpark.Sql(ctx, "SET spark.sql.parquet.enableVectorizedReader=false"); err != nil {
			t.Logf("WARNING: could not disable Spark's vectorized parquet reader, so parquet assertions may report spurious empty strings for nullable byte-array columns: %v", err)
		}
		if _, err := sharedSpark.Sql(ctx, "SELECT 1"); err != nil {
			t.Logf("Spark session warm-up query failed (non-fatal): %v", err)
		}
	})
	return sharedSpark, sharedSparkErr
}

// RefreshTable reloads a table's latest snapshot: the shared session caches snapshots, so a table
// written after it was first read would otherwise read stale.
func RefreshTable(ctx context.Context, spark sql.SparkSession, table string) error {
	_, err := spark.Sql(ctx, "REFRESH TABLE "+table)
	return err
}

// DescribeSchema maps each column of a table or view to its data type, skipping the blank and "#"
// section rows DESCRIBE appends; the raw rows are returned for callers that read those sections.
func DescribeSchema(ctx context.Context, t *testing.T, spark sql.SparkSession, table string) (map[string]string, []types.Row) {
	t.Helper()
	df, err := spark.Sql(ctx, "DESCRIBE TABLE "+table)
	require.NoErrorf(t, err, "failed to describe %s", table)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect the description of %s", table)

	schema := make(map[string]string, len(rows))
	for _, row := range rows {
		col, ok := row.Value("col_name").(string)
		require.Truef(t, ok, "DESCRIBE %s: col_name is not a string: %T", table, row.Value("col_name"))
		dataType, ok := row.Value("data_type").(string)
		require.Truef(t, ok, "DESCRIBE %s: data_type is not a string: %T", table, row.Value("data_type"))
		if col != "" && !strings.HasPrefix(col, "#") {
			schema[col] = dataType
		}
	}
	return schema, rows
}

// dropIcebergTable drops an Iceberg table using Spark SQL
func DropIcebergTable(t *testing.T, tableName, icebergDB string) {
	t.Helper()
	ctx := t.Context()
	spark, err := SparkSession(ctx, t)
	if err != nil {
		t.Logf("Failed to connect to Spark Connect server for dropping table: %v", err)
		return
	}

	fullTableName := fmt.Sprintf("%s.%s.%s", IcebergCatalog, icebergDB, tableName)
	dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName)
	t.Logf("Dropping Iceberg table: %s", dropQuery)

	_, err = spark.Sql(ctx, dropQuery)
	if err != nil {
		t.Logf("Failed to drop Iceberg table %s: %v", fullTableName, err)
		return
	}
	t.Logf("Successfully dropped Iceberg table: %s", fullTableName)
}
