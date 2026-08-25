package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils"
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
		defer testutils.TrackPhaseTiming(t, "spark", "session build")()
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
