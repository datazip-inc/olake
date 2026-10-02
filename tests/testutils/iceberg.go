package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/minio/minio-go/v7"
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

// icebergSchema is the part of an Iceberg metadata file that names column types. A field's type
// is a string for a primitive and an object for a struct, list or map.
type icebergSchema struct {
	SchemaID int `json:"schema-id"`
	Fields   []struct {
		Name string          `json:"name"`
		Type json.RawMessage `json:"type"`
	} `json:"fields"`
}

// IcebergSchemaTypes returns a table's current schema as column -> Iceberg type ("fixed[16]",
// "binary", "long", ...), read from the metadata file the table's metadata log points at. Spark
// reports Iceberg's fixed[n] as plain binary, so this is the only view in which a fixed width can
// be asserted.
func IcebergSchemaTypes(ctx context.Context, spark sql.SparkSession, fullTableName string) (map[string]string, error) {
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT file FROM %s.metadata_log_entries ORDER BY timestamp DESC LIMIT 1", fullTableName))
	if err != nil {
		return nil, fmt.Errorf("failed to query the metadata log of %s: %s", fullTableName, err)
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the metadata log of %s: %s", fullTableName, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%s has no metadata log entry", fullTableName)
	}
	location, _ := rows[0].Value("file").(string)
	bucket, key, found := strings.Cut(strings.TrimPrefix(location, "s3a://"), "/")
	if !strings.HasPrefix(location, "s3a://") || !found {
		return nil, fmt.Errorf("%s: metadata location %q is not an s3a path", fullTableName, location)
	}
	client, err := NewMinIOClient()
	if err != nil {
		return nil, err
	}
	object, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %s", location, err)
	}
	defer func() { _ = object.Close() }()
	var metadata struct {
		CurrentSchemaID int             `json:"current-schema-id"`
		Schemas         []icebergSchema `json:"schemas"`
		Schema          *icebergSchema  `json:"schema"` // format version 1 carries a single schema
	}
	if err := json.NewDecoder(object).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %s", location, err)
	}
	current := metadata.Schema
	for i := range metadata.Schemas {
		if metadata.Schemas[i].SchemaID == metadata.CurrentSchemaID {
			current = &metadata.Schemas[i]
		}
	}
	if current == nil {
		return nil, fmt.Errorf("%s: schema %d not found in %s", fullTableName, metadata.CurrentSchemaID, location)
	}
	types := make(map[string]string, len(current.Fields))
	for _, field := range current.Fields {
		var primitive string
		if json.Unmarshal(field.Type, &primitive) == nil {
			types[field.Name] = primitive
		} else {
			types[field.Name] = string(field.Type)
		}
	}
	return types, nil
}
