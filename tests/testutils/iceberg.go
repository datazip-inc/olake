package testutils

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/minio/minio-go/v7"
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

// icebergMetadataFile is the part of an Iceberg metadata.json the readers here use.
type icebergMetadataFile struct {
	FormatVersion   int                     `json:"format-version"`
	LastColumnID    int                     `json:"last-column-id"`
	CurrentSchemaID int                     `json:"current-schema-id"`
	Schemas         []icebergMetadataSchema `json:"schemas"`
	Schema          *icebergMetadataSchema  `json:"schema"` // format version 1 carries a single schema
	DefaultSpecID   int                     `json:"default-spec-id"`
	PartitionSpecs  []struct {
		SpecID int `json:"spec-id"`
		Fields []struct {
			Name      string `json:"name"`
			Transform string `json:"transform"`
			SourceID  int    `json:"source-id"`
		} `json:"fields"`
	} `json:"partition-specs"`
	DefaultSortOrderID int `json:"default-sort-order-id"`
	SortOrders         []struct {
		OrderID int `json:"order-id"`
		Fields  []struct {
			Transform string `json:"transform"`
			SourceID  int    `json:"source-id"`
			Direction string `json:"direction"`
			NullOrder string `json:"null-order"`
		} `json:"fields"`
	} `json:"sort-orders"`
	Properties        map[string]string `json:"properties"`
	CurrentSnapshotID *int64            `json:"current-snapshot-id"`
	Snapshots         []struct {
		SnapshotID int64             `json:"snapshot-id"`
		Summary    map[string]string `json:"summary"`
	} `json:"snapshots"`
}

// icebergMetadataSchema is one schema of a metadata file. A field's type is a string for a primitive
// and an object for a struct, list or map.
type icebergMetadataSchema struct {
	SchemaID           int   `json:"schema-id"`
	IdentifierFieldIDs []int `json:"identifier-field-ids"`
	Fields             []struct {
		ID   int             `json:"id"`
		Name string          `json:"name"`
		Type json.RawMessage `json:"type"`
	} `json:"fields"`
}

// currentSchema is the schema the table reads with, nil when the file names one it does not carry.
func (f icebergMetadataFile) currentSchema() *icebergMetadataSchema {
	current := f.Schema
	for i := range f.Schemas {
		if f.Schemas[i].SchemaID == f.CurrentSchemaID {
			current = &f.Schemas[i]
		}
	}
	return current
}

func (f icebergMetadataFile) partitioned() bool {
	for _, spec := range f.PartitionSpecs {
		if spec.SpecID == f.DefaultSpecID {
			return len(spec.Fields) > 0
		}
	}
	return false
}

// readIcebergMetadataFile fetches and parses the metadata file the table's metadata log points at
// last, returning its location for error messages.
func readIcebergMetadataFile(ctx context.Context, spark sql.SparkSession, fullTableName string) (icebergMetadataFile, string, error) {
	var metadata icebergMetadataFile
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT file FROM %s.metadata_log_entries ORDER BY timestamp DESC LIMIT 1", fullTableName))
	if err != nil {
		return metadata, "", fmt.Errorf("failed to query the metadata log of %s: %s", fullTableName, err)
	}
	rows, err := df.Collect(ctx)
	if err != nil {
		return metadata, "", fmt.Errorf("failed to read the metadata log of %s: %s", fullTableName, err)
	}
	if len(rows) == 0 {
		return metadata, "", fmt.Errorf("%s has no metadata log entry", fullTableName)
	}
	location, _ := rows[0].Value("file").(string)
	bucket, key, found := strings.Cut(strings.TrimPrefix(location, "s3a://"), "/")
	if !strings.HasPrefix(location, "s3a://") || !found {
		return metadata, location, fmt.Errorf("%s: metadata location %q is not an s3a path", fullTableName, location)
	}
	client, err := NewMinIOClient()
	if err != nil {
		return metadata, location, err
	}
	object, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return metadata, location, fmt.Errorf("failed to fetch %s: %s", location, err)
	}
	defer func() { _ = object.Close() }()
	if err := json.NewDecoder(object).Decode(&metadata); err != nil {
		return metadata, location, fmt.Errorf("failed to parse %s: %s", location, err)
	}
	return metadata, location, nil
}

// IcebergSchemaTypes returns a table's current schema as column -> Iceberg type ("fixed[16]",
// "binary", "long", ...), read from the metadata file the table's metadata log points at. Spark
// reports Iceberg's fixed[n] as plain binary, so this is the only view in which a fixed width can
// be asserted.
func IcebergSchemaTypes(ctx context.Context, spark sql.SparkSession, fullTableName string) (map[string]string, error) {
	metadata, location, err := readIcebergMetadataFile(ctx, spark, fullTableName)
	if err != nil {
		return nil, err
	}
	current := metadata.currentSchema()
	if current == nil {
		return nil, fmt.Errorf("%s: schema %d not found in %s", fullTableName, metadata.CurrentSchemaID, location)
	}
	fieldTypes := make(map[string]string, len(current.Fields))
	for _, field := range current.Fields {
		var primitive string
		if json.Unmarshal(field.Type, &primitive) == nil {
			fieldTypes[field.Name] = primitive
		} else {
			fieldTypes[field.Name] = string(field.Type)
		}
	}
	return fieldTypes, nil
}

// Snapshot summary keys read per commit and on the current snapshot. File counts and sizes are left
// to MetaFiles.
var (
	snapshotRecordKeys = []string{"added-records", "deleted-records", "added-position-deletes", "removed-position-deletes", "added-equality-deletes", "removed-equality-deletes"}
	snapshotTotalKeys  = []string{"total-records", "total-position-deletes", "total-equality-deletes"}
	snapshotFileKeys   = []string{"total-data-files", "total-delete-files"}
)

// icebergFile is one row of the table's files metadata table, data and delete files alike.
type icebergFile struct {
	Content     string // 0 data, 1 position deletes (and deletion vectors), 2 equality deletes
	Format      string
	Partition   string // the partition tuple as JSON; "" for an unpartitioned table
	EqualityIDs []int
	Records     int64
	NullCounts  map[string]int64 // field id -> null count
}

// ReadIcebergMetadata reads and normalizes a table's metadata: its current metadata.json and its
// files metadata table.
func ReadIcebergMetadata(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string) DestinationMetadata {
	t.Helper()
	file, _, err := readIcebergMetadataFile(ctx, spark, fullTableName)
	require.NoError(t, err)
	files := readIcebergFiles(ctx, t, spark, fullTableName, file.partitioned())
	metadata, err := normalizeIcebergMetadata(file, files)
	require.NoErrorf(t, err, "failed to normalize the metadata of %s", fullTableName)
	return metadata
}

// readIcebergFiles reads the files metadata table with every value cast to a string, so nothing
// depends on how Spark Connect hands back ints, arrays or maps. An unpartitioned table's files
// table has no partition column.
func readIcebergFiles(ctx context.Context, t *testing.T, spark sql.SparkSession, fullTableName string, partitioned bool) []icebergFile {
	t.Helper()
	partition := "''"
	if partitioned {
		partition = "to_json(`partition`)"
	}
	query := fmt.Sprintf("SELECT CAST(content AS STRING) AS content, file_format, %s AS part, "+
		"COALESCE(to_json(equality_ids), '[]') AS equality_ids, CAST(record_count AS STRING) AS record_count, "+
		"COALESCE(to_json(null_value_counts), '{}') AS null_counts FROM %s.files", partition, fullTableName)
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to query the files of %s", fullTableName)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect the files of %s", fullTableName)

	files := make([]icebergFile, 0, len(rows))
	for _, row := range rows {
		file := icebergFile{
			Content:   fmt.Sprint(row.Value("content")),
			Format:    fmt.Sprint(row.Value("file_format")),
			Partition: fmt.Sprint(row.Value("part")),
		}
		file.Records, err = strconv.ParseInt(fmt.Sprint(row.Value("record_count")), 10, 64)
		require.NoErrorf(t, err, "record_count of a file in %s", fullTableName)
		require.NoErrorf(t, json.Unmarshal([]byte(fmt.Sprint(row.Value("equality_ids"))), &file.EqualityIDs), "equality_ids of a file in %s", fullTableName)
		require.NoErrorf(t, json.Unmarshal([]byte(fmt.Sprint(row.Value("null_counts"))), &file.NullCounts), "null_value_counts of a file in %s", fullTableName)
		files = append(files, file)
	}
	return files
}

// normalizeIcebergMetadata is the pure half of ReadIcebergMetadata, kept apart so it is unit-testable.
func normalizeIcebergMetadata(file icebergMetadataFile, files []icebergFile) (DestinationMetadata, error) {
	m := newDestinationMetadata()

	// Field ids become column names everywhere they are referenced. A column is named by its latest
	// schema, and each column's ids across schemas are collected to check it kept one.
	names := map[int]string{}
	ids := map[string]map[int]bool{}
	schemas := file.Schemas
	if len(schemas) == 0 && file.Schema != nil {
		schemas = []icebergMetadataSchema{*file.Schema} // format version 1 carries a single schema
	}
	for _, schema := range schemas {
		for _, field := range schema.Fields {
			names[field.ID] = field.Name
			if ids[field.Name] == nil {
				ids[field.Name] = map[int]bool{}
			}
			ids[field.Name][field.ID] = true
		}
	}
	name := func(id int) string {
		if n, ok := names[id]; ok {
			return n
		}
		return fmt.Sprintf("(unknown field %d)", id)
	}

	// spec
	m.set(MetaSpec, "format-version", strconv.Itoa(file.FormatVersion))
	for _, spec := range file.PartitionSpecs {
		if spec.SpecID != file.DefaultSpecID {
			continue
		}
		m.set(MetaSpec, "partition-fields", strconv.Itoa(len(spec.Fields)))
		for i, field := range spec.Fields {
			m.set(MetaSpec, fmt.Sprintf("partition[%d]", i), fmt.Sprintf("%s=%s(%s)", field.Name, field.Transform, name(field.SourceID)))
			m.PartitionColumns = append(m.PartitionColumns, name(field.SourceID))
		}
	}
	for _, order := range file.SortOrders {
		if order.OrderID != file.DefaultSortOrderID {
			continue
		}
		m.set(MetaSpec, "sort-fields", strconv.Itoa(len(order.Fields)))
		for i, field := range order.Fields {
			m.set(MetaSpec, fmt.Sprintf("sort[%d]", i), fmt.Sprintf("%s(%s) %s %s", field.Transform, name(field.SourceID), field.Direction, field.NullOrder))
		}
	}

	// identifier_fields and schemas
	current := file.currentSchema()
	if current == nil {
		return DestinationMetadata{}, fmt.Errorf("current schema %d not found among %d schemas", file.CurrentSchemaID, len(file.Schemas))
	}
	identifiers := make([]string, 0, len(current.IdentifierFieldIDs))
	for _, id := range current.IdentifierFieldIDs {
		identifiers = append(identifiers, name(id))
	}
	slices.Sort(identifiers)
	m.set(MetaIdentifierFields, "columns", strings.Join(identifiers, ","))
	m.set(MetaSchemas, "count", strconv.Itoa(len(file.Schemas)))
	m.set(MetaSchemas, "last-column-id", strconv.Itoa(file.LastColumnID))

	// field_ids: a column must keep its id across schema versions; readers match columns by id.
	var changed []string
	for column, columnIDs := range ids {
		if len(columnIDs) > 1 {
			changed = append(changed, column)
		}
	}
	slices.Sort(changed)
	m.set(MetaFieldIDs, "changed", strings.Join(changed, ","))

	// properties and olake_2pc
	for key, value := range file.Properties {
		if key != "olake_2pc" {
			m.set(MetaProperties, key, value)
		}
	}
	normalizeOlake2PC(m, file.Properties["olake_2pc"])

	// snapshots: how many, and how many of each kind of commit, in no order.
	m.set(MetaSnapshots, "count", strconv.Itoa(len(file.Snapshots)))
	for _, snapshot := range file.Snapshots {
		parts := []string{snapshot.Summary["operation"]}
		for _, key := range snapshotRecordKeys {
			if value, ok := snapshot.Summary[key]; ok {
				parts = append(parts, key+"="+value)
			}
		}
		m.add(MetaSnapshots, "commit["+strings.Join(parts, " ")+"]", 1)
		if file.CurrentSnapshotID != nil && snapshot.SnapshotID == *file.CurrentSnapshotID {
			for _, key := range snapshotTotalKeys {
				m.set(MetaTotals, key, snapshot.Summary[key])
			}
			for _, key := range snapshotFileKeys {
				m.set(MetaFiles, key, snapshot.Summary[key])
			}
		}
	}

	// files, data_records, delete_records, column_stats
	for _, f := range files {
		partition := f.Partition
		if partition == "" {
			partition = "(unpartitioned)"
		}
		switch f.Content {
		case "0":
			m.addPartition(MetaFiles, partition, "data %p", 1)
			m.addPartition(MetaDataRecords, partition, "%p", f.Records)
			for id, nulls := range f.NullCounts {
				fieldID, err := strconv.Atoi(id)
				if err != nil {
					return DestinationMetadata{}, fmt.Errorf("null count keyed by %q, not a field id", id)
				}
				m.add(MetaColumnStats, name(fieldID)+".null_count", nulls)
			}
		case "1", "2":
			kind := map[string]string{"1": "position", "2": "equality"}[f.Content]
			columns := make([]string, 0, len(f.EqualityIDs))
			for _, id := range f.EqualityIDs {
				columns = append(columns, name(id))
			}
			slices.Sort(columns)
			group := fmt.Sprintf("%%p %s %s [%s]", kind, f.Format, strings.Join(columns, ","))
			m.addPartition(MetaFiles, partition, "delete "+group, 1)
			m.addPartition(MetaDeleteRecords, partition, group, f.Records)
		default:
			return DestinationMetadata{}, fmt.Errorf("unknown file content %q", f.Content)
		}
	}
	return m, nil
}
