package compatibility

// Comparing the two runs' destination metadata.
//
// compareDestinationOutputs proves both sides hold the same rows. Two destinations can hold the same
// rows and still differ where readers and resumes look: one more snapshot per sync, equality deletes
// where the other wrote positional ones, a column without stats, an olake_2pc a later sync cannot
// resume from. This file reads each side's metadata, strips what differs by construction between two
// runs, and compares the rest as one flat map, so a difference reads as one line per key.
//
// What differs by construction, and is never compared: snapshot ids and their order (backfill chunks
// commit in any order), timestamps, object paths and sizes, Iceberg field ids (TypeSchema.ToIceberg
// walks a sync.Map, so a new table numbers its columns in a different order every run; ids are
// mapped back to column names instead), and olake_2pc's thread id and source positions (each side
// reads its own source table).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/require"
	"github.com/minio/minio-go/v7"
	"github.com/parquet-go/parquet-go"
)

// Metadata categories: the unit a metadata rule exempts. Not every destination fills every one.
const (
	metaSpec             = "spec"              // iceberg: format version, partition spec, sort order
	metaIdentifierFields = "identifier_fields" // iceberg: identifier columns
	metaSchemas          = "schemas"           // iceberg: schema versions, last column id
	metaFieldIDs         = "field_ids"         // iceberg: columns whose field id changed between schemas
	metaProperties       = "properties"        // iceberg: table properties; parquet: compression codecs
	metaOlake2PC         = "olake_2pc"         // iceberg: olake_2pc property; parquet: metadata.json
	metaStaging          = "staging"           // parquet: _olake_2pc objects left after the sync
	metaSnapshots        = "snapshots"         // iceberg: snapshot count and per-commit record counts
	metaTotals           = "totals"            // iceberg: current snapshot's record and delete totals
	metaFiles            = "files"             // file counts per partition and kind
	metaDataRecords      = "data_records"      // records per partition, from file metadata
	metaDeleteRecords    = "delete_records"    // iceberg: delete records per partition, kind and format
	metaColumnStats      = "column_stats"      // null counts per column, from file statistics
	metaFileSchema       = "file_schema"       // parquet: physical and logical type per column
)

var metadataCategories = []string{
	metaSpec, metaIdentifierFields, metaSchemas, metaFieldIDs, metaProperties, metaOlake2PC, metaStaging,
	metaSnapshots, metaTotals, metaFiles, metaDataRecords, metaDeleteRecords, metaColumnStats, metaFileSchema,
}

// Snapshot summary keys compared per commit and on the current snapshot. File counts and sizes are
// left to metaFiles, which a rule can drop on its own.
var (
	snapshotRecordKeys = []string{"added-records", "deleted-records", "added-position-deletes", "removed-position-deletes", "added-equality-deletes", "removed-equality-deletes"}
	snapshotTotalKeys  = []string{"total-records", "total-position-deletes", "total-equality-deletes"}
	snapshotFileKeys   = []string{"total-data-files", "total-delete-files"}
)

// destinationMetadata is one side's normalized metadata: category -> key -> value.
type destinationMetadata map[string]map[string]string

func (m destinationMetadata) set(category, key, value string) {
	if m[category] == nil {
		m[category] = map[string]string{}
	}
	m[category][key] = value
}

// add sums n into a counter key.
func (m destinationMetadata) add(category, key string, n int64) {
	current, _ := strconv.ParseInt(m[category][key], 10, 64)
	m.set(category, key, strconv.FormatInt(current+n, 10))
}

// flatten is the compared form: "category.key" -> value, without the skipped categories.
func (m destinationMetadata) flatten(skip []string) map[string]string {
	flat := map[string]string{}
	for category, values := range m {
		if slices.Contains(skip, category) {
			continue
		}
		for key, value := range values {
			flat[category+"."+key] = value
		}
	}
	return flat
}

// partitionStats collects the per-partition counts of files, data_records and delete_records. A key
// carries the partition as "%p", so it can be written either with the partition in it or, when
// partition values differ between the runs by construction, without.
type partitionStats map[string]map[string]map[string]int64 // category -> partition -> key -> count

func (p partitionStats) add(category, partition, key string, n int64) {
	if p[category] == nil {
		p[category] = map[string]map[string]int64{}
	}
	if p[category][partition] == nil {
		p[category][partition] = map[string]int64{}
	}
	p[category][partition][key] += n
}

// emit writes the counts into m. With opaque partitions -- partitioned on a column whose values
// cannot match across two runs, such as MongoDB's server-generated _id -- each partition is reduced
// to its counts, and what is compared is how many partitions have each set of counts.
func (p partitionStats) emit(m destinationMetadata, opaque bool) {
	for category, partitions := range p {
		for partition, counts := range partitions {
			if !opaque {
				for key, n := range counts {
					m.add(category, strings.ReplaceAll(key, "%p", partition), n)
				}
				continue
			}
			var signature []string
			for key, n := range counts {
				label := strings.Join(strings.Fields(strings.ReplaceAll(key, "%p", "")), " ")
				if label == "" {
					label = "records"
				}
				signature = append(signature, fmt.Sprintf("%s=%d", label, n))
			}
			slices.Sort(signature)
			m.add(category, "partitions["+strings.Join(signature, ", ")+"]", 1)
		}
	}
}

// opaquePartitions reports whether any partition source column is compared by type only, so its
// values -- and the partition values made from them -- differ between the two runs by construction.
func opaquePartitions(partitionColumns, typeOnly []string) bool {
	return slices.ContainsFunc(partitionColumns, func(column string) bool {
		return slices.Contains(typeOnly, column) || slices.Contains(typeOnly, testutils.Reformat(column))
	})
}

// partitionRegexColumns are the source columns a stream's partition regex names: the first token of
// each "{column,transform}" group.
func partitionRegexColumns(partitionRegex string) []string {
	var columns []string
	for _, group := range partitionGroup.FindAllStringSubmatch(partitionRegex, -1) {
		column, _, _ := strings.Cut(group[1], ",")
		if column = strings.TrimSpace(column); column != "" {
			columns = append(columns, column)
		}
	}
	return columns
}

var partitionGroup = regexp.MustCompile(`\{([^}]*)\}`)

// compareDestinationMetadata asserts both sides' metadata match outside the skipped categories. It
// runs after compareDestinationOutputs, so the rows already match.
func compareDestinationMetadata(t *testing.T, diag *diagnostics, reference, upgraded destinationMetadata, skip []string) {
	referenceFlat, upgradedFlat := reference.flatten(skip), upgraded.flatten(skip)
	if !maps.Equal(referenceFlat, upgradedFlat) {
		diag.fatalf(t, "destination metadata differs between the reference and upgrade runs.\n%s",
			indent(require.MapDiff("metadata key", "reference run", "post olake upgrade", referenceFlat, upgradedFlat), "  "))
	}
	t.Logf("metadata identical: %d keys compared, categories skipped by rule: %v", len(referenceFlat), skip)
}

// normalizeOlake2PC keeps what of an olake_2pc document both runs share: which fields it carries,
// dedup_inserts, and how many full-refresh commits it recorded. The thread id and the source
// position in state differ between the two runs by construction.
func normalizeOlake2PC(m destinationMetadata, raw string) {
	if raw == "" {
		m.set(metaOlake2PC, "present", "false")
		return
	}
	m.set(metaOlake2PC, "present", "true")
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		m.set(metaOlake2PC, "parse_error", err.Error())
		return
	}
	m.set(metaOlake2PC, "fields", strings.Join(slices.Sorted(maps.Keys(doc)), ","))
	if v, ok := doc["dedup_inserts"]; ok {
		m.set(metaOlake2PC, "dedup_inserts", fmt.Sprint(v))
	}
	if ids, ok := doc["full_refresh_committed_ids"].([]any); ok {
		m.set(metaOlake2PC, "full_refresh_committed_ids", strconv.Itoa(len(ids)))
	}
}

// --- Iceberg ---

// icebergMetadataFile is the part of metadata.json compared.
type icebergMetadataFile struct {
	FormatVersion   int                     `json:"format-version"`
	LastColumnID    int                     `json:"last-column-id"`
	CurrentSchemaID int                     `json:"current-schema-id"`
	Schemas         []icebergMetadataSchema `json:"schemas"`
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

type icebergMetadataSchema struct {
	SchemaID           int   `json:"schema-id"`
	IdentifierFieldIDs []int `json:"identifier-field-ids"`
	Fields             []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"fields"`
}

// icebergFile is one row of the table's files metadata table, data and delete files alike.
type icebergFile struct {
	Content     string // 0 data, 1 position deletes (and deletion vectors), 2 equality deletes
	Format      string
	Partition   string // the partition tuple as JSON; "" for an unpartitioned table
	EqualityIDs []int
	Records     int64
	NullCounts  map[string]int64 // field id -> null count
}

// readIcebergMetadata reads and normalizes one side's table: its current metadata.json and its files
// metadata table.
func readIcebergMetadata(ctx context.Context, t *testing.T, spark sql.SparkSession, table string, typeOnly []string) destinationMetadata {
	t.Helper()
	raw, location := readIcebergMetadataFile(ctx, t, spark, table)
	var file icebergMetadataFile
	require.NoErrorf(t, json.Unmarshal(raw, &file), "failed to parse %s", location)

	files := readIcebergFiles(ctx, t, spark, table, file.partitioned())
	metadata, err := normalizeIcebergMetadata(file, files, typeOnly)
	require.NoErrorf(t, err, "failed to normalize the metadata of %s", table)
	return metadata
}

// readIcebergMetadataFile fetches the metadata.json the table's metadata log points at last, as
// testutils.IcebergSchemaTypes does.
func readIcebergMetadataFile(ctx context.Context, t *testing.T, spark sql.SparkSession, table string) ([]byte, string) {
	t.Helper()
	df, err := spark.Sql(ctx, fmt.Sprintf("SELECT file FROM %s.metadata_log_entries ORDER BY timestamp DESC LIMIT 1", table))
	require.NoErrorf(t, err, "failed to query the metadata log of %s", table)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to read the metadata log of %s", table)
	require.NotEmpty(t, rows, "%s has no metadata log entry", table)
	location, _ := rows[0].Value("file").(string)
	bucket, key, found := strings.Cut(strings.TrimPrefix(location, "s3a://"), "/")
	require.Truef(t, strings.HasPrefix(location, "s3a://") && found, "%s: metadata location %q is not an s3a path", table, location)

	client, err := testutils.NewMinIOClient()
	require.NoError(t, err, "failed to create the MinIO client")
	object, err := client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	require.NoErrorf(t, err, "failed to fetch %s", location)
	defer func() { _ = object.Close() }()
	raw, err := io.ReadAll(object)
	require.NoErrorf(t, err, "failed to read %s", location)
	return raw, location
}

func (f icebergMetadataFile) partitioned() bool {
	for _, spec := range f.PartitionSpecs {
		if spec.SpecID == f.DefaultSpecID {
			return len(spec.Fields) > 0
		}
	}
	return false
}

// readIcebergFiles reads the files metadata table with every value cast to a string, so nothing
// depends on how Spark Connect hands back ints, arrays or maps. An unpartitioned table's files
// table has no partition column.
func readIcebergFiles(ctx context.Context, t *testing.T, spark sql.SparkSession, table string, partitioned bool) []icebergFile {
	t.Helper()
	partition := "''"
	if partitioned {
		partition = "to_json(`partition`)"
	}
	query := fmt.Sprintf("SELECT CAST(content AS STRING) AS content, file_format, %s AS part, "+
		"COALESCE(to_json(equality_ids), '[]') AS equality_ids, CAST(record_count AS STRING) AS record_count, "+
		"COALESCE(to_json(null_value_counts), '{}') AS null_counts FROM %s.files", partition, table)
	df, err := spark.Sql(ctx, query)
	require.NoErrorf(t, err, "failed to query the files of %s", table)
	rows, err := df.Collect(ctx)
	require.NoErrorf(t, err, "failed to collect the files of %s", table)

	files := make([]icebergFile, 0, len(rows))
	for _, row := range rows {
		file := icebergFile{
			Content:   fmt.Sprint(row.Value("content")),
			Format:    fmt.Sprint(row.Value("file_format")),
			Partition: fmt.Sprint(row.Value("part")),
		}
		file.Records, err = strconv.ParseInt(fmt.Sprint(row.Value("record_count")), 10, 64)
		require.NoErrorf(t, err, "record_count of a file in %s", table)
		require.NoErrorf(t, json.Unmarshal([]byte(fmt.Sprint(row.Value("equality_ids"))), &file.EqualityIDs), "equality_ids of a file in %s", table)
		require.NoErrorf(t, json.Unmarshal([]byte(fmt.Sprint(row.Value("null_counts"))), &file.NullCounts), "null_value_counts of a file in %s", table)
		files = append(files, file)
	}
	return files
}

// normalizeIcebergMetadata is the pure half of readIcebergMetadata, kept apart so it is unit-testable.
// typeOnly are the columns compared by type only; a partition on one of them is compared by its
// counts, not its values (partitionStats.emit).
func normalizeIcebergMetadata(file icebergMetadataFile, files []icebergFile, typeOnly []string) (destinationMetadata, error) {
	m := destinationMetadata{}

	// Field ids become column names everywhere they are referenced. A column is named by its latest
	// schema, and each column's ids across schemas are collected to check it kept one.
	names := map[int]string{}
	ids := map[string]map[int]bool{}
	for _, schema := range file.Schemas {
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
	m.set(metaSpec, "format-version", strconv.Itoa(file.FormatVersion))
	var partitionColumns []string
	for _, spec := range file.PartitionSpecs {
		if spec.SpecID != file.DefaultSpecID {
			continue
		}
		m.set(metaSpec, "partition-fields", strconv.Itoa(len(spec.Fields)))
		for i, field := range spec.Fields {
			m.set(metaSpec, fmt.Sprintf("partition[%d]", i), fmt.Sprintf("%s=%s(%s)", field.Name, field.Transform, name(field.SourceID)))
			partitionColumns = append(partitionColumns, name(field.SourceID))
		}
	}
	for _, order := range file.SortOrders {
		if order.OrderID != file.DefaultSortOrderID {
			continue
		}
		m.set(metaSpec, "sort-fields", strconv.Itoa(len(order.Fields)))
		for i, field := range order.Fields {
			m.set(metaSpec, fmt.Sprintf("sort[%d]", i), fmt.Sprintf("%s(%s) %s %s", field.Transform, name(field.SourceID), field.Direction, field.NullOrder))
		}
	}

	// identifier_fields and schemas
	var current *icebergMetadataSchema
	for i := range file.Schemas {
		if file.Schemas[i].SchemaID == file.CurrentSchemaID {
			current = &file.Schemas[i]
		}
	}
	if current == nil {
		return nil, fmt.Errorf("current schema %d not found among %d schemas", file.CurrentSchemaID, len(file.Schemas))
	}
	identifiers := make([]string, 0, len(current.IdentifierFieldIDs))
	for _, id := range current.IdentifierFieldIDs {
		identifiers = append(identifiers, name(id))
	}
	slices.Sort(identifiers)
	m.set(metaIdentifierFields, "columns", strings.Join(identifiers, ","))
	m.set(metaSchemas, "count", strconv.Itoa(len(file.Schemas)))
	m.set(metaSchemas, "last-column-id", strconv.Itoa(file.LastColumnID))

	// field_ids: a column must keep its id across schema versions; readers match columns by id.
	var changed []string
	for column, columnIDs := range ids {
		if len(columnIDs) > 1 {
			changed = append(changed, column)
		}
	}
	slices.Sort(changed)
	m.set(metaFieldIDs, "changed", strings.Join(changed, ","))

	// properties and olake_2pc
	for key, value := range file.Properties {
		if key != "olake_2pc" {
			m.set(metaProperties, key, value)
		}
	}
	normalizeOlake2PC(m, file.Properties["olake_2pc"])

	// snapshots: how many, and how many of each kind of commit, in no order.
	m.set(metaSnapshots, "count", strconv.Itoa(len(file.Snapshots)))
	for _, snapshot := range file.Snapshots {
		parts := []string{snapshot.Summary["operation"]}
		for _, key := range snapshotRecordKeys {
			if value, ok := snapshot.Summary[key]; ok {
				parts = append(parts, key+"="+value)
			}
		}
		m.add(metaSnapshots, "commit["+strings.Join(parts, " ")+"]", 1)
		if file.CurrentSnapshotID != nil && snapshot.SnapshotID == *file.CurrentSnapshotID {
			for _, key := range snapshotTotalKeys {
				m.set(metaTotals, key, snapshot.Summary[key])
			}
			for _, key := range snapshotFileKeys {
				m.set(metaFiles, key, snapshot.Summary[key])
			}
		}
	}

	// files, data_records, delete_records, column_stats
	byPartition := partitionStats{}
	for _, f := range files {
		partition := f.Partition
		if partition == "" {
			partition = "(unpartitioned)"
		}
		switch f.Content {
		case "0":
			byPartition.add(metaFiles, partition, "data %p", 1)
			byPartition.add(metaDataRecords, partition, "%p", f.Records)
			for id, nulls := range f.NullCounts {
				fieldID, err := strconv.Atoi(id)
				if err != nil {
					return nil, fmt.Errorf("null count keyed by %q, not a field id", id)
				}
				m.add(metaColumnStats, name(fieldID)+".null_count", nulls)
			}
		case "1", "2":
			kind := map[string]string{"1": "position", "2": "equality"}[f.Content]
			columns := make([]string, 0, len(f.EqualityIDs))
			for _, id := range f.EqualityIDs {
				columns = append(columns, name(id))
			}
			slices.Sort(columns)
			group := fmt.Sprintf("%%p %s %s [%s]", kind, f.Format, strings.Join(columns, ","))
			byPartition.add(metaFiles, partition, "delete "+group, 1)
			byPartition.add(metaDeleteRecords, partition, group, f.Records)
		default:
			return nil, fmt.Errorf("unknown file content %q", f.Content)
		}
	}
	byPartition.emit(m, opaquePartitions(partitionColumns, typeOnly))
	return m, nil
}

// --- Parquet ---

// parquetObject is one object of a parquet table's folder, by its key relative to that folder.
type parquetObject struct {
	key  string
	data []byte
}

// readParquetMetadata reads and normalizes one side's parquet table folder: metadata.json, what is
// left under _olake_2pc, and the footer of every data file. opaque is opaquePartitions for the
// stream's partition columns.
func readParquetMetadata(ctx context.Context, t *testing.T, db, table string, opaque bool) destinationMetadata {
	t.Helper()
	client, err := testutils.NewMinIOClient()
	require.NoError(t, err, "failed to create the MinIO client")
	keys, err := testutils.ParquetTableObjects(ctx, client, db, table)
	require.NoErrorf(t, err, "failed to list the objects of %s/%s", db, table)

	objects := make([]parquetObject, 0, len(keys))
	for _, key := range keys {
		object := parquetObject{key: key}
		// Staging is counted, not read: after a committed sync nothing should be left there.
		if key == parquetMetadataKey || strings.HasSuffix(key, ".parquet") && !strings.HasPrefix(key, parquetStagingPrefix) {
			object.data, err = testutils.ReadParquetTableObject(ctx, client, db, table, key)
			require.NoErrorf(t, err, "failed to read %s/%s/%s", db, table, key)
		}
		objects = append(objects, object)
	}
	metadata, err := normalizeParquetMetadata(objects, opaque)
	require.NoErrorf(t, err, "failed to normalize the metadata of %s/%s", db, table)
	return metadata
}

// The parquet writer's 2PC layout, relative to the table folder (destination/parquet/parquet_2pc.go).
const (
	parquetMetadataKey   = "metadata.json"
	parquetStagingPrefix = "_olake_2pc/"
)

// normalizeParquetMetadata is the pure half of readParquetMetadata, kept apart so it is unit-testable.
// A data file is grouped by its folder, which is its partition.
func normalizeParquetMetadata(objects []parquetObject, opaque bool) (destinationMetadata, error) {
	m := destinationMetadata{}
	byPartition := partitionStats{}
	m.set(metaStaging, "objects", "0")
	normalizeOlake2PC(m, "")

	schemas := map[string]map[string]bool{} // column -> its type in each file
	codecs := map[string]map[string]bool{}  // column -> its codec in each row group
	for _, object := range objects {
		switch {
		case strings.HasPrefix(object.key, parquetStagingPrefix):
			m.add(metaStaging, "objects", 1)
		case object.key == parquetMetadataKey:
			normalizeOlake2PC(m, string(object.data))
		case strings.HasSuffix(object.key, ".parquet"):
			partition := path.Dir(object.key)
			if partition == "." {
				partition = "(unpartitioned)"
			}
			file, err := parquet.OpenFile(bytes.NewReader(object.data), int64(len(object.data)))
			if err != nil {
				return nil, fmt.Errorf("failed to open %s: %s", object.key, err)
			}
			byPartition.add(metaFiles, partition, "data %p", 1)
			byPartition.add(metaDataRecords, partition, "%p", file.NumRows())

			for _, columnPath := range file.Schema().Columns() {
				leaf, _ := file.Schema().Lookup(columnPath...)
				column := strings.Join(columnPath, ".")
				if schemas[column] == nil {
					schemas[column] = map[string]bool{}
				}
				schemas[column][parquetColumnType(leaf.Node)] = true
			}
			for _, rowGroup := range file.Metadata().RowGroups {
				for _, chunk := range rowGroup.Columns {
					column := strings.Join(chunk.MetaData.PathInSchema, ".")
					m.add(metaColumnStats, column+".null_count", chunk.MetaData.Statistics.NullCount)
					if codecs[column] == nil {
						codecs[column] = map[string]bool{}
					}
					codecs[column][chunk.MetaData.Codec.String()] = true
				}
			}
		}
	}
	byPartition.emit(m, opaque)
	for column, types := range schemas {
		m.set(metaFileSchema, column, strings.Join(slices.Sorted(maps.Keys(types)), " | "))
	}
	for column, columnCodecs := range codecs {
		m.set(metaProperties, "codec."+column, strings.Join(slices.Sorted(maps.Keys(columnCodecs)), " | "))
	}
	return m, nil
}

// parquetColumnType is a leaf column's physical type with its width, logical type and repetition:
// the parts a reader decodes by, which Spark's schema folds together (INT96 and INT64 timestamps both
// read as timestamp).
func parquetColumnType(node parquet.Node) string {
	kind := node.Type().Kind().String()
	if node.Type().Kind() == parquet.FixedLenByteArray {
		kind = fmt.Sprintf("%s(%d)", kind, node.Type().Length())
	}
	logical := "-"
	if lt := node.Type().LogicalType(); lt != nil {
		logical = lt.String()
	}
	repetition := "required"
	switch {
	case node.Repeated():
		repetition = "repeated"
	case node.Optional():
		repetition = "optional"
	}
	return fmt.Sprintf("%s %s %s", kind, logical, repetition)
}
