package testutils

// One destination table's metadata, normalized so that two runs of the same input describe their
// tables alike: ReadIcebergMetadata (iceberg.go) and ReadParquetMetadata (parquet.go) build it.
//
// What differs by construction between two runs is left out: snapshot ids and their order (backfill
// chunks commit in any order), timestamps, object paths and sizes, Iceberg field ids
// (TypeSchema.ToIceberg walks a sync.Map, so a new table numbers its columns in a different order
// every run; ids are mapped back to column names instead), and olake_2pc's thread id and source
// positions (each run reads its own source table).

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Metadata categories: the first part of every key, grouping what is read. Not every destination
// fills every one.
const (
	MetaSpec             = "spec"              // iceberg: format version, partition spec, sort order
	MetaIdentifierFields = "identifier_fields" // iceberg: identifier columns
	MetaSchemas          = "schemas"           // iceberg: schema versions, last column id
	MetaFieldIDs         = "field_ids"         // iceberg: columns whose field id changed between schemas
	MetaProperties       = "properties"        // iceberg: table properties; parquet: compression codecs
	MetaOlake2PC         = "olake_2pc"         // iceberg: olake_2pc property; parquet: metadata.json
	MetaStaging          = "staging"           // parquet: _olake_2pc objects left after the sync
	MetaSnapshots        = "snapshots"         // iceberg: snapshot count and per-commit record counts
	MetaTotals           = "totals"            // iceberg: current snapshot's record and delete totals
	MetaFiles            = "files"             // file counts per partition and kind
	MetaDataRecords      = "data_records"      // records per partition, from file metadata
	MetaDeleteRecords    = "delete_records"    // iceberg: delete records per partition, kind and format
	MetaColumnStats      = "column_stats"      // null counts per column, from file statistics
	MetaFileSchema       = "file_schema"       // parquet: physical and logical type per column
)

// DestinationMetadata is one table's normalized metadata.
type DestinationMetadata struct {
	// Values is category -> key -> value.
	Values map[string]map[string]string
	// Partitions holds the per-partition counts of files, data_records and delete_records, as read:
	// category -> partition -> key -> count. A key carries the partition as "%p", so a caller can
	// write it either with the partition in it or, when partition values cannot be compared, without.
	Partitions map[string]map[string]map[string]int64
	// PartitionColumns are the source columns of the table's current partition spec. Parquet keeps
	// no spec, so it leaves them empty; the stream's partition regex names them instead
	// (TestConfig.PartitionColumns).
	PartitionColumns []string
}

func newDestinationMetadata() DestinationMetadata {
	return DestinationMetadata{Values: map[string]map[string]string{}, Partitions: map[string]map[string]map[string]int64{}}
}

func (m DestinationMetadata) set(category, key, value string) {
	if m.Values[category] == nil {
		m.Values[category] = map[string]string{}
	}
	m.Values[category][key] = value
}

// add sums n into a counter key.
func (m DestinationMetadata) add(category, key string, n int64) {
	current, _ := strconv.ParseInt(m.Values[category][key], 10, 64)
	m.set(category, key, strconv.FormatInt(current+n, 10))
}

// addPartition sums n into a partition's counter key.
func (m DestinationMetadata) addPartition(category, partition, key string, n int64) {
	if m.Partitions[category] == nil {
		m.Partitions[category] = map[string]map[string]int64{}
	}
	if m.Partitions[category][partition] == nil {
		m.Partitions[category][partition] = map[string]int64{}
	}
	m.Partitions[category][partition][key] += n
}

// normalizeOlake2PC keeps what of an olake_2pc document two runs share: which fields it carries,
// dedup_inserts, and how many full-refresh commits it recorded. The thread id and the source
// position in state differ between two runs by construction. Each field is its own key and an
// absent document writes none, so a document or field a newer build adds is a new key rather than a
// changed value.
func normalizeOlake2PC(m DestinationMetadata, raw string) {
	if raw == "" {
		return
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		m.set(MetaOlake2PC, "parse_error", err.Error())
		return
	}
	for field := range doc {
		m.set(MetaOlake2PC, "field."+field, "present")
	}
	if v, ok := doc["dedup_inserts"]; ok {
		m.set(MetaOlake2PC, "dedup_inserts", fmt.Sprint(v))
	}
	if ids, ok := doc["full_refresh_committed_ids"].([]any); ok {
		m.set(MetaOlake2PC, "full_refresh_committed_ids", strconv.Itoa(len(ids)))
	}
}
