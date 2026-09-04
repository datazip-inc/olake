package types

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	oldSchemaTemplate = map[string]*Property{
		"id": {
			Type:                  NewSet(Int64),
			DestinationColumnName: "id",
		},
		"name": {
			Type:                  NewSet(String),
			DestinationColumnName: "name",
		},
	}

	newSchemaTemplate = map[string]*Property{
		"id": {
			Type:                  NewSet(Float64),
			DestinationColumnName: "id",
		},
		"email": {
			Type:                  NewSet(String),
			DestinationColumnName: "email",
		},
	}
)

func boolPtr(b bool) *bool { return &b }

func oldSchema() *TypeSchema {
	return createSchemaFromTemplate(oldSchemaTemplate)
}

func newSchema() *TypeSchema {
	return createSchemaFromTemplate(newSchemaTemplate)
}

func createSchemaFromTemplate(template map[string]*Property) *TypeSchema {
	schema := NewTypeSchema()

	for key, prop := range template {
		propCopy := &Property{
			Type:                  prop.Type,
			DestinationColumnName: prop.DestinationColumnName,
		}
		schema.Properties.Store(key, propCopy)
	}
	return schema
}

func createSelectedColumns(columns []string, syncNewColumns bool) *SelectedColumns {
	return &SelectedColumns{
		Columns:        columns,
		SyncNewColumns: syncNewColumns,
	}
}

func compareCatalogs(t *testing.T, expected, actual *Catalog, testName string) {
	assert.Equal(t, len(expected.Streams), len(actual.Streams))

	for i := range expected.Streams {
		es, as := expected.Streams[i].Stream, actual.Streams[i].Stream
		assert.Equal(t, es.Name, as.Name)
		assert.Equal(t, es.Namespace, as.Namespace)
		assert.Equal(t, es.SyncMode, as.SyncMode)
		assert.Equal(t, es.CursorField, as.CursorField)
		assert.Equal(t, es.DestinationDatabase, as.DestinationDatabase)
		assert.Equal(t, es.DestinationTable, as.DestinationTable)
		validateBasicSchemas(t, es.Schema, as.Schema, testName)
	}

	// to handle non-deterministic ordering
	sortSelectedStreams(expected.SelectedStreams)
	sortSelectedStreams(actual.SelectedStreams)

	assert.Equal(t, expected.SelectedStreams, actual.SelectedStreams)
}

func sortSelectedStreams(selectedStreams map[string][]StreamMetadata) {
	for _, metadataList := range selectedStreams {
		for i := range metadataList {
			if metadataList[i].SelectedColumns != nil && metadataList[i].SelectedColumns.Columns != nil {
				sort.Strings(metadataList[i].SelectedColumns.Columns)
			}
		}
	}
}

func TestCatalogGetWrappedCatalog(t *testing.T) {
	testCases := []struct {
		name     string
		streams  []*Stream
		driver   string
		expected *Catalog
	}{
		// empty streams slice should return empty catalog
		{
			name:    "empty streams",
			streams: []*Stream{},
			driver:  "postgres",
			expected: &Catalog{
				Streams:         []*ConfiguredStream{},
				SelectedStreams: make(map[string][]StreamMetadata),
			},
		},
		// nil streams slice should return empty catalog
		{
			name:    "nil streams slice",
			streams: nil,
			driver:  "mysql",
			expected: &Catalog{
				Streams:         []*ConfiguredStream{},
				SelectedStreams: make(map[string][]StreamMetadata),
			},
		},
		// single stream in postgres
		{
			name: "single stream - relational driver (postgres)",
			streams: []*Stream{
				{
					Name:      "stream1",
					Namespace: "namespace1",
					Schema:    &TypeSchema{Properties: sync.Map{}},
				},
			},
			driver: "postgres",
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "stream1",
							Namespace: "namespace1",
							Schema:    &TypeSchema{Properties: sync.Map{}},
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:     "stream1",
							PartitionRegex: "",
						},
					},
				},
			},
		},
		{
			name: "single stream - non-relational driver (mongodb)",
			streams: []*Stream{
				{
					Name:      "collection1",
					Namespace: "database1",
					Schema:    &TypeSchema{Properties: sync.Map{}},
				},
			},
			driver: "mongodb",
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "collection1",
							Namespace: "database1",
							Schema:    &TypeSchema{Properties: sync.Map{}},
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"database1": {
						{
							StreamName:     "collection1",
							PartitionRegex: "",
						},
					},
				},
			},
		},
		// multiple streams tests
		{
			name: "multiple streams with complete properties",
			streams: []*Stream{
				{
					Name:                    "users",
					Namespace:               "public",
					Schema:                  &TypeSchema{Properties: sync.Map{}},
					SupportedSyncModes:      NewSet(SyncMode("full_refresh"), SyncMode("incremental")),
					SourceDefinedPrimaryKey: NewSet("id"),
					AvailableCursorFields:   NewSet("updated_at", "created_at"),
					SyncMode:                SyncMode("incremental"),
					CursorField:             "updated_at",
					DestinationDatabase:     "analytics",
					DestinationTable:        "dim_users",
				},
				{
					Name:                    "orders",
					Namespace:               "public",
					Schema:                  &TypeSchema{Properties: sync.Map{}},
					SupportedSyncModes:      NewSet(SyncMode("full_refresh"), SyncMode("cdc")),
					SourceDefinedPrimaryKey: NewSet("order_id"),
					AvailableCursorFields:   NewSet("order_date"),
					SyncMode:                SyncMode("cdc"),
					DestinationDatabase:     "analytics",
					DestinationTable:        "fact_orders",
				},
			},
			driver: "postgres",
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "users",
							Namespace:               "public",
							Schema:                  &TypeSchema{Properties: sync.Map{}},
							SupportedSyncModes:      NewSet(SyncMode("full_refresh"), SyncMode("incremental")),
							SourceDefinedPrimaryKey: NewSet("id"),
							AvailableCursorFields:   NewSet("updated_at", "created_at"),
							SyncMode:                SyncMode("incremental"),
							CursorField:             "updated_at",
							DestinationDatabase:     "analytics",
							DestinationTable:        "dim_users",
						},
					},
					{
						Stream: &Stream{
							Name:                    "orders",
							Namespace:               "public",
							Schema:                  &TypeSchema{Properties: sync.Map{}},
							SupportedSyncModes:      NewSet(SyncMode("full_refresh"), SyncMode("cdc")),
							SourceDefinedPrimaryKey: NewSet("order_id"),
							AvailableCursorFields:   NewSet("order_date"),
							SyncMode:                SyncMode("cdc"),
							DestinationDatabase:     "analytics",
							DestinationTable:        "fact_orders",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{
							StreamName:     "users",
							PartitionRegex: "",
							CursorField:    "updated_at",
						},
						{
							StreamName:     "orders",
							PartitionRegex: "",
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetWrappedCatalog(tc.streams, tc.driver)
			compareCatalogs(t, tc.expected, result, tc.name)

			if len(tc.streams) > 0 {
				for i := range tc.streams {
					assert.Same(t, tc.streams[i], result.Streams[i].Stream, "Stream pointer reference should be preserved")
				}
			}
		})
	}
}

func TestCatalogMergeCatalogs(t *testing.T) {
	testCases := []struct {
		name       string
		oldCatalog *Catalog
		newCatalog *Catalog
		expected   *Catalog
	}{
		// when old catalog is nil, new catalog should be returned unchanged
		{
			name:       "nil old catalog returns new catalog unchanged",
			oldCatalog: nil,
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "stream1",
							Namespace: "namespace1",
							Schema:    oldSchema(),
							SyncMode:  SyncMode("full_refresh"),
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "test_regex", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id"}, false)},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:      "stream1",
							Namespace: "namespace1",
							Schema:    oldSchema(),
							SyncMode:  SyncMode("full_refresh"),
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "test_regex", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id"}, false)},
					},
				},
			},
		},
		// when merging single stream, old catalog metadata and selected stream data should be preserved
		{
			name: "single stream merge -- old stream fields carried forward",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("cdc"),
							SourceDefinedPrimaryKey: NewSet("id"),
							AvailableCursorFields:   NewSet("updated_at", "created_at"),
							CursorField:             "updated_at",
							DestinationDatabase:     "db:namespace1",
							DestinationTable:        "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  newSchema(),
							SupportedSyncModes:      NewSet(SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("incremental"),
							AvailableCursorFields:   NewSet("created_at"),
							SourceDefinedPrimaryKey: NewSet("id"),
							CursorField:             "created_at",
							DestinationDatabase:     "db:namespace1",
							DestinationTable:        "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "new_partition", Filter: "new_filter <= 8", AppendMode: boolPtr(false), Normalization: boolPtr(false), SelectedColumns: createSelectedColumns([]string{"id", "email", "created_at"}, false)},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  newSchema(),
							SupportedSyncModes:      NewSet(SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("cdc"), // from old stream
							SourceDefinedPrimaryKey: NewSet("id"),
							AvailableCursorFields:   NewSet("created_at"),
							CursorField:             "updated_at",    // from old stream
							DestinationDatabase:     "db:namespace1", // from old stream
							DestinationTable:        "stream1",       // from old stream
						},
					},
				},
				// selected_streams carries old metadata (AppendMode/Normalization/PartitionRegex/Filter)
				// SyncMode/CursorField/DestDB/DestTable live on Stream, not duplicated into metadata
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:      "stream1",
							PartitionRegex:  "user_partition",
							Filter:          "test_filter > 10",
							AppendMode:      boolPtr(true),
							Normalization:   boolPtr(true),
							SelectedColumns: createSelectedColumns([]string{"id"}, false), // "name" dropped (not in new schema)
						},
					},
				},
			},
		},
		// new stream introduced, existing stream keeps old config, new stream gets discover defaults
		{
			name: "new stream introduced",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SyncMode:                SyncMode("incremental"),
							SourceDefinedPrimaryKey: NewSet("id"),
							CursorField:             "updated_at",
							DestinationDatabase:     "db:namespace1",
							DestinationTable:        "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "old_partition", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SyncMode:                SyncMode("cdc"),
							CursorField:             "id",
							SourceDefinedPrimaryKey: NewSet("id"),
							DestinationDatabase:     "db:newNamespace1",
							DestinationTable:        "newStream1",
						},
					},
					{
						Stream: &Stream{
							Name:                    "stream2",
							Namespace:               "namespace2",
							Schema:                  newSchema(),
							SyncMode:                SyncMode("full_refresh"),
							SourceDefinedPrimaryKey: NewSet("id"),
							DestinationDatabase:     "db:namespace2",
							DestinationTable:        "stream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "new_partition", Filter: "new_filter <= 8", AppendMode: boolPtr(false), Normalization: boolPtr(false), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "", Filter: "new_filter <= 8", AppendMode: boolPtr(false), Normalization: boolPtr(false), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SyncMode:            SyncMode("incremental"), // preserved from old
							CursorField:         "updated_at",            // preserved from old
							DestinationDatabase: "db:namespace1",         // preserved from old
							DestinationTable:    "stream1",               // preserved from old
						},
					},
					{
						Stream: &Stream{
							Name:                "stream2",
							Namespace:           "namespace2",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "db:namespace2", // new stream keeps its discover dest
							DestinationTable:    "stream2",
						},
					},
				},
				// stream2 is NOT selected, only stream1 from old selected_streams carries forward
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:      "stream1",
							PartitionRegex:  "old_partition",
							Filter:          "test_filter > 10",
							AppendMode:      boolPtr(true),
							Normalization:   boolPtr(true),
							SelectedColumns: createSelectedColumns([]string{"id", "name"}, false),
						},
					},
				},
			},
		},
		// removed stream drops from selected_streams; remaining stream keeps its config
		{
			name: "old stream removed",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("incremental"),
							CursorField:             "id",
							SourceDefinedPrimaryKey: NewSet("id"),
							DestinationDatabase:     "db:newNamespace1",
							DestinationTable:        "newStream1",
						},
					},
					{
						Stream: &Stream{
							Name:                    "stream2",
							Namespace:               "namespace2",
							Schema:                  newSchema(),
							SourceDefinedPrimaryKey: NewSet("id"),
							SyncMode:                SyncMode("full_refresh"),
							DestinationDatabase:     "db:namespace2",
							DestinationTable:        "stream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("incremental"),
							SourceDefinedPrimaryKey: NewSet("id"),
							CursorField:             "updated_at",
							DestinationDatabase:     "db:namespace1",
							DestinationTable:        "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "new_filter <= 8", AppendMode: boolPtr(false), Normalization: boolPtr(false), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SourceDefinedPrimaryKey: NewSet("id"),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("incremental"), // from old stream
							CursorField:             "id",                    // from old stream
							DestinationDatabase:     "db:newNamespace1",      // from old stream
							DestinationTable:        "newStream1",            // from old stream
						},
					},
				},
				// stream2 was removed from new catalog → dropped from selected_streams
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:      "stream1",
							PartitionRegex:  "user_partition",
							Filter:          "test_filter > 10",
							AppendMode:      boolPtr(true),
							Normalization:   boolPtr(true),
							SelectedColumns: createSelectedColumns([]string{"id", "name"}, false),
						},
					},
				},
			},
		},
		// when destination database is updated, old catalog metadata should be preserved
		{
			name: "destination database updation",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SourceDefinedPrimaryKey: NewSet("id"),
							SyncMode:                SyncMode("incremental"),
							CursorField:             "id",
							DestinationDatabase:     "",
							DestinationTable:        "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("incremental"),
							SourceDefinedPrimaryKey: NewSet("id"),
							CursorField:             "updated_at",
							DestinationDatabase:     "db:namespace1",
							DestinationTable:        "newStream1",
						},
					},
					{
						Stream: &Stream{
							Name:                    "stream2",
							Namespace:               "namespace2",
							Schema:                  newSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("full_refresh"),
							SourceDefinedPrimaryKey: NewSet("id"),
							DestinationDatabase:     "db:namespace2",
							DestinationTable:        "newStream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: boolPtr(true), Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "another_partition", Filter: "new_filter <= 8", AppendMode: boolPtr(false), Normalization: boolPtr(false), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                    "stream1",
							Namespace:               "namespace1",
							Schema:                  oldSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("incremental"),
							CursorField:             "id", // from old stream
							SourceDefinedPrimaryKey: NewSet("id"),
							DestinationDatabase:     "", // from old stream (empty -- no prefix)
							DestinationTable:        "stream1",
						},
					},
					{
						Stream: &Stream{
							Name:                    "stream2",
							Namespace:               "namespace2",
							Schema:                  newSchema(),
							SupportedSyncModes:      NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:                SyncMode("full_refresh"),
							SourceDefinedPrimaryKey: NewSet("id"),
							DestinationDatabase:     "", // prefix="" so no override
							DestinationTable:        "newStream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{
							StreamName:      "stream1",
							PartitionRegex:  "user_partition",
							Filter:          "test_filter > 10",
							Normalization:   boolPtr(true),
							SelectedColumns: createSelectedColumns([]string{"id", "name"}, false),
						},
					},
				},
			},
		},
		// when old stream has empty CursorField, new-catalogs CursorField should be used instead of being overwritten
		{
			name: "use new cursor field when old cursor field is empty",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "users",
							Namespace:           "public",
							Schema:              oldSchema(),
							SyncMode:            SyncMode("full_refresh"),
							CursorField:         "",
							DestinationDatabase: "db:public",
							DestinationTable:    "users",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{StreamName: "users", Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "users",
							Namespace:           "public",
							Schema:              newSchema(),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "created_at",
							DestinationDatabase: "db:public",
							DestinationTable:    "users",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{StreamName: "users", Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "users",
							Namespace:           "public",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"), // from old stream
							CursorField:         "created_at",             // old was empty → new value kept
							DestinationDatabase: "db:public",
							DestinationTable:    "users",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{
							StreamName:      "users",
							Normalization:   boolPtr(true),
							SelectedColumns: createSelectedColumns([]string{"id"}, false), // "name" dropped, "email" new+not-sync
						},
					},
				},
			},
		},
		// StreamMetadata.SyncMode/CursorField/DestDB/DestTable survive round-trip through merge
		// (the whole metadata record is preserved as-is from old selected_streams)
		{
			name: "selected_streams configurable fields survive merge",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "users",
							Namespace:           "public",
							Schema:              oldSchema(),
							SyncMode:            SyncMode("full_refresh"), // stream says full_refresh
							DestinationDatabase: "db:public",
							DestinationTable:    "users",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{
							StreamName: "users",
							// user overrides via selected_streams:
							SyncMode:            INCREMENTAL,
							CursorField:         "updated_at",
							DestinationDatabase: "custom:public",
							DestinationTable:    "custom_users",
							Normalization:       boolPtr(true),
						},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "users",
							Namespace:           "public",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "db:public",
							DestinationTable:    "users",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{StreamName: "users"},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "users",
							Namespace:           "public",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"), // Stream.SyncMode from old stream
							DestinationDatabase: "db:public",              // Stream.DestDB from old stream
							DestinationTable:    "users",
						},
					},
				},
				// The whole old metadata record is carried forward intact, including the
				// user-set SyncMode/CursorField/DestDB/DestTable overrides.
				SelectedStreams: map[string][]StreamMetadata{
					"public": {
						{
							StreamName:          "users",
							SyncMode:            INCREMENTAL,
							CursorField:         "updated_at",
							DestinationDatabase: "custom:public",
							DestinationTable:    "custom_users",
							Normalization:       boolPtr(true),
						},
					},
				},
			},
		},
		// new stream not yet selected -- keeps its discover dest on Stream, not added to selected_streams
		{
			name: "new stream is not selected -- not added to selected_streams",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "a", Namespace: "ns1", Schema: oldSchema(), DestinationDatabase: "pg:ns1"}},
					{Stream: &Stream{Name: "b", Namespace: "ns2", Schema: oldSchema(), DestinationDatabase: "pg:ns2"}},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"ns1": {{StreamName: "a", DestinationDatabase: "pg:ns1"}},
					"ns2": {{StreamName: "b", DestinationDatabase: "pg:ns2"}},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "a", Namespace: "ns1", Schema: newSchema(), DestinationDatabase: "pg:ns1"}},
					{Stream: &Stream{Name: "b", Namespace: "ns2", Schema: newSchema(), DestinationDatabase: "pg:ns2"}},
					{Stream: &Stream{Name: "c", Namespace: "ns3", Schema: newSchema(), DestinationDatabase: "pg:ns3"}},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"ns1": {{StreamName: "a", Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id"}, true)}},
					"ns2": {{StreamName: "b", Normalization: boolPtr(true), SelectedColumns: createSelectedColumns([]string{"id"}, true)}},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "a", Namespace: "ns1", Schema: newSchema(), DestinationDatabase: "pg:ns1"}},
					{Stream: &Stream{Name: "b", Namespace: "ns2", Schema: newSchema(), DestinationDatabase: "pg:ns2"}},
					{Stream: &Stream{Name: "c", Namespace: "ns3", Schema: newSchema(), DestinationDatabase: "pg:ns3"}},
				},
				// stream c is NOT added to selected_streams -- user must opt in explicitly.
				// a and b carry the OLD metadata forward (not the new catalog's metadata).
				SelectedStreams: map[string][]StreamMetadata{
					"ns1": {{StreamName: "a", DestinationDatabase: "pg:ns1"}},
					"ns2": {{StreamName: "b", DestinationDatabase: "pg:ns2"}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := mergeCatalogs(tc.oldCatalog, tc.newCatalog)
			compareCatalogs(t, tc.expected, result, tc.name)
		})
	}
}

func TestCatalogGetDestDBPrefix(t *testing.T) {
	testCases := []struct {
		name          string
		streams       []*ConfiguredStream
		expectedConst bool
		expectedPref  string
	}{
		{
			name:          "empty streams slice",
			streams:       []*ConfiguredStream{},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name:          "nil streams slice",
			streams:       nil,
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "single stream - simple constant database",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "analytics"}},
			},
			expectedConst: true,
			expectedPref:  "analytics",
		},
		{
			name: "single stream - simple prefix with table",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:table_name"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "single stream - empty database name",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: ""}},
			},
			expectedConst: true,
			expectedPref:  "",
		},
		{
			name: "single stream - only colon",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: ":"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "single stream - colon at end",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "single stream - colon at end",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: ":suffix"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "single stream - multiple colons",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:schema:table"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "multiple streams - same constant database",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "analytics"}},
				{Stream: &Stream{DestinationDatabase: "analytics"}},
				{Stream: &Stream{DestinationDatabase: "analytics"}},
			},
			expectedConst: true,
			expectedPref:  "analytics",
		},
		{
			name: "multiple streams - same empty constant",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: ""}},
				{Stream: &Stream{DestinationDatabase: ""}},
			},
			expectedConst: true,
			expectedPref:  "",
		},
		{
			name: "multiple streams - same prefix different tables",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table2"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table3"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "multiple streams - same prefix with complex suffixes",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:schema.table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix:schema#table2"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "multiple streams - same prefix with empty suffix",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "multiple streams - different constants",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "analytics"}},
				{Stream: &Stream{DestinationDatabase: "warehouse"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "multiple streams - different prefixes",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix1:table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix2:table2"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "multiple streams - mix of prefix and constant",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:table1"}},
				{Stream: &Stream{DestinationDatabase: "analytics"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "multiple streams - constant vs empty string",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "analytics"}},
				{Stream: &Stream{DestinationDatabase: ""}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "multiple streams - all empty databases",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: ""}},
				{Stream: &Stream{DestinationDatabase: ""}},
				{Stream: &Stream{DestinationDatabase: ""}},
			},
			expectedConst: true,
			expectedPref:  "",
		},
		{
			name: "multiple streams - same prefix with multiple colons",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:schema:table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix:schema:table2"}},
			},
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "multiple streams - whitespace in names",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "my prefix:table1"}},
				{Stream: &Stream{DestinationDatabase: "my prefix:table2"}},
			},
			expectedConst: false,
			expectedPref:  "my prefix",
		},
		{
			name: "multiple streams - special characters in prefix",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix#123-test:table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix#123-test:table2"}},
			},
			expectedConst: false,
			expectedPref:  "prefix#123-test",
		},
		{
			name: "multiple streams - unicode characters",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "préfix:table1"}},
				{Stream: &Stream{DestinationDatabase: "préfix:table2"}},
			},
			expectedConst: false,
			expectedPref:  "préfix",
		},
		{
			name: "multiple streams - only colons",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: ":"}},
				{Stream: &Stream{DestinationDatabase: ":"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "many streams - same constant",
			streams: func() []*ConfiguredStream {
				streams := make([]*ConfiguredStream, 100)
				for i := range streams {
					streams[i] = &ConfiguredStream{
						Stream: &Stream{DestinationDatabase: "constant_db"},
					}
				}
				return streams
			}(),
			expectedConst: true,
			expectedPref:  "constant_db",
		},
		{
			name: "many streams - same prefix",
			streams: func() []*ConfiguredStream {
				streams := make([]*ConfiguredStream, 100)
				for i := range streams {
					streams[i] = &ConfiguredStream{
						Stream: &Stream{DestinationDatabase: fmt.Sprintf("prefix:table%d", i)},
					}
				}
				return streams
			}(),
			expectedConst: false,
			expectedPref:  "prefix",
		},
		{
			name: "many streams - first different",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "different:table"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table2"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table3"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
		{
			name: "many streams - last different breaks pattern",
			streams: []*ConfiguredStream{
				{Stream: &Stream{DestinationDatabase: "prefix:table1"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table2"}},
				{Stream: &Stream{DestinationDatabase: "prefix:table3"}},
				{Stream: &Stream{DestinationDatabase: "different:table"}},
			},
			expectedConst: false,
			expectedPref:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			constantValue, prefix := getDestDBPrefix(tc.streams)
			assert.Equal(t, tc.expectedConst, constantValue, "Constant value flag should match")
			assert.Equal(t, tc.expectedPref, prefix, "Prefix should match")
		})
	}
}

// validateBasicSchemas checks if two schemas have the same properties
func validateBasicSchemas(t *testing.T, expected, actual *TypeSchema, testName string) {
	if expected == nil && actual == nil {
		return
	}

	if expected == nil || actual == nil {
		t.Errorf("%s: One schema is nil - expected: %v, actual: %v", testName, expected, actual)
		return
	}

	expectedProps := make(map[string]*Property)
	expected.Properties.Range(func(key, value interface{}) bool {
		expectedProps[key.(string)] = value.(*Property)
		return true
	})

	actualProps := make(map[string]*Property)
	actual.Properties.Range(func(key, value interface{}) bool {
		actualProps[key.(string)] = value.(*Property)
		return true
	})

	if len(expectedProps) != len(actualProps) {
		t.Errorf("%s: Schema property count mismatch - expected: %d, actual: %d", testName, len(expectedProps), len(actualProps))
		return
	}

	for key, expectedProp := range expectedProps {
		actualProp, exists := actualProps[key]
		if !exists {
			t.Errorf("%s: Property %s missing in actual schema", testName, key)
			continue
		}

		if expectedProp.DestinationColumnName != actualProp.DestinationColumnName {
			t.Errorf("%s: Property %s destination column mismatch - expected: %s, actual: %s",
				testName, key, expectedProp.DestinationColumnName, actualProp.DestinationColumnName)
		}

		if expectedProp.Type.Len() != actualProp.Type.Len() {
			t.Errorf("%s: Property %s type count mismatch - expected: %d, actual: %d",
				testName, key, expectedProp.Type.Len(), actualProp.Type.Len())
		}
	}
}

func TestGetStreamsDelta(t *testing.T) {
	t.Run("identical catalogs produce empty delta", func(t *testing.T) {
		cat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, DestinationDatabase: "db:public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: boolPtr(true), SyncMode: INCREMENTAL}},
			},
		}
		delta := GetStreamsDelta(cat, cat)
		assert.Empty(t, delta.Streams)
		assert.Empty(t, delta.SelectedStreams)
	})

	t.Run("new stream not in old catalog added to delta", func(t *testing.T) {
		old := &Catalog{
			Streams:         []*ConfiguredStream{},
			SelectedStreams: map[string][]StreamMetadata{},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: boolPtr(true)}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
		assert.Equal(t, "users", delta.Streams[0].Stream.Name)
	})

	t.Run("sync mode change detected -- metadata priority over stream", func(t *testing.T) {
		// old: metadata.SyncMode = cdc (overrides stream.SyncMode = full_refresh)
		// new: metadata.SyncMode = incremental
		// effective old = cdc, effective new = incremental → different
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: SyncMode("full_refresh")}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: SyncMode("cdc")}},
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: SyncMode("full_refresh")}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: SyncMode("incremental")}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
	})

	t.Run("old dest DB preserved in delta output (metadata priority)", func(t *testing.T) {
		// even when new metadata has a different dest DB, delta output uses OLD value
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: SyncMode("cdc")}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: SyncMode("cdc"), DestinationDatabase: "old_db"}},
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: SyncMode("incremental")}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: SyncMode("incremental"), DestinationDatabase: "new_db"}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
		require.Len(t, delta.SelectedStreams["public"], 1)
		assert.Equal(t, "old_db", delta.Streams[0].Stream.DestinationDatabase)
		assert.Equal(t, "old_db", delta.SelectedStreams["public"][0].DestinationDatabase)
	})

	t.Run("old format: dest DB on stream (not metadata) -- fallback used for comparison and delta output", func(t *testing.T) {
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: SyncMode("cdc"), DestinationDatabase: "old_db"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users"}}, // no SyncMode/DestDB in metadata
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: SyncMode("incremental"), DestinationDatabase: "new_db"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users"}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
		// delta output uses old Stream.DestDB as the preserved value
		assert.Equal(t, "old_db", delta.Streams[0].Stream.DestinationDatabase)
	})

	t.Run("normalization change (*bool) detected", func(t *testing.T) {
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: boolPtr(true)}},
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: boolPtr(false)}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
	})

	t.Run("nil normalization vs explicit false is not a delta when DSP default is false", func(t *testing.T) {
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: nil}},
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: boolPtr(false)}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		assert.Empty(t, delta.Streams)
	})

	t.Run("nil normalization vs explicit false is a delta when DSP default is true", func(t *testing.T) {
		dsp := &DefaultStreamProperties{Normalization: true}
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", DefaultStreamProperties: dsp}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: nil}},
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", DefaultStreamProperties: dsp}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", Normalization: boolPtr(false)}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
	})

	t.Run("append mode change (*bool) detected", func(t *testing.T) {
		old := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", AppendMode: boolPtr(false)}},
			},
		}
		newCat := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", AppendMode: boolPtr(true)}},
			},
		}
		delta := GetStreamsDelta(old, newCat)
		require.Len(t, delta.Streams, 1)
	})

	t.Run("writing the effective value onto selected_streams is not a delta", func(t *testing.T) {
		// omitted selected_streams field falls back to streams[] / DSP; repeating that
		// same value on selected_streams must not produce a difference.
		cases := []struct {
			name string
			old  *Catalog
			new  *Catalog
		}{
			{
				name: "sync_mode from streams[]",
				old: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
				},
				new: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: CDC}}},
				},
			},
			{
				name: "cursor_field from streams[]",
				old: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
				},
				new: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", CursorField: "updated_at"}}},
				},
			},
			{
				name: "destination_database from streams[]",
				old: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "old_db"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
				},
				new: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "old_db"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationDatabase: "old_db"}}},
				},
			},
			{
				name: "destination_table from streams[]",
				old: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "users_dest"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
				},
				new: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "users_dest"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationTable: "users_dest"}}},
				},
			},
			{
				name: "append_mode DSP false",
				old: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
				},
				new: &Catalog{
					Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
					SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: boolPtr(false)}}},
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				delta := GetStreamsDelta(tc.old, tc.new)
				assert.Empty(t, delta.Streams)
			})
		}
	})
}

func writeCatalogFile(t *testing.T, dir, name string, catalog *Catalog) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data, err := json.Marshal(catalog)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	return path
}

func TestResolveCatalog(t *testing.T) {
	dir := t.TempDir()

	// combined file: both streams[] and selected_streams
	combinedCatalog := &Catalog{
		Streams: []*ConfiguredStream{
			{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema()}},
		},
		SelectedStreams: map[string][]StreamMetadata{
			"public": {{StreamName: "users", SyncMode: INCREMENTAL, Normalization: boolPtr(true)}},
		},
	}
	combinedPath := writeCatalogFile(t, dir, "combined.json", combinedCatalog)

	// streams-only file: has streams[] but no selected_streams
	streamsOnlyPath := writeCatalogFile(t, dir, "streams_only.json", &Catalog{
		Streams: []*ConfiguredStream{
			{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema()}},
		},
	})

	// selected-streams file: only selected_streams (no streams[])
	selectedOnlyPath := writeCatalogFile(t, dir, "selected_only.json", &Catalog{
		SelectedStreams: map[string][]StreamMetadata{
			"public": {{
				StreamName:      "users",
				SyncMode:        INCREMENTAL,
				Normalization:   boolPtr(true),
				SelectedColumns: createSelectedColumns([]string{"id", "name"}, true),
			}},
		},
	})

	t.Run("combined file without selectedStreamsPath works", func(t *testing.T) {
		resolved, err := ResolveCatalog(combinedPath, "")
		require.NoError(t, err)
		require.Len(t, resolved.Streams, 1)
		assert.Equal(t, "users", resolved.Streams[0].Stream.Name)
		assert.Equal(t, INCREMENTAL, resolved.SelectedStreams["public"][0].SyncMode)
	})

	t.Run("split layout: streams from streamsFile, selected_streams from selectedStreamsFile", func(t *testing.T) {
		resolved, err := ResolveCatalog(streamsOnlyPath, selectedOnlyPath)
		require.NoError(t, err)
		require.Len(t, resolved.Streams, 1)
		assert.Equal(t, "users", resolved.Streams[0].Stream.Name)
		require.Len(t, resolved.SelectedStreams["public"], 1)
		assert.Equal(t, INCREMENTAL, resolved.SelectedStreams["public"][0].SyncMode)
		assert.Equal(t, []string{"id", "name"}, resolved.SelectedStreams["public"][0].SelectedColumns.Columns)
	})

	t.Run("split layout: selectedStreamsFile overlays combined streamsFile selected_streams", func(t *testing.T) {
		// even if combinedPath already has selected_streams, the selectedStreamsFile replaces it
		resolved, err := ResolveCatalog(combinedPath, selectedOnlyPath)
		require.NoError(t, err)
		// streams[] from combined
		require.Len(t, resolved.Streams, 1)
		// selected_streams from selectedOnlyPath (not from combinedPath)
		require.Len(t, resolved.SelectedStreams["public"], 1)
		assert.NotNil(t, resolved.SelectedStreams["public"][0].SelectedColumns)
		assert.Equal(t, []string{"id", "name"}, resolved.SelectedStreams["public"][0].SelectedColumns.Columns)
	})

	t.Run("empty file loads as empty catalog", func(t *testing.T) {
		emptyPath := writeCatalogFile(t, dir, "empty.json", &Catalog{})
		resolved, err := ResolveCatalog(emptyPath, "")
		require.NoError(t, err)
		assert.Empty(t, resolved.Streams)
		assert.Empty(t, resolved.SelectedStreams)
	})

	t.Run("streams file with only selected_streams returns error", func(t *testing.T) {
		_, err := ResolveCatalog(selectedOnlyPath, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no streams[]")
	})

	t.Run("selectedStreamsFile missing returns error", func(t *testing.T) {
		_, err := ResolveCatalog(streamsOnlyPath, filepath.Join(dir, "no-such-file.json"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read selected_streams")
	})

	t.Run("selectedStreamsFile with empty selected_streams returns error", func(t *testing.T) {
		emptySelectedPath := writeCatalogFile(t, dir, "empty_selected.json", &Catalog{})
		_, err := ResolveCatalog(streamsOnlyPath, emptySelectedPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no selected_streams")
	})

	t.Run("missing streamsFile returns error", func(t *testing.T) {
		_, err := ResolveCatalog(filepath.Join(dir, "no-such-streams.json"), "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read streams")
	})
}

func TestLogCatalog(t *testing.T) {
	t.Run("combined write when SelectedStreamsPath not set", func(t *testing.T) {
		dir := t.TempDir()
		streamsPath := filepath.Join(dir, "streams.json")

		viper.Set(constants.StreamsPath, streamsPath)
		viper.Set(constants.SelectedStreamsPath, "")
		t.Cleanup(func() {
			viper.Set(constants.StreamsPath, "")
			viper.Set(constants.SelectedStreamsPath, "")
		})

		discovered := []*Stream{
			{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at"},
		}

		LogCatalog(discovered, nil, "postgres")

		data, err := os.ReadFile(streamsPath)
		require.NoError(t, err)
		var combined Catalog
		require.NoError(t, json.Unmarshal(data, &combined))

		// streams[] present
		require.Len(t, combined.Streams, 1)
		assert.Equal(t, "users", combined.Streams[0].Stream.Name)
		assert.Equal(t, []string{"id", "name"}, combined.Streams[0].Stream.SelectableColumns)

		// selected_streams present (lean: only StreamName + CursorField)
		require.Len(t, combined.SelectedStreams["public"], 1)
		sm := combined.SelectedStreams["public"][0]
		assert.Equal(t, "users", sm.StreamName)
		assert.Equal(t, "updated_at", sm.CursorField)
		assert.Nil(t, sm.Normalization) // NOT written in lean discover
		assert.Nil(t, sm.AppendMode)    // NOT written in lean discover

		// preview file written alongside streams.json
		previewPath := filepath.Join(dir, "selected_streams.json")
		previewData, err := os.ReadFile(previewPath)
		require.NoError(t, err)
		var preview Catalog
		require.NoError(t, json.Unmarshal(previewData, &preview))
		assert.Empty(t, preview.Streams)
		require.Len(t, preview.SelectedStreams["public"], 1)
		assert.Equal(t, "users", preview.SelectedStreams["public"][0].StreamName)
	})

	t.Run("split write when SelectedStreamsPath is set", func(t *testing.T) {
		dir := t.TempDir()
		streamsPath := filepath.Join(dir, "streams.json")
		selectedStreamsPath := filepath.Join(dir, "selected_streams.json")

		viper.Set(constants.StreamsPath, streamsPath)
		viper.Set(constants.SelectedStreamsPath, selectedStreamsPath)
		t.Cleanup(func() {
			viper.Set(constants.StreamsPath, "")
			viper.Set(constants.SelectedStreamsPath, "")
		})

		discovered := []*Stream{
			{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at"},
			{Name: "orders", Namespace: "public", Schema: oldSchema(), SyncMode: CDC},
		}

		LogCatalog(discovered, nil, "postgres")

		// streams file must have streams[] and NO selected_streams
		streamsData, err := os.ReadFile(streamsPath)
		require.NoError(t, err)
		var streamsFile Catalog
		require.NoError(t, json.Unmarshal(streamsData, &streamsFile))
		require.Len(t, streamsFile.Streams, 2)
		assert.Empty(t, streamsFile.SelectedStreams)

		// selected_streams file must have selected_streams and NO streams[]
		selectedData, err := os.ReadFile(selectedStreamsPath)
		require.NoError(t, err)
		var selectedFile Catalog
		require.NoError(t, json.Unmarshal(selectedData, &selectedFile))
		assert.Empty(t, selectedFile.Streams)
		require.Len(t, selectedFile.SelectedStreams["public"], 2)
		// incremental stream has cursor; cdc does not
		names := map[string]string{}
		for _, sm := range selectedFile.SelectedStreams["public"] {
			names[sm.StreamName] = sm.CursorField
		}
		assert.Equal(t, "updated_at", names["users"])
		assert.Equal(t, "", names["orders"])
	})

	t.Run("merge preserves old selected_streams config on combined write", func(t *testing.T) {
		dir := t.TempDir()
		streamsPath := filepath.Join(dir, "streams.json")

		viper.Set(constants.StreamsPath, streamsPath)
		viper.Set(constants.SelectedStreamsPath, "")
		t.Cleanup(func() {
			viper.Set(constants.StreamsPath, "")
			viper.Set(constants.SelectedStreamsPath, "")
		})

		oldCatalog := &Catalog{
			Streams: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
			},
			SelectedStreams: map[string][]StreamMetadata{
				"public": {{
					StreamName:      "users",
					SyncMode:        INCREMENTAL,
					CursorField:     "updated_at",
					Normalization:   boolPtr(true),
					SelectedColumns: createSelectedColumns([]string{"id"}, true),
				}},
			},
		}

		newDiscovered := []*Stream{
			{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: CDC},
			{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH},
		}

		LogCatalog(newDiscovered, oldCatalog, "postgres")

		data, err := os.ReadFile(streamsPath)
		require.NoError(t, err)
		var merged Catalog
		require.NoError(t, json.Unmarshal(data, &merged))

		require.Len(t, merged.Streams, 2)

		var usersStream *Stream
		for _, cs := range merged.Streams {
			if cs.Stream.Name == "users" {
				usersStream = cs.Stream
				break
			}
		}
		require.NotNil(t, usersStream)
		assert.Equal(t, []string{"email", "id"}, usersStream.SelectableColumns)

		// old selected_streams preserved (users) -- orders is new and not auto-selected
		require.Len(t, merged.SelectedStreams["public"], 1)
		sm := merged.SelectedStreams["public"][0]
		assert.Equal(t, "users", sm.StreamName)
		// metadata from old selected_streams carried forward
		assert.Equal(t, INCREMENTAL, sm.SyncMode)
		assert.Equal(t, "updated_at", sm.CursorField)
		assert.Equal(t, boolPtr(true), sm.Normalization)
	})
}
