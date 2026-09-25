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
	"github.com/datazip-inc/olake/utils/errs"
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

func streamIDs(streams []*ConfiguredStream) []string {
	ids := make([]string, 0, len(streams))
	for _, s := range streams {
		if s != nil && s.Stream != nil {
			ids = append(ids, s.Stream.ID())
		}
	}
	return ids
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
							SyncMode:       SyncMode("incremental"),
							CursorField:    "updated_at",
						},
						{
							StreamName:     "orders",
							PartitionRegex: "",
							SyncMode:       SyncMode("cdc"),
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
						{StreamName: "stream1", PartitionRegex: "test_regex", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id"}, false)},
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
						{StreamName: "stream1", PartitionRegex: "test_regex", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id"}, false)},
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
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
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
						{StreamName: "stream1", PartitionRegex: "new_partition", Filter: "new_filter <= 8", AppendMode: new(false), Normalization: new(false), SelectedColumns: createSelectedColumns([]string{"id", "email", "created_at"}, false)},
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
							AppendMode:      new(true),
							Normalization:   new(true),
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
						{StreamName: "stream1", PartitionRegex: "old_partition", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
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
						{StreamName: "stream1", PartitionRegex: "new_partition", Filter: "new_filter <= 8", AppendMode: new(false), Normalization: new(false), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "", Filter: "new_filter <= 8", AppendMode: new(false), Normalization: new(false), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
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
							AppendMode:      new(true),
							Normalization:   new(true),
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
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
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
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "new_filter <= 8", AppendMode: new(false), Normalization: new(false), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
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
							AppendMode:      new(true),
							Normalization:   new(true),
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
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
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
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: new(true), Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "another_partition", Filter: "new_filter <= 8", AppendMode: new(false), Normalization: new(false), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
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
							Normalization:   new(true),
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
						{StreamName: "users", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "name"}, false)},
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
						{StreamName: "users", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id", "email"}, false)},
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
							Normalization:   new(true),
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
							Normalization:       new(true),
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
							Normalization:       new(true),
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
					"ns1": {{StreamName: "a", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id"}, true)}},
					"ns2": {{StreamName: "b", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id"}, true)}},
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

// selectedStreamNames returns the stream names under each selected_streams namespace, in order.
func selectedStreamNames(selectedStreams map[string][]StreamMetadata) map[string][]string {
	names := make(map[string][]string, len(selectedStreams))
	for namespace, metadataList := range selectedStreams {
		for _, metadata := range metadataList {
			names[namespace] = append(names[namespace], metadata.StreamName)
		}
	}
	return names
}

func TestCatalogWriteToFile(t *testing.T) {
	testCases := []struct {
		name    string
		catalog *Catalog
		// file contents: stream IDs and selected stream names, in order
		expectedStreams  []string
		expectedSelected map[string][]string
	}{
		{
			name: "streams sorted by namespace then name",
			catalog: &Catalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "users", Namespace: "sales"}},
					{Stream: &Stream{Name: "orders", Namespace: "public"}},
					{Stream: &Stream{Name: "accounts", Namespace: "sales"}},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"sales":  {{StreamName: "users"}, {StreamName: "accounts"}},
					"public": {{StreamName: "orders"}},
				},
			},
			expectedStreams:  []string{"public.orders", "sales.accounts", "sales.users"},
			expectedSelected: map[string][]string{"public": {"orders"}, "sales": {"accounts", "users"}},
		},
		{
			name: "streams in one namespace sorted by name",
			catalog: &Catalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "zebra", Namespace: "public"}},
					{Stream: &Stream{Name: "orders", Namespace: "public"}},
					{Stream: &Stream{Name: "accounts", Namespace: "public"}},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {{StreamName: "zebra"}, {StreamName: "accounts"}, {StreamName: "orders"}},
				},
			},
			expectedStreams:  []string{"public.accounts", "public.orders", "public.zebra"},
			expectedSelected: map[string][]string{"public": {"accounts", "orders", "zebra"}},
		},
		{
			name:    "empty catalog",
			catalog: &Catalog{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.json")
			require.NoError(t, tc.catalog.WriteToFile(path))

			var written Catalog
			readCatalogFile(t, path, &written)
			if len(tc.expectedStreams) == 0 {
				assert.Empty(t, written.Streams)
				assert.Empty(t, written.SelectedStreams)
				return
			}
			assert.Equal(t, tc.expectedStreams, streamIDs(written.Streams))
			assert.Equal(t, tc.expectedSelected, selectedStreamNames(written.SelectedStreams))
		})
	}
}

func TestGetStreamsDelta(t *testing.T) {
	testCases := []struct {
		name       string
		oldCatalog *Catalog
		newCatalog *Catalog
		// nil means no delta. A changed stream is carried with the new values, except the
		// destination, which keeps the old one so clear-destination drops the table holding the data.
		expectedDelta *Catalog
	}{
		{
			name: "identical catalogs produce empty delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, DestinationDatabase: "db:public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(true), SyncMode: INCREMENTAL}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, DestinationDatabase: "db:public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(true), SyncMode: INCREMENTAL}}},
			},
		},
		{
			name:       "new stream not in old catalog added to delta",
			oldCatalog: &Catalog{Streams: []*ConfiguredStream{}, SelectedStreams: map[string][]StreamMetadata{}},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "db:public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(true)}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "db:public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(true)}}},
			},
		},
		{
			// effective old sync mode = cdc (selected_streams over streams[]), effective new = incremental
			name: "sync mode change detected -- selected_streams priority over streams[]",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: FULLREFRESH}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: CDC}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: FULLREFRESH}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: INCREMENTAL}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: FULLREFRESH}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: INCREMENTAL}}},
			},
		},
		{
			name: "old destination database from selected_streams kept in delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: CDC, DestinationDatabase: "old_db"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: INCREMENTAL, DestinationDatabase: "new_db"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, DestinationDatabase: "old_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: INCREMENTAL, DestinationDatabase: "old_db"}}},
			},
		},
		{
			name: "old destination database from streams[] kept in delta (legacy catalog)",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC, DestinationDatabase: "old_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, DestinationDatabase: "new_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, DestinationDatabase: "old_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationDatabase: "old_db"}}},
			},
		},
		{
			name: "normalization change detected",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(true)}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(false)}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(false)}}},
			},
		},
		{
			name: "unset normalization vs false is not a delta when the default is false",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(false)}}},
			},
		},
		{
			name: "unset normalization vs false is a delta when the default is true",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DefaultStreamProperties: &DefaultStreamProperties{Normalization: true}}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DefaultStreamProperties: &DefaultStreamProperties{Normalization: true}}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(false)}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(false)}}},
			},
		},
		{
			name: "append mode change detected",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: new(false)}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: new(true)}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: new(true)}}},
			},
		},
		{
			name: "unset append_mode vs false is a delta when the default is true",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DefaultStreamProperties: &DefaultStreamProperties{AppendMode: true}}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DefaultStreamProperties: &DefaultStreamProperties{AppendMode: true}}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: new(false)}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: new(false)}}},
			},
		},
		{
			name: "cursor_field change on an incremental stream is a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", CursorField: "created_at"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", CursorField: "created_at"}}},
			},
		},
		{
			// the cursor is only used by incremental sync
			name: "cursor_field change on a cdc stream is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", CursorField: "created_at"}}},
			},
		},
		{
			name: "destination_table change is a delta and keeps the old table",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "users"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationTable: "custom_users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "users"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationTable: "users_v2"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "custom_users"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationTable: "custom_users"}}},
			},
		},
		{
			// old value on streams[], new value on selected_streams
			name: "adding a different sync_mode on selected_streams is a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: INCREMENTAL}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: INCREMENTAL}}},
			},
		},
		{
			// the override is removed, so the destination falls back to streams[]: old_db -> discovered_db
			name: "removing the destination_database override from selected_streams is a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "discovered_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationDatabase: "old_db"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "discovered_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "old_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationDatabase: "old_db"}}},
			},
		},
		{
			// the override equals the streams[] value, so the destination does not change
			name: "removing a destination_database override equal to streams[] is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "discovered_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationDatabase: "discovered_db"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "discovered_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
		},
		// an unset selected_streams field falls back to streams[] or the default stream properties;
		// setting that same value on selected_streams is not a delta
		{
			name: "sync_mode from streams[] repeated on selected_streams is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", SyncMode: CDC}}},
			},
		},
		{
			name: "cursor_field from streams[] repeated on selected_streams is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", SyncMode: INCREMENTAL, CursorField: "updated_at"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", CursorField: "updated_at"}}},
			},
		},
		{
			name: "destination_database from streams[] repeated on selected_streams is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "old_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationDatabase: "old_db"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationDatabase: "old_db"}}},
			},
		},
		{
			name: "destination_table from streams[] repeated on selected_streams is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "users_dest"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", DestinationTable: "users_dest"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", DestinationTable: "users_dest"}}},
			},
		},
		{
			name: "default append_mode repeated on selected_streams is not a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", AppendMode: new(false)}}},
			},
		},
		// fields that exist only on selected_streams
		{
			name: "partition_regex change is a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", PartitionRegex: "/{created_at,day}"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", PartitionRegex: "/{created_at,month}"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", PartitionRegex: "/{created_at,month}"}}},
			},
		},
		{
			name: "filter change is a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Filter: "id > 10"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Filter: "id > 20"}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Filter: "id > 20"}}},
			},
		},
		{
			name: "filter_config change is a delta",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", FilterConfig: &FilterConfig{
					LogicalOperator: "and", Conditions: []FilterCondition{{Column: "id", Operator: ">", Value: 10}},
				}}}},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", FilterConfig: &FilterConfig{
					LogicalOperator: "and", Conditions: []FilterCondition{{Column: "id", Operator: ">", Value: 20}},
				}}}},
			},
			expectedDelta: &Catalog{
				Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", FilterConfig: &FilterConfig{
					LogicalOperator: "and", Conditions: []FilterCondition{{Column: "id", Operator: ">", Value: 20}},
				}}}},
			},
		},
		{
			name: "use_source_column_names change is a delta",
			oldCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users"}}},
			},
			newCatalog: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", UseSourceColumnNames: true}}},
			},
			expectedDelta: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public"}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", UseSourceColumnNames: true}}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			delta := GetStreamsDelta(tc.oldCatalog, tc.newCatalog)
			if tc.expectedDelta == nil {
				assert.Empty(t, delta.Streams)
				assert.Empty(t, delta.SelectedStreams)
				return
			}
			// GetStreamsDelta walks a map, so its order is not fixed
			delta.sortByNamespaceStreamName()
			compareCatalogs(t, tc.expectedDelta, delta, tc.name)
		})
	}
}

func writeCatalogFile(t *testing.T, dir, name string, catalog *Catalog) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data, err := json.Marshal(catalog)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	return path
}

func writeLegacyCatalogFile(t *testing.T, dir, name string, catalog *LegacyCatalog) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data, err := json.Marshal(catalog)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	return path
}

func TestResolveCatalog(t *testing.T) {
	testCases := []struct {
		name string
		// files to write; a nil file is not written. available and selected are passed together.
		legacy    *LegacyCatalog
		available *Catalog
		selected  *Catalog
		// expected catalog, or the expected error
		expected     *Catalog
		expectedCode string
		expectedErr  string
	}{
		{
			name: "legacy: streams.json resolves with legacy values made explicit",
			legacy: &LegacyCatalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: CDC}}},
				SelectedStreams: map[string][]LegacyStreamMetadata{"public": {{StreamName: "users", Normalization: true, AppendMode: false}}},
			},
			expected: &Catalog{
				Streams:         []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: CDC}}},
				SelectedStreams: map[string][]StreamMetadata{"public": {{StreamName: "users", Normalization: new(true), AppendMode: new(false)}}},
			},
		},
		{
			name:      "new format: available_streams + selected_streams resolve together",
			available: &Catalog{Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema()}}}},
			selected: &Catalog{SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: INCREMENTAL, SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)}},
			}},
			expected: &Catalog{
				Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema()}}},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {{StreamName: "users", SyncMode: INCREMENTAL, SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)}},
				},
			},
		},
		{
			name:     "empty legacy file loads as empty catalog",
			legacy:   &LegacyCatalog{},
			expected: &Catalog{},
		},
		{
			name: "legacy streams[] without selected_streams returns error",
			legacy: &LegacyCatalog{
				Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema()}}},
			},
			expectedCode: codeLegacySelectedStreamsEmpty,
			expectedErr:  "no selected_streams",
		},
		{
			name: "legacy selected_streams without streams[] returns error",
			legacy: &LegacyCatalog{
				SelectedStreams: map[string][]LegacyStreamMetadata{"public": {{StreamName: "users"}}},
			},
			expectedCode: codeLegacyStreamsMissing,
			expectedErr:  "no streams[]",
		},
		{
			name:        "missing legacy file returns error",
			expectedErr: "failed to read streams",
		},
		{
			name:      "available_streams file with no streams[] returns error",
			available: &Catalog{},
			selected: &Catalog{SelectedStreams: map[string][]StreamMetadata{
				"public": {{StreamName: "users"}},
			}},
			expectedCode: codeAvailableStreamsEmpty,
		},
		{
			name:         "selected_streams file with no selected_streams returns error",
			available:    &Catalog{Streams: []*ConfiguredStream{{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema()}}}},
			selected:     &Catalog{},
			expectedCode: codeSelectedStreamsEmpty,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			streamsPath, availablePath, selectedPath := filepath.Join(dir, "streams.json"), "", ""
			if tc.legacy != nil {
				streamsPath = writeLegacyCatalogFile(t, dir, "streams.json", tc.legacy)
			}
			if tc.available != nil {
				availablePath = writeCatalogFile(t, dir, "available_streams.json", tc.available)
				selectedPath = writeCatalogFile(t, dir, "selected_streams.json", tc.selected)
			}

			resolved, err := ResolveCatalog(streamsPath, availablePath, selectedPath)
			if tc.expectedCode != "" || tc.expectedErr != "" {
				require.Error(t, err)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
				if tc.expectedCode != "" {
					got := errs.From(errs.Classify(err))
					assert.Equal(t, errs.CatalogError, got.Category)
					assert.Equal(t, tc.expectedCode, got.Code)
				}
				return
			}
			require.NoError(t, err)
			if len(tc.expected.Streams) == 0 && len(tc.expected.SelectedStreams) == 0 {
				assert.Empty(t, resolved.Streams)
				assert.Empty(t, resolved.SelectedStreams)
				return
			}
			compareCatalogs(t, tc.expected, resolved, tc.name)
		})
	}
}

// setLogCatalogPaths points viper at fresh streams.json / available_streams.json /
// selected_streams.json paths in dir and registers cleanup.
func setLogCatalogPaths(t *testing.T, dir string) (streamsPath, availablePath, selectedPath string) {
	t.Helper()
	streamsPath = filepath.Join(dir, "streams.json")
	availablePath = filepath.Join(dir, "available_streams.json")
	selectedPath = filepath.Join(dir, "selected_streams.json")
	viper.Set(constants.StreamsPath, streamsPath)
	viper.Set(constants.AvailableStreamsPath, availablePath)
	viper.Set(constants.SelectedStreamsPath, selectedPath)
	t.Cleanup(func() {
		viper.Set(constants.StreamsPath, "")
		viper.Set(constants.AvailableStreamsPath, "")
		viper.Set(constants.SelectedStreamsPath, "")
	})
	return
}

// readCatalogFile unmarshals a catalog file written by LogCatalog into out.
func readCatalogFile(t *testing.T, path string, out any) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, out))
}

// sortLegacySelectedColumns sorts each legacy selected_columns list, so the comparison does not
// depend on schema iteration order.
func sortLegacySelectedColumns(selectedStreams map[string][]LegacyStreamMetadata) {
	for _, metadataList := range selectedStreams {
		for i := range metadataList {
			if metadataList[i].SelectedColumns != nil {
				sort.Strings(metadataList[i].SelectedColumns.Columns)
			}
		}
	}
}

func TestLogCatalog(t *testing.T) {
	defaults := &DefaultStreamProperties{Normalization: true, UpdateType: UpdateTypeEquality}

	testCases := []struct {
		name             string
		discovered       []*Stream
		oldCatalog       *Catalog
		oldLegacyCatalog *LegacyCatalog
		// expected file contents; streams[] are compared in order
		expectedAvailable []*ConfiguredStream
		expectedSelected  map[string][]StreamMetadata
		expectedLegacy    *LegacyCatalog
		// selectable_columns per stream ID in available_streams.json; nil skips the check
		expectedSelectableColumns map[string][]string
	}{
		{
			name: "new user (no prior catalog): writes all three files",
			discovered: []*Stream{
				{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DefaultStreamProperties: defaults},
			},
			expectedAvailable: []*ConfiguredStream{
				{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at"}},
			},
			// selected_streams.json is sparse: normalization/append_mode are left to the defaults
			expectedSelected: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: INCREMENTAL, CursorField: "updated_at"}},
			},
			// streams.json spells out every field: defaults and all columns
			expectedLegacy: &LegacyCatalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at"}},
				},
				SelectedStreams: map[string][]LegacyStreamMetadata{
					"public": {{StreamName: "users", Normalization: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)}},
				},
			},
		},
		{
			name: "all three files sorted by namespace then name",
			discovered: []*Stream{
				{Name: "users", Namespace: "sales", Schema: oldSchema(), DefaultStreamProperties: defaults},
				{Name: "orders", Namespace: "public", Schema: oldSchema(), DefaultStreamProperties: defaults},
				{Name: "accounts", Namespace: "sales", Schema: oldSchema(), DefaultStreamProperties: defaults},
				{Name: "zebra", Namespace: "public", Schema: oldSchema(), DefaultStreamProperties: defaults},
			},
			expectedAvailable: []*ConfiguredStream{
				{Stream: &Stream{Name: "orders", Namespace: "public", Schema: oldSchema()}},
				{Stream: &Stream{Name: "zebra", Namespace: "public", Schema: oldSchema()}},
				{Stream: &Stream{Name: "accounts", Namespace: "sales", Schema: oldSchema()}},
				{Stream: &Stream{Name: "users", Namespace: "sales", Schema: oldSchema()}},
			},
			expectedSelected: map[string][]StreamMetadata{
				"public": {{StreamName: "orders"}, {StreamName: "zebra"}},
				"sales":  {{StreamName: "accounts"}, {StreamName: "users"}},
			},
			expectedLegacy: &LegacyCatalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "orders", Namespace: "public", Schema: oldSchema()}},
					{Stream: &Stream{Name: "zebra", Namespace: "public", Schema: oldSchema()}},
					{Stream: &Stream{Name: "accounts", Namespace: "sales", Schema: oldSchema()}},
					{Stream: &Stream{Name: "users", Namespace: "sales", Schema: oldSchema()}},
				},
				SelectedStreams: map[string][]LegacyStreamMetadata{
					"public": {
						{StreamName: "orders", Normalization: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)},
						{StreamName: "zebra", Normalization: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)},
					},
					"sales": {
						{StreamName: "accounts", Normalization: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)},
						{StreamName: "users", Normalization: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"id", "name"}, true)},
					},
				},
			},
		},
		{
			// users keeps its old streams[] fields and selection; orders is new and not auto-selected
			name: "existing new-format user: merge carries the old catalog forward",
			discovered: []*Stream{
				{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: CDC, DefaultStreamProperties: defaults},
				{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH, DefaultStreamProperties: defaults},
			},
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"public": {{StreamName: "users", SyncMode: INCREMENTAL, CursorField: "updated_at", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"id"}, true)}},
				},
			},
			expectedAvailable: []*ConfiguredStream{
				{Stream: &Stream{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH, DestinationDatabase: "analytics"}},
				{Stream: &Stream{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
			},
			// email is new in the schema and sync_new_columns is true, so it is added
			expectedSelected: map[string][]StreamMetadata{
				"public": {{StreamName: "users", SyncMode: INCREMENTAL, CursorField: "updated_at", Normalization: new(true), SelectedColumns: createSelectedColumns([]string{"email", "id"}, true)}},
			},
			expectedLegacy: &LegacyCatalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH, DestinationDatabase: "analytics"}},
					{Stream: &Stream{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
				},
				SelectedStreams: map[string][]LegacyStreamMetadata{
					"public": {{StreamName: "users", Normalization: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"email", "id"}, true)}},
				},
			},
			expectedSelectableColumns: map[string][]string{"public.users": {"email", "id"}},
		},
		{
			// no prior available/selected files, only streams.json: the upgrade path for a legacy user
			name: "existing legacy user: selected_streams.json seeded from the legacy selection",
			discovered: []*Stream{
				{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: CDC, DefaultStreamProperties: defaults},
				{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH, DefaultStreamProperties: defaults},
			},
			oldLegacyCatalog: &LegacyCatalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "users", Namespace: "public", Schema: oldSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
				},
				SelectedStreams: map[string][]LegacyStreamMetadata{
					"public": {{StreamName: "users", Normalization: false, AppendMode: true, PartitionRegex: "user_partition", SelectedColumns: createSelectedColumns([]string{"id"}, false)}},
				},
			},
			expectedAvailable: []*ConfiguredStream{
				{Stream: &Stream{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH, DestinationDatabase: "analytics"}},
				{Stream: &Stream{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
			},
			// legacy values become explicit, so the defaults cannot change them
			expectedSelected: map[string][]StreamMetadata{
				"public": {{StreamName: "users", PartitionRegex: "user_partition", Normalization: new(false), AppendMode: new(true), SelectedColumns: createSelectedColumns([]string{"id"}, false)}},
			},
			expectedLegacy: &LegacyCatalog{
				Streams: []*ConfiguredStream{
					{Stream: &Stream{Name: "orders", Namespace: "public", Schema: newSchema(), SyncMode: FULLREFRESH, DestinationDatabase: "analytics"}},
					{Stream: &Stream{Name: "users", Namespace: "public", Schema: newSchema(), SyncMode: INCREMENTAL, CursorField: "updated_at", DestinationDatabase: "analytics", DestinationTable: "users"}},
				},
				SelectedStreams: map[string][]LegacyStreamMetadata{
					"public": {{StreamName: "users", PartitionRegex: "user_partition", Normalization: false, AppendMode: true, UpdateType: string(UpdateTypeEquality), SelectedColumns: createSelectedColumns([]string{"id"}, false)}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			streamsPath, availablePath, selectedPath := setLogCatalogPaths(t, t.TempDir())

			LogCatalog(tc.discovered, tc.oldCatalog, tc.oldLegacyCatalog, "postgres")

			// available_streams.json: streams[] only
			var available Catalog
			readCatalogFile(t, availablePath, &available)
			assert.Empty(t, available.SelectedStreams)
			compareCatalogs(t, &Catalog{Streams: tc.expectedAvailable}, &Catalog{Streams: available.Streams}, tc.name)
			for _, configured := range available.Streams {
				if columns, ok := tc.expectedSelectableColumns[configured.Stream.ID()]; ok {
					assert.Equal(t, columns, configured.Stream.SelectableColumns)
				}
			}

			// selected_streams.json: selected_streams only
			var selected Catalog
			readCatalogFile(t, selectedPath, &selected)
			assert.Empty(t, selected.Streams)
			sortSelectedStreams(selected.SelectedStreams)
			assert.Equal(t, tc.expectedSelected, selected.SelectedStreams)

			// streams.json: legacy shape of the same merge
			var legacy LegacyCatalog
			readCatalogFile(t, streamsPath, &legacy)
			compareCatalogs(t, &Catalog{Streams: tc.expectedLegacy.Streams}, &Catalog{Streams: legacy.Streams}, tc.name)
			sortLegacySelectedColumns(legacy.SelectedStreams)
			assert.Equal(t, tc.expectedLegacy.SelectedStreams, legacy.SelectedStreams)
		})
	}
}
