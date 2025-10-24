package types

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
							AppendMode:     false,
							Normalization:  true,
						},
					},
				},
			},
		},
		// single stream in mongodb, should return normalization as false
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
							AppendMode:     false,
							Normalization:  false,
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
							AppendMode:     false,
							Normalization:  true,
						},
						{
							StreamName:     "orders",
							PartitionRegex: "",
							AppendMode:     false,
							Normalization:  true,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetWrappedCatalog(tc.streams, tc.driver)
			assert.Equal(t, tc.expected, result, "The generated catalog should match the expected catalog")

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
							Schema:    &TypeSchema{Properties: sync.Map{}},
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "test_regex", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
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
						{StreamName: "stream1", PartitionRegex: "test_regex", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
		},
		// when merging single stream, old catalog metadata and selected stream data should be preserved
		{
			name: "single stream merge",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                  "stream1",
							Namespace:             "namespace1",
							Schema:                oldSchema(),
							SupportedSyncModes:    NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:              SyncMode("cdc"),
							AvailableCursorFields: NewSet("updated_at", "created_at"),
							CursorField:           "updated_at",
							DestinationDatabase:   "db:namespace1",
							DestinationTable:      "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                  "stream1",
							Namespace:             "namespace1",
							Schema:                newSchema(),
							SupportedSyncModes:    NewSet(SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:              SyncMode("incremental"),
							AvailableCursorFields: NewSet("created_at"),
							CursorField:           "created_at",
							DestinationDatabase:   "db:namespace1",
							DestinationTable:      "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "new_partition", Filter: "new_filter <= 8", AppendMode: false, Normalization: false},
					},
				},
			},
			expected: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                  "stream1",
							Namespace:             "namespace1",
							Schema:                newSchema(),
							SupportedSyncModes:    NewSet(SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:              SyncMode("cdc"),
							AvailableCursorFields: NewSet("created_at"),
							CursorField:           "updated_at",
							DestinationDatabase:   "db:namespace1",
							DestinationTable:      "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
		},
		// new stream gets added with its metadata, while existing stream's configuration and selected stream data are preserved
		{
			name: "new stream introduced",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "updated_at",
							DestinationDatabase: "db:namespace1",
							DestinationTable:    "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "old_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SyncMode:            SyncMode("cdc"),
							CursorField:         "id",
							DestinationDatabase: "db:newNamespace1",
							DestinationTable:    "newStream1",
						},
					},
					{
						Stream: &Stream{
							Name:                "stream2",
							Namespace:           "namespace2",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "db:namespace2",
							DestinationTable:    "stream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "new_partition", Filter: "new_filter <= 8", AppendMode: false, Normalization: false},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "", Filter: "new_filter <= 8", AppendMode: false, Normalization: false},
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
							SyncMode:            SyncMode("incremental"),
							CursorField:         "updated_at",
							DestinationDatabase: "db:namespace1",
							DestinationTable:    "stream1",
						},
					},
					{
						Stream: &Stream{
							Name:                "stream2",
							Namespace:           "namespace2",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "db:namespace2",
							DestinationTable:    "stream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "old_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
		},
		// Removed streams are excluded from the result, but remaining streams keep their original configuration
		{
			name: "old stream removed",
			oldCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "id",
							DestinationDatabase: "db:newNamespace1",
							DestinationTable:    "newStream1",
						},
					},
					{
						Stream: &Stream{
							Name:                "stream2",
							Namespace:           "namespace2",
							Schema:              newSchema(),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "db:namespace2",
							DestinationTable:    "stream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "updated_at",
							DestinationDatabase: "db:namespace1",
							DestinationTable:    "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "new_filter <= 8", AppendMode: false, Normalization: false},
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
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "id",
							DestinationDatabase: "db:newNamespace1",
							DestinationTable:    "newStream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
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
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "id",
							DestinationDatabase: "",
							DestinationTable:    "stream1",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", Normalization: true},
					},
				},
			},
			newCatalog: &Catalog{
				Streams: []*ConfiguredStream{
					{
						Stream: &Stream{
							Name:                "stream1",
							Namespace:           "namespace1",
							Schema:              oldSchema(),
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "updated_at",
							DestinationDatabase: "db:namespace1",
							DestinationTable:    "newStream1",
						},
					},
					{
						Stream: &Stream{
							Name:                "stream2",
							Namespace:           "namespace2",
							Schema:              newSchema(),
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "db:namespace2",
							DestinationTable:    "newStream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", AppendMode: true, Normalization: true},
					},
					"namespace2": {
						{StreamName: "stream2", PartitionRegex: "another_partition", Filter: "new_filter <= 8", AppendMode: false, Normalization: false},
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
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("incremental"),
							CursorField:         "id",
							DestinationDatabase: "",
							DestinationTable:    "stream1",
						},
					},
					{
						Stream: &Stream{
							Name:                "stream2",
							Namespace:           "namespace2",
							Schema:              newSchema(),
							SupportedSyncModes:  NewSet(SyncMode("cdc"), SyncMode("incremental"), SyncMode("full_refresh")),
							SyncMode:            SyncMode("full_refresh"),
							DestinationDatabase: "",
							DestinationTable:    "newStream2",
						},
					},
				},
				SelectedStreams: map[string][]StreamMetadata{
					"namespace1": {
						{StreamName: "stream1", PartitionRegex: "user_partition", Filter: "test_filter > 10", Normalization: true},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := mergeCatalogs(tc.oldCatalog, tc.newCatalog)
			assert.Equal(t, len(tc.expected.Streams), len(result.Streams), "Stream count should match")
			assert.Equal(t, tc.expected.SelectedStreams, result.SelectedStreams, "SelectedStreams should match")

			for i, expectedStream := range tc.expected.Streams {
				actualStream := result.Streams[i]

				assert.Equal(t, expectedStream.Stream.Name, actualStream.Stream.Name, "Stream name should match")
				assert.Equal(t, expectedStream.Stream.Namespace, actualStream.Stream.Namespace, "Stream namespace should match")
				assert.Equal(t, expectedStream.Stream.SyncMode, actualStream.Stream.SyncMode, "SyncMode should match")
				assert.Equal(t, expectedStream.Stream.CursorField, actualStream.Stream.CursorField, "CursorField should match")
				assert.Equal(t, expectedStream.Stream.DestinationDatabase, actualStream.Stream.DestinationDatabase, "DestinationDatabase should match")
				assert.Equal(t, expectedStream.Stream.DestinationTable, actualStream.Stream.DestinationTable, "DestinationTable should match")

				validateBasicSchemas(t, expectedStream.Stream.Schema, actualStream.Stream.Schema, tc.name)
			}
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
