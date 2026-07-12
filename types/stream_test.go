package types

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/goccy/go-json"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestNewStream(t *testing.T) {
	tests := []struct {
		//test identifier
		testName string

		//test values
		name           string
		namespace      string
		sourceDatabase *string

		//expected result stream
		expectedStream *Stream
	}{
		{
			testName:       "stream with empty 'name' but all other fields filled in",
			name:           "",
			namespace:      "grades",
			sourceDatabase: stringPtr("gradesdb"),
			expectedStream: &Stream{
				Name:                    "",
				Namespace:               "grades",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     "gradesdb:grades",
				DestinationTable:        "",
			},
		},
		{
			testName:       "stream with empty 'namespace' but all other fields filled in",
			name:           "students",
			namespace:      "",
			sourceDatabase: stringPtr("gradesdb"),
			expectedStream: &Stream{
				Name:                    "students",
				Namespace:               "",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     "gradesdb",
				DestinationTable:        "students",
			},
		},
		{
			testName:       "stream with nil 'sourceDatabase' but all other fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: nil,
			expectedStream: &Stream{
				Name:                    "students",
				Namespace:               "grades",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     ":grades",
				DestinationTable:        "students",
			},
		},
		{
			testName:       "stream with empty 'sourceDatabase' but all other fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr(""),
			expectedStream: &Stream{
				Name:                    "students",
				Namespace:               "grades",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     ":grades",
				DestinationTable:        "students",
			},
		},
		{
			testName:       "stream with all fields empty or nil",
			name:           "",
			namespace:      "",
			sourceDatabase: nil,
			expectedStream: &Stream{
				Name:                    "",
				Namespace:               "",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     "",
				DestinationTable:        "",
			},
		},
		{
			testName:       "stream with all fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr("gradesdb"),
			expectedStream: &Stream{
				Name:                    "students",
				Namespace:               "grades",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     "gradesdb:grades",
				DestinationTable:        "students",
			},
		},
		{
			testName:       "stream with uppercase and special characters in 'name' and 'namespace'",
			name:           "User-Orders.v2",
			namespace:      "My.Schema",
			sourceDatabase: nil,
			expectedStream: &Stream{
				Name:                    "User-Orders.v2",
				Namespace:               "My.Schema",
				Schema:                  NewTypeSchema(),
				SupportedSyncModes:      NewSet[SyncMode](),
				SourceDefinedPrimaryKey: NewSet[string](),
				AvailableCursorFields:   NewSet[string](),
				DestinationDatabase:     ":my_schema",
				DestinationTable:        "user_orders_v2",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := NewStream(tc.name, tc.namespace, tc.sourceDatabase)
			assert.Equal(t, tc.expectedStream, stream)
		})
	}
}

func TestStreamID(t *testing.T) {
	tests := []struct {
		testName   string
		name       string
		namespace  string
		expectedID string
	}{
		{
			testName:   "name field empty namespace filled",
			name:       "",
			namespace:  "gradesDb",
			expectedID: "gradesDb.",
		},
		{
			testName:   "namespace field empty name and Id filled",
			name:       "students",
			namespace:  "",
			expectedID: "students",
		},
		{
			testName:   "all fields filled",
			name:       "students",
			namespace:  "gradesDb",
			expectedID: "gradesDb.students",
		},
		{
			testName:   "all fields empty",
			name:       "",
			namespace:  "",
			expectedID: "",
		},
		{
			testName:   "special characters in fields",
			name:       "Students %",
			namespace:  "gradesDb-v2",
			expectedID: "gradesDb-v2.Students %",
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := &Stream{
				Name:      tc.name,
				Namespace: tc.namespace,
			}
			assert.Equal(t, tc.expectedID, stream.ID())
		})
	}
}

func TestStreamWithSyncMode(t *testing.T) {
	tests := []struct {
		testName      string
		modes         []SyncMode
		expectedModes []SyncMode
	}{
		{
			testName:      "single mode",
			modes:         []SyncMode{FULLREFRESH},
			expectedModes: []SyncMode{FULLREFRESH},
		},
		{
			testName:      "multiple modes",
			modes:         []SyncMode{FULLREFRESH, INCREMENTAL},
			expectedModes: []SyncMode{FULLREFRESH, INCREMENTAL},
		},
		{
			testName:      "all modes",
			modes:         []SyncMode{FULLREFRESH, INCREMENTAL, CDC, STRICTCDC},
			expectedModes: []SyncMode{FULLREFRESH, INCREMENTAL, CDC, STRICTCDC},
		},
		{
			testName:      "duplicate modes",
			modes:         []SyncMode{FULLREFRESH, FULLREFRESH, INCREMENTAL},
			expectedModes: []SyncMode{FULLREFRESH, INCREMENTAL},
		},
		{
			testName:      "empty modes",
			modes:         []SyncMode{},
			expectedModes: []SyncMode{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithSyncMode(tc.modes...)

			// should return the exact same pointer
			assert.Same(t, stream, returnedStream, "should return the same instance")

			// check if the set now contains the added modes
			assert.ElementsMatch(t, tc.expectedModes, returnedStream.SupportedSyncModes.Array())
		})
	}
}

func TestStreamWithPrimaryKey(t *testing.T) {
	tests := []struct {
		testName     string
		keys         []string
		expectedKeys []string
	}{
		{
			testName:     "single key",
			keys:         []string{"id"},
			expectedKeys: []string{"id"},
		},
		{
			testName:     "composite key",
			keys:         []string{"tenant_id", "user_id", "order_id"},
			expectedKeys: []string{"tenant_id", "user_id", "order_id"},
		},
		{
			testName:     "duplicate keys",
			keys:         []string{"id", "id", "user_uuid"},
			expectedKeys: []string{"id", "user_uuid"},
		},
		{
			testName:     "empty keys",
			keys:         []string{},
			expectedKeys: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithPrimaryKey(tc.keys...)

			assert.Same(t, stream, returnedStream, "should return the same instance")

			assert.ElementsMatch(t, tc.expectedKeys, stream.SourceDefinedPrimaryKey.Array())
		})
	}
}

func TestStreamWithCursorField(t *testing.T) {
	tests := []struct {
		testName        string
		columns         []string
		expectedColumns []string
	}{
		{
			testName:        "single column",
			columns:         []string{"updated_at"},
			expectedColumns: []string{"updated_at"},
		},
		{
			testName:        "multiple columns",
			columns:         []string{"updated_at", "inserted_at"},
			expectedColumns: []string{"updated_at", "inserted_at"},
		},
		{
			testName:        "duplicate columns",
			columns:         []string{"updated_at", "updated_at", "inserted_at"},
			expectedColumns: []string{"updated_at", "inserted_at"},
		},
		{
			testName:        "empty columns",
			columns:         []string{},
			expectedColumns: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithCursorField(tc.columns...)

			assert.Same(t, stream, returnedStream, "should return the same instance")

			assert.ElementsMatch(t, tc.expectedColumns, stream.AvailableCursorFields.Array())
		})
	}
}

func TestStreamWithSchema(t *testing.T) {
	setSchema := NewTypeSchema()

	tests := []struct {
		testName       string
		schema         *TypeSchema
		expectedSchema *TypeSchema
	}{
		{
			testName:       "set schema",
			schema:         setSchema,
			expectedSchema: setSchema,
		},
		{
			testName:       "clear schema",
			schema:         nil,
			expectedSchema: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithSchema(tc.schema)

			assert.Same(t, stream, returnedStream, "should return the same stream instance")
			assert.Same(t, tc.schema, stream.Schema, "Stream.Schema should point to the expected schema")
		})
	}

	t.Run("replace existing schema", func(t *testing.T) {
		stream := NewStream("users", "public", nil)
		firstSchema := NewTypeSchema()
		secondSchema := NewTypeSchema()

		stream.WithSchema(firstSchema)
		assert.Same(t, firstSchema, stream.Schema, "first schema should be set")

		returnedStream := stream.WithSchema(secondSchema)
		assert.Same(t, stream, returnedStream, "should return the same stream instance")
		assert.Same(t, secondSchema, stream.Schema, "first schema should be replaced by second schema")
		assert.NotSame(t, firstSchema, stream.Schema, "first schema should no longer be referenced here")
	})
}

func TestStreamUpsertField(t *testing.T) {
	tests := []struct {
		testName string

		column        string
		typ           DataType
		nullable      bool
		isOLakeColumn bool

		expectedTypes    []DataType
		expectedOlakeCol bool
	}{
		{
			testName:         "null string datatype test",
			column:           "Student-Name",
			typ:              String,
			nullable:         true,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{String, Null},
			expectedOlakeCol: false,
		},
		{
			testName:         "non-null string datatype test",
			column:           "Student-Name",
			typ:              String,
			nullable:         false,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{String},
			expectedOlakeCol: false,
		},
		{
			testName:         "null int datatype test",
			column:           "Student-Roll",
			typ:              Int64,
			nullable:         true,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Int64, Null},
			expectedOlakeCol: false,
		},
		{
			testName:         "non-null int datatype test",
			column:           "Student-Roll",
			typ:              Int64,
			nullable:         false,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Int64},
			expectedOlakeCol: false,
		},
		{
			testName:         "null float datatype test",
			column:           "Student-Percentage",
			typ:              Float64,
			nullable:         true,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Float64, Null},
			expectedOlakeCol: false,
		},
		{
			testName:         "non-null float datatype test",
			column:           "Student-Percentage %",
			typ:              Float64,
			nullable:         false,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Float64},
			expectedOlakeCol: false,
		},
		{
			testName:         "null bool datatype test",
			column:           "Present",
			typ:              Bool,
			nullable:         true,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Bool, Null},
			expectedOlakeCol: false,
		},
		{
			testName:         "non-null bool datatype test",
			column:           "Present",
			typ:              Bool,
			nullable:         false,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Bool},
			expectedOlakeCol: false,
		},
		{
			testName:         "null timestamp datatype test",
			column:           "Student-Admission",
			typ:              Timestamp,
			nullable:         true,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Timestamp, Null},
			expectedOlakeCol: false,
		},
		{
			testName:         "non-null timestamp datatype test",
			column:           "Student-Admission",
			typ:              Timestamp,
			nullable:         false,
			isOLakeColumn:    false,
			expectedTypes:    []DataType{Timestamp},
			expectedOlakeCol: false,
		},
		{
			testName:         "Olake Column test",
			column:           "metadataOlake",
			typ:              String,
			nullable:         false,
			isOLakeColumn:    true,
			expectedTypes:    []DataType{String},
			expectedOlakeCol: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			asserts := assert.New(t)
			stream := NewStream("grades", "students", nil)
			stream.UpsertField(tc.column, tc.typ, tc.nullable, tc.isOLakeColumn)

			value, ok := stream.Schema.Properties.Load(tc.column)
			asserts.True(ok, "Schema should have column '%s'", tc.column)
			property := value.(*Property)
			asserts.Equal(utils.Reformat(tc.column), property.DestinationColumnName)
			asserts.Equal(tc.expectedOlakeCol, property.OlakeColumn)

			assert.ElementsMatch(t, tc.expectedTypes, property.Type.Array())
		})
	}

	//test for merging two datatypes on subsequent upsert calls with same names
	t.Run("Multiple datatypes test", func(t *testing.T) {
		asserts := assert.New(t)
		stream := NewStream("phones", "seller", nil)
		stream.UpsertField("codename", Int64, false, false)
		stream.UpsertField("codename", String, true, true)

		val, ok := stream.Schema.Properties.Load("codename")
		asserts.True(ok, "Schema should have column 'codename'")

		property := val.(*Property)

		asserts.True(property.Type.Exists(Int64), "1st assert should be present TYPE : 'INT64'")
		asserts.True(property.Type.Exists(String), "2nd assert should be present TYPE : 'String'")
		asserts.True(property.Type.Exists(Null), "2nd assert added Null as well should be present TYPE : 'Null'")

		asserts.Equal(false, property.OlakeColumn)
	})
}

func TestStreamWrap(t *testing.T) {
	tests := []struct {
		testName  string
		syncIndex int
	}{
		{
			testName:  "wrap with index 0",
			syncIndex: 0,
		},
		{
			testName:  "wrap with negative index",
			syncIndex: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			configuredStream := stream.Wrap(tc.syncIndex)

			assert.Equal(t, stream, configuredStream.Stream, "Should wrap the exact same stream instance")
		})
	}
}

func TestStreamUnmarshalJSON(t *testing.T) {
	tests := []struct {
		testName            string
		jsonData            []byte
		wantErr             bool
		expectedName        string
		expectedNamespace   string
		expectedSyncMode    SyncMode
		expectedCursorField string
		expectedSyncModes   []SyncMode
		expectedPrimaryKeys []string
		expectedCursors     []string
	}{
		{
			testName: "proper intilization when missing fields",
			jsonData: []byte(`{
				"name":      "users",
				"namespace": "public"
			}`),
			wantErr:           false,
			expectedName:      "users",
			expectedNamespace: "public",
		},
		{
			testName: "correct data loading",
			jsonData: []byte(`{
				"name":"orders",
				"supported_sync_modes":["full_refresh","incremental"],
				"source_defined_primary_key":["id"],
				"available_cursor_fields":["updated_at"]
			}`),
			wantErr:             false,
			expectedName:        "orders",
			expectedSyncModes:   []SyncMode{FULLREFRESH, INCREMENTAL},
			expectedPrimaryKeys: []string{"id"},
			expectedCursors:     []string{"updated_at"},
		},
		{
			testName: "invalid json test",
			jsonData: []byte(`illegaljson`),
			wantErr:  true,
		},
		{
			testName:          "empty json test",
			jsonData:          []byte(`{}`),
			wantErr:           false,
			expectedName:      "",
			expectedNamespace: "",
		},
		{
			testName: "all fields populated test",
			jsonData: []byte(`{
				"name":"locations",
				"namespace":"deliveries",
				"sync_mode":"incremental",
				"cursor_field": "updated_at",
				"supported_sync_modes": ["full_refresh", "incremental"],
				"source_defined_primary_key": ["location_id"],
				"available_cursor_fields": ["created_at", "updated_at"]
			}`),
			wantErr:             false,
			expectedName:        "locations",
			expectedNamespace:   "deliveries",
			expectedSyncMode:    SyncMode("incremental"),
			expectedCursorField: "updated_at",
			expectedSyncModes:   []SyncMode{FULLREFRESH, INCREMENTAL},
			expectedPrimaryKeys: []string{"location_id"},
			expectedCursors:     []string{"created_at", "updated_at"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			asserts := assert.New(t)

			var stream Stream
			err := json.Unmarshal(tc.jsonData, &stream)

			if tc.wantErr {
				asserts.Error(err)
				return
			}
			asserts.NoError(err)

			asserts.Equal(tc.expectedName, stream.Name)
			asserts.Equal(tc.expectedNamespace, stream.Namespace)
			asserts.Equal(tc.expectedSyncMode, stream.SyncMode)
			asserts.Equal(tc.expectedCursorField, stream.CursorField)

			asserts.NotNil(stream.AvailableCursorFields, "AvailableCursorFields should be initialized")
			asserts.NotNil(stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
			asserts.NotNil(stream.SupportedSyncModes, "SupportedSyncModes should be initialized")

			for _, mode := range tc.expectedSyncModes {
				asserts.True(stream.SupportedSyncModes.Exists(mode), "SupportedSyncModes should contain %v", mode)
			}
			for _, key := range tc.expectedPrimaryKeys {
				asserts.True(stream.SourceDefinedPrimaryKey.Exists(key), "SourceDefinedPrimaryKey should contain %q", key)
			}
			for _, cursor := range tc.expectedCursors {
				asserts.True(stream.AvailableCursorFields.Exists(cursor), "AvailableCursorFields should contain %q", cursor)
			}
		})
	}
}

func TestStreamsToMap(t *testing.T) {
	tests := []struct {
		testName    string
		streams     []*Stream
		expectedLen int
	}{
		{
			testName:    "empty input test",
			streams:     []*Stream{},
			expectedLen: 0,
		},
		{
			testName:    "single stream test",
			streams:     []*Stream{NewStream("users", "public", nil)},
			expectedLen: 1,
		},
		{
			testName: "multiple streams test",
			streams: []*Stream{
				NewStream("users", "public", nil),
				NewStream("orders", "public", nil),
			},
			expectedLen: 2,
		},
		{
			testName: "duplicate IDs test",
			streams: []*Stream{
				NewStream("users", "public", nil),
				NewStream("users", "public", nil),
			},
			expectedLen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			asserts := assert.New(t)

			streamMap := StreamsToMap(tc.streams...)
			asserts.Equal(tc.expectedLen, len(streamMap))

			expectedWinners := make(map[string]*Stream)
			for _, stream := range tc.streams {
				expectedWinners[stream.ID()] = stream
			}
			for id, expected := range expectedWinners {
				mapped, exists := streamMap[id]
				asserts.True(exists, "map should have key for stream ID %q", id)
				asserts.Same(expected, mapped, "map value should be the latest stream for its ID")
			}
		})
	}
}

func TestLogCatalog(t *testing.T) {
	tests := []struct {
		testName    string
		streams     []*Stream
		driver      string
		expectedLen int
	}{
		{
			testName: "writes catalog file with all streams",
			streams: []*Stream{
				NewStream("users", "public", nil),
				NewStream("orders", "public", nil),
			},
			driver:      "postgres",
			expectedLen: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			asserts := assert.New(t)

			tempDir := t.TempDir()
			tmpFilePath := filepath.Join(tempDir, "catalog.json")
			viper.Set(constants.StreamsPath, tmpFilePath)
			t.Cleanup(func() { viper.Set(constants.StreamsPath, "") })

			LogCatalog(tc.streams, nil, tc.driver)

			_, err := os.Stat(tmpFilePath)
			asserts.NoError(err, "LogCatalog should create the streams file")

			content, err := os.ReadFile(tmpFilePath)
			asserts.NoError(err, "Should be able to read the generated file")

			var savedCatalog Catalog
			err = json.Unmarshal(content, &savedCatalog)
			asserts.NoError(err, "File content should be valid JSON")
			asserts.Equal(tc.expectedLen, len(savedCatalog.Streams), "Saved catalog should contain all streams")
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
