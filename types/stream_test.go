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
				Name:                "",
				Namespace:           "grades",
				DestinationDatabase: "gradesdb:grades",
				DestinationTable:    "",
			},
		},
		{
			testName:       "stream with empty 'namespace' but all other fields filled in",
			name:           "students",
			namespace:      "",
			sourceDatabase: stringPtr("gradesdb"),
			expectedStream: &Stream{
				Name:                "students",
				Namespace:           "",
				DestinationDatabase: "gradesdb",
				DestinationTable:    "students",
			},
		},
		{
			testName:       "stream with nil 'sourceDatabase' but all other fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: nil,
			expectedStream: &Stream{
				Name:                "students",
				Namespace:           "grades",
				DestinationDatabase: ":grades",
				DestinationTable:    "students",
			},
		},
		{
			testName:       "stream with empty 'sourceDatabase' but all other fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr(""),
			expectedStream: &Stream{
				Name:                "students",
				Namespace:           "grades",
				DestinationDatabase: ":grades",
				DestinationTable:    "students",
			},
		},
		{
			testName:       "stream with all fields empty or nil",
			name:           "",
			namespace:      "",
			sourceDatabase: nil,
			expectedStream: &Stream{
				Name:                "",
				Namespace:           "",
				DestinationDatabase: "",
				DestinationTable:    "",
			},
		},
		{
			testName:       "stream with all fields filled in",
			name:           "students",
			namespace:      "grades",
			sourceDatabase: stringPtr("gradesdb"),
			expectedStream: &Stream{
				Name:                "students",
				Namespace:           "grades",
				DestinationDatabase: "gradesdb:grades",
				DestinationTable:    "students",
			},
		},
		{
			testName:       "stream with uppercase and special characters in 'name' and 'namespace",
			name:           "User-Orders.v2",
			namespace:      "My.Schema",
			sourceDatabase: nil,
			expectedStream: &Stream{
				Name:                "User-Orders.v2",
				Namespace:           "My.Schema",
				DestinationDatabase: ":my_schema",
				DestinationTable:    "user_orders_v2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			asserts := assert.New(t)

			stream := NewStream(tt.name, tt.namespace, tt.sourceDatabase)

			asserts.Equal(tt.expectedStream.Name, stream.Name)
			asserts.Equal(tt.expectedStream.Namespace, stream.Namespace)
			asserts.NotNil(stream.SupportedSyncModes, "SupportedSyncModes should be initialized")
			asserts.NotNil(stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
			asserts.NotNil(stream.AvailableCursorFields, "AvailableCursorFields should be initialized")
			asserts.NotNil(stream.Schema, "Schema should be initialized")
			asserts.Equal(tt.expectedStream.DestinationDatabase, stream.DestinationDatabase)
			asserts.Equal(tt.expectedStream.DestinationTable, stream.DestinationTable)
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
			testName:   "name field empty namespace and Id filled",
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

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			stream := &Stream{
				Name:      tt.name,
				Namespace: tt.namespace,
			}
			assert.Equal(t, tt.expectedID, stream.ID())
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

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			outputStream := stream.WithSyncMode(tt.modes...)

			// check if it returns the exact same pointer
			assert.Same(t, stream, outputStream, "Should return the same instance")

			// check if the set contains added modes
			assert.ElementsMatch(t, tt.expectedModes, outputStream.SupportedSyncModes.Array())
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
			testName:     "multiple keys",
			keys:         []string{"id", "user_uuid"},
			expectedKeys: []string{"id", "user_uuid"},
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
		{
			testName:     "keys with underscores",
			keys:         []string{"user_id", "order_id"},
			expectedKeys: []string{"user_id", "order_id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithPrimaryKey(tt.keys...)

			assert.Same(t, stream, returnedStream, "Should return the same instance")

			assert.ElementsMatch(t, tt.expectedKeys, stream.SourceDefinedPrimaryKey.Array())
		})
	}
}

func TestStreamWithCursorField(t *testing.T) {
	tests := []struct {
		testName       string
		fields         []string
		expectedFields []string
	}{
		{
			testName:       "single field",
			fields:         []string{"updated_at"},
			expectedFields: []string{"updated_at"},
		},
		{
			testName:       "multiple fields",
			fields:         []string{"updated_at", "inserted_at"},
			expectedFields: []string{"updated_at", "inserted_at"},
		},
		{
			testName:       "timestamp fields",
			fields:         []string{"created_at", "updated_at", "deleted_at"},
			expectedFields: []string{"created_at", "updated_at", "deleted_at"},
		},
		{
			testName:       "duplicate fields",
			fields:         []string{"updated_at", "updated_at", "inserted_at"},
			expectedFields: []string{"updated_at", "inserted_at"},
		},
		{
			testName:       "empty fields",
			fields:         []string{},
			expectedFields: []string{},
		},
		{
			testName:       "fields with underscores",
			fields:         []string{"last_modified", "date_created"},
			expectedFields: []string{"last_modified", "date_created"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			outputStream := stream.WithCursorField(tt.fields...)

			assert.Same(t, stream, outputStream, "Should return the same instance")

			assert.ElementsMatch(t, tt.expectedFields, stream.AvailableCursorFields.Array())
		})
	}
}

func TestStreamWithSchema(t *testing.T) {
	t.Run("set new schema", func(t *testing.T) {
		stream := NewStream("users", "public", nil)
		newSchema := NewTypeSchema()
		returnedStream := stream.WithSchema(newSchema)

		assert.Same(t, stream, returnedStream, "Should return the same stream instance")
		assert.Same(t, newSchema, stream.Schema, "Stream.Schema should point to the input schema")
	})

	t.Run("replace existing schema", func(t *testing.T) {
		stream := NewStream("users", "public", nil)
		firstSchema := NewTypeSchema()
		secondSchema := NewTypeSchema()

		stream.WithSchema(firstSchema)
		assert.Same(t, firstSchema, stream.Schema, "First schema should be set")

		returnedStream := stream.WithSchema(secondSchema)
		assert.Same(t, stream, returnedStream, "Should return the same stream instance")
		assert.Same(t, secondSchema, stream.Schema, "Second schema should replace first schema")
		assert.NotSame(t, firstSchema, stream.Schema, "First schema should no longer be referenced")
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

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			asserts := assert.New(t)

			stream := NewStream("grades", "students", nil)
			stream.UpsertField(tt.column, tt.typ, tt.nullable, tt.isOLakeColumn)

			value, ok := stream.Schema.Properties.Load(tt.column)
			asserts.True(ok, "Schema should have column '%s'", tt.column)
			property := value.(*Property)
			asserts.Equal(utils.Reformat(tt.column), property.DestinationColumnName)
			asserts.Equal(tt.expectedOlakeCol, property.OlakeColumn)

			assert.ElementsMatch(t, tt.expectedTypes, property.Type.Array())
		})
	}

	//test for merging two datatypes on subsequent upsert calls with same names
	t.Run("Multiple datatypes test", func(t *testing.T) {
		asserts := assert.New(t)
		stream := NewStream("phones", "seller", nil)

		stream.UpsertField("codename", Int64, false, false)
		stream.UpsertField("codename", String, true, true)

		val, ok := stream.Schema.Properties.Load("codename")
		asserts.True(ok, "Schema should have column 'codename")

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
			testName:  "wrap with index 1",
			syncIndex: 1,
		},
		{
			testName:  "wrap with negative index",
			syncIndex: -1,
		},
		{
			testName:  "wrap with large index",
			syncIndex: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			configuredStream := stream.Wrap(tt.syncIndex)

			assert.NotNil(t, configuredStream, "Should return a configuredStream")
			assert.Same(t, stream, configuredStream.Stream, "Should wrap the exact same stream instance")
		})
	}
}

func TestStreamUnmarshalJSON(t *testing.T) {
	t.Run("Safe intilization on Missing Fields", func(t *testing.T) {
		jsonData := []byte(`{
			"name":      "users",
			"namespace": "public"
		}`)

		var stream Stream

		err := json.Unmarshal(jsonData, &stream)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "users", stream.Name)
		assert.Equal(t, "public", stream.Namespace)

		// to prevent nil pointer panics later
		assert.NotNil(t, stream.AvailableCursorFields, "AvailableCursorFields should be initialized")
		assert.NotNil(t, stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
		assert.NotNil(t, stream.SupportedSyncModes, "SupportedSyncModes should be initialized")
	})

	t.Run("Correct data loading", func(t *testing.T) {
		jsonData := []byte(`{
			"name":"orders",
			"supported_sync_modes":["full_refresh","incremental"],
			"source_defined_primary_key":["id"],
			"available_cursor_fields":["updated_at"]
		}`)

		var stream Stream

		err := json.Unmarshal(jsonData, &stream)

		assert.NoError(t, err)
		assert.Equal(t, "orders", stream.Name)

		// Verify data was actually loaded into the sets
		assert.True(t, stream.AvailableCursorFields.Exists("updated_at"), "Should contain cursor 'updated_at'")
		assert.True(t, stream.SupportedSyncModes.Exists("full_refresh"), "Should contain full_refresh")
		assert.True(t, stream.SourceDefinedPrimaryKey.Exists("id"), "Should contain primary key 'id'")
	})

	t.Run("Invalid json test", func(t *testing.T) {
		jsonData := []byte(`illegaljson`)

		var stream Stream
		err := json.Unmarshal(jsonData, &stream)

		assert.Error(t, err)
	})

	t.Run("Empty json test", func(t *testing.T) {
		asserts := assert.New(t)

		jsonData := []byte(`{}`)

		var stream Stream
		err := json.Unmarshal(jsonData, &stream)

		asserts.NoError(err)
		asserts.Equal("", stream.Name)
		asserts.Equal("", stream.Namespace)

		//sets will still be initialized
		asserts.NotNil(stream.AvailableCursorFields, "AvailableCursorFields should be initialized")
		asserts.NotNil(stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
		asserts.NotNil(stream.SupportedSyncModes, "SupportedSyncModes should be initialized")
	})

	t.Run("All fields populated test", func(t *testing.T) {
		jsonData := []byte(`{
		"name":"locations",
		"namespace":"deliveries",
		"sync_mode":"incremental",
		"cursor_field": "updated_at",
        "supported_sync_modes": ["full_refresh", "incremental"],
        "source_defined_primary_key": ["location_id"],
        "available_cursor_fields": ["created_at", "updated_at"]
		}`)

		asserts := assert.New(t)

		var stream Stream

		err := json.Unmarshal(jsonData, &stream)

		asserts.NoError(err)
		asserts.Equal("locations", stream.Name)
		asserts.Equal("deliveries", stream.Namespace)
		asserts.Equal(SyncMode("incremental"), stream.SyncMode)
		asserts.Equal("updated_at", stream.CursorField)
		asserts.True(stream.SupportedSyncModes.Exists(FULLREFRESH))
		asserts.True(stream.SupportedSyncModes.Exists(INCREMENTAL))
		asserts.True(stream.SourceDefinedPrimaryKey.Exists("location_id"))
		asserts.True(stream.AvailableCursorFields.Exists("created_at"))
		asserts.True(stream.AvailableCursorFields.Exists("updated_at"))
	})
}

func TestStreamsToMap(t *testing.T) {

	t.Run("empty input test", func(t *testing.T) {
		streamMap := StreamsToMap()
		assert.Equal(t, 0, len(streamMap), "map should be empty")
	})

	t.Run("single stream test", func(t *testing.T) {
		stream := NewStream("users", "public", nil)
		streamMap := StreamsToMap(stream)
		asserts := assert.New(t)

		asserts.Equal(1, len(streamMap))
		mapped, exists := streamMap[stream.ID()]
		asserts.True(exists, "map should have key for stream")
		asserts.Same(stream, mapped, "map value should point to original stream object")
	})

	t.Run("multiple streams test", func(t *testing.T) {
		stream1 := NewStream("users", "public", nil)
		stream2 := NewStream("orders", "public", nil)

		streamMap := StreamsToMap(stream1, stream2)

		asserts := assert.New(t)

		asserts.Equal(2, len(streamMap), "Map should have 2 streams")

		// verify keys and values are same pointers
		mappedS1, existsS1 := streamMap[stream1.ID()]
		asserts.True(existsS1, "Map should have key for stream1")
		asserts.Same(stream1, mappedS1, "Map value should point to original stream1 object")

		mappedS2, existsS2 := streamMap[stream2.ID()]
		asserts.True(existsS2, "Map should have key for stream2")
		asserts.Same(stream2, mappedS2, "Map value should point to original stream2 object")
	})

	t.Run("duplicate IDs test", func(t *testing.T) {
		stream1 := NewStream("users", "public", nil)
		stream2 := NewStream("users", "public", nil)

		streamMap := StreamsToMap(stream1, stream2)

		assert.Equal(t, 1, len(streamMap), "duplicate Ids should result in single entry only")
		mapped := streamMap[stream1.ID()]
		assert.Same(t, stream2, mapped, "latest stream should win")
	})
}

func TestLogCatalog(t *testing.T) {
	tempDir := t.TempDir()
	tmpFilePath := filepath.Join(tempDir, "catalog.json")
	viper.Set(constants.StreamsPath, tmpFilePath)
	t.Cleanup(func() { viper.Set(constants.StreamsPath, "") })

	streams := []*Stream{
		NewStream("users", "public", nil),
		NewStream("orders", "public", nil),
	}

	LogCatalog(streams, nil, "postgres")

	_, err := os.Stat(tmpFilePath)
	assert.NoError(t, err, "Logcatalog should create the streams file")

	content, err := os.ReadFile(tmpFilePath)
	assert.NoError(t, err, "Should be able to read the generated file")

	var savedCatalog Catalog

	err = json.Unmarshal(content, &savedCatalog)
	assert.NoError(t, err, "File content should be valid JSON")
	assert.Equal(t, 2, len(savedCatalog.Streams), "Saved catalog should contain 2 streams")
}

// helper to get string pointer
func stringPtr(s string) *string {
	return &s
}
