package types

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/goccy/go-json"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestStream_NewStream(t *testing.T) {
	stream := NewStream("users", "public", nil)

	assert.Equal(t, "users", stream.Name)
	assert.Equal(t, "public", stream.Namespace)

	assert.NotNil(t, stream.SupportedSyncModes, "SupportedSyncModes should be initialized")
	assert.NotNil(t, stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
	assert.NotNil(t, stream.AvailableCursorFields, "AvailableCursorFields should be initialized")
	assert.NotNil(t, stream.Schema, "Schema should be initialized")

	assert.NotEmpty(t, stream.DestinationDatabase, "DestinationDatabase should be generated")
	assert.NotEmpty(t, stream.DestinationTable, "DestinationTable should be generated")
}

func TestStream_WithSyncMode(t *testing.T) {
	tests := []struct {
		name               string
		modes              []SyncMode
		expectedModes      []SyncMode
		notExpectedModes   []SyncMode
	}{
		{
			name:             "single mode",
			modes:            []SyncMode{FULLREFRESH},
			expectedModes:    []SyncMode{FULLREFRESH},
			notExpectedModes: []SyncMode{INCREMENTAL, CDC},
		},
		{
			name:             "multiple modes",
			modes:            []SyncMode{FULLREFRESH, INCREMENTAL},
			expectedModes:    []SyncMode{FULLREFRESH, INCREMENTAL},
			notExpectedModes: []SyncMode{CDC},
		},
		{
			name:             "all modes",
			modes:            []SyncMode{FULLREFRESH, INCREMENTAL, CDC, STRICTCDC},
			expectedModes:    []SyncMode{FULLREFRESH, INCREMENTAL, CDC, STRICTCDC},
			notExpectedModes: []SyncMode{},
		},
		{
			name:             "duplicate modes",
			modes:            []SyncMode{FULLREFRESH, FULLREFRESH, INCREMENTAL},
			expectedModes:    []SyncMode{FULLREFRESH, INCREMENTAL},
			notExpectedModes: []SyncMode{CDC},
		},
		{
			name:             "empty modes",
			modes:            []SyncMode{},
			expectedModes:    []SyncMode{},
			notExpectedModes: []SyncMode{FULLREFRESH, INCREMENTAL, CDC},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			outputStream := stream.WithSyncMode(tt.modes...)

			// check if it returns the exact same pointer
			assert.Same(t, stream, outputStream, "Should return the same instance")

			// check if the set contains added modes
			for _, mode := range tt.expectedModes {
				assert.True(t, outputStream.SupportedSyncModes.Exists(mode), "Should contain %v", mode)
			}

			// check if the set does not contain other modes
			for _, mode := range tt.notExpectedModes {
				assert.False(t, outputStream.SupportedSyncModes.Exists(mode), "Should not contain %v", mode)
			}
		})
	}
}

func TestStream_WithPrimaryKey(t *testing.T) {
	tests := []struct {
		name               string
		keys               []string
		expectedKeys       []string
		notExpectedKeys    []string
	}{
		{
			name:            "single key",
			keys:            []string{"id"},
			expectedKeys:    []string{"id"},
			notExpectedKeys: []string{"user_uuid", "created_at"},
		},
		{
			name:            "multiple keys",
			keys:            []string{"id", "user_uuid"},
			expectedKeys:    []string{"id", "user_uuid"},
			notExpectedKeys: []string{"created_at"},
		},
		{
			name:            "composite key",
			keys:            []string{"tenant_id", "user_id", "order_id"},
			expectedKeys:    []string{"tenant_id", "user_id", "order_id"},
			notExpectedKeys: []string{"id"},
		},
		{
			name:            "duplicate keys",
			keys:            []string{"id", "id", "user_uuid"},
			expectedKeys:    []string{"id", "user_uuid"},
			notExpectedKeys: []string{"created_at"},
		},
		{
			name:            "empty keys",
			keys:            []string{},
			expectedKeys:    []string{},
			notExpectedKeys: []string{"id", "user_uuid"},
		},
		{
			name:            "keys with underscores",
			keys:            []string{"user_id", "order_id"},
			expectedKeys:    []string{"user_id", "order_id"},
			notExpectedKeys: []string{"userid", "orderid"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			returnedStream := stream.WithPrimaryKey(tt.keys...)

			assert.Same(t, stream, returnedStream, "Should return the same instance")

			for _, key := range tt.expectedKeys {
				assert.True(t, stream.SourceDefinedPrimaryKey.Exists(key), "Should contain '%s'", key)
			}

			for _, key := range tt.notExpectedKeys {
				assert.False(t, stream.SourceDefinedPrimaryKey.Exists(key), "Should not contain '%s'", key)
			}
		})
	}
}

func TestStream_WithCursorField(t *testing.T) {
	tests := []struct {
		name                string
		fields              []string
		expectedFields      []string
		notExpectedFields   []string
	}{
		{
			name:              "single field",
			fields:            []string{"updated_at"},
			expectedFields:    []string{"updated_at"},
			notExpectedFields: []string{"inserted_at", "id"},
		},
		{
			name:              "multiple fields",
			fields:            []string{"updated_at", "inserted_at"},
			expectedFields:    []string{"updated_at", "inserted_at"},
			notExpectedFields: []string{"id"},
		},
		{
			name:              "timestamp fields",
			fields:            []string{"created_at", "updated_at", "deleted_at"},
			expectedFields:    []string{"created_at", "updated_at", "deleted_at"},
			notExpectedFields: []string{"id"},
		},
		{
			name:              "duplicate fields",
			fields:            []string{"updated_at", "updated_at", "inserted_at"},
			expectedFields:    []string{"updated_at", "inserted_at"},
			notExpectedFields: []string{"id"},
		},
		{
			name:              "empty fields",
			fields:            []string{},
			expectedFields:    []string{},
			notExpectedFields: []string{"updated_at", "inserted_at"},
		},
		{
			name:              "fields with underscores",
			fields:            []string{"last_modified", "date_created"},
			expectedFields:    []string{"last_modified", "date_created"},
			notExpectedFields: []string{"lastmodified", "datecreated"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			outputStream := stream.WithCursorField(tt.fields...)

			assert.Same(t, stream, outputStream, "Should return the same instance")

			for _, field := range tt.expectedFields {
				assert.True(t, stream.AvailableCursorFields.Exists(field), "Should contain '%s'", field)
			}

			for _, field := range tt.notExpectedFields {
				assert.False(t, stream.AvailableCursorFields.Exists(field), "Should not contain '%s'", field)
			}
		})
	}
}

func TestStream_WithSchema(t *testing.T) {
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

func TestStream_Wrap(t *testing.T) {
	tests := []struct {
		name      string
		syncIndex int
	}{
		{
			name:      "wrap with index 0",
			syncIndex: 0,
		},
		{
			name:      "wrap with index 1",
			syncIndex: 1,
		},
		{
			name:      "wrap with negative index",
			syncIndex: -1,
		},
		{
			name:      "wrap with large index",
			syncIndex: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("users", "public", nil)
			configuredStream := stream.Wrap(tt.syncIndex)

			assert.NotNil(t, configuredStream, "Should return a configuredStream")
			assert.Same(t, stream, configuredStream.Stream, "Should wrap the exact same stream instance")
		})
	}
}

func TestStream_WithUpsertField(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		dataType DataType
		nullable bool
		check    func(t *testing.T, stream *Stream, fieldName string)
	}{
		{
			name:     "non-nullable string field",
			field:    "type",
			dataType: String,
			nullable: false,
			check: func(t *testing.T, stream *Stream, fieldName string) {
				val, ok := stream.Schema.Properties.Load(fieldName)
				assert.True(t, ok, "Schema should contain '%s'", fieldName)
				prop, ok := val.(*Property)
				assert.True(t, ok, "Value should be of type *Property")
				assert.True(t, prop.Type.Exists(String), "%s should contain String type", fieldName)
				assert.False(t, prop.Type.Exists(Null), "%s should not contain NULL type", fieldName)
			},
		},
		{
			name:     "nullable integer field",
			field:    "price",
			dataType: Int64,
			nullable: true,
			check: func(t *testing.T, stream *Stream, fieldName string) {
				val, ok := stream.Schema.Properties.Load(fieldName)
				assert.True(t, ok, "Schema should contain '%s'", fieldName)
				prop, ok := val.(*Property)
				assert.True(t, ok, "Value should be of type *Property")
				assert.True(t, prop.Type.Exists(Int64), "%s should contain Int64 type", fieldName)
				assert.True(t, prop.Type.Exists(Null), "%s should contain Null type because nullable=true", fieldName)
			},
		},
		{
			name:     "non-nullable boolean field",
			field:    "is_active",
			dataType: Bool,
			nullable: false,
			check: func(t *testing.T, stream *Stream, fieldName string) {
				val, ok := stream.Schema.Properties.Load(fieldName)
				assert.True(t, ok, "Schema should contain '%s'", fieldName)
				prop, ok := val.(*Property)
				assert.True(t, ok, "Value should be of type *Property")
				assert.True(t, prop.Type.Exists(Bool), "%s should contain Bool type", fieldName)
				assert.False(t, prop.Type.Exists(Null), "%s should not contain NULL type", fieldName)
			},
		},
		{
			name:     "nullable timestamp field",
			field:    "deleted_at",
			dataType: Timestamp,
			nullable: true,
			check: func(t *testing.T, stream *Stream, fieldName string) {
				val, ok := stream.Schema.Properties.Load(fieldName)
				assert.True(t, ok, "Schema should contain '%s'", fieldName)
				prop, ok := val.(*Property)
				assert.True(t, ok, "Value should be of type *Property")
				assert.True(t, prop.Type.Exists(Timestamp), "%s should contain Timestamp type", fieldName)
				assert.True(t, prop.Type.Exists(Null), "%s should contain Null type because nullable=true", fieldName)
			},
		},
		{
			name:     "overwrite existing field",
			field:    "status",
			dataType: String,
			nullable: false,
			check: func(t *testing.T, stream *Stream, fieldName string) {
				val, ok := stream.Schema.Properties.Load(fieldName)
				assert.True(t, ok, "Schema should contain '%s'", fieldName)
				prop, ok := val.(*Property)
				assert.True(t, ok, "Value should be of type *Property")
				assert.True(t, prop.Type.Exists(String), "%s should contain String type", fieldName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewStream("products", "inventory", nil)

			// For the overwrite test case, add the field first
			if tt.name == "overwrite existing field" {
				stream.UpsertField(tt.field, Int64, true)
			}

			stream.UpsertField(tt.field, tt.dataType, tt.nullable)
			tt.check(t, stream, tt.field)
		})
	}
}

func TestStream_UnmarshalJSON(t *testing.T) {
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
		assert.NotNil(t, stream.SupportedSyncModes, "SourceDefinedPrimaryKey should be initialized")
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
}

func TestStreamsToMap(t *testing.T) {
	stream1 := NewStream("users", "public", nil)
	stream2 := NewStream("orders", "publc", nil)

	streamMap := StreamsToMap(stream1, stream2)

	assert.Equal(t, 2, len(streamMap), "Map should have only 2 streams")

	// verify if key's and values are same pointers
	// 1.Stream1
	mappedS1, existsS1 := streamMap[stream1.ID()]
	assert.True(t, existsS1, "Map should contain key for stream1")
	assert.Same(t, stream1, mappedS1, "Map value should point to original stream1 object")

	// 2.Stream2
	mappedS2, existsS2 := streamMap[stream2.ID()]
	assert.True(t, existsS2, "Map should contain key for stream2")
	assert.Same(t, stream2, mappedS2, "Map value should point to original stream2 object")

}

func TestLogCatalog(t *testing.T) {
	tempDir := t.TempDir()
	tmpFilePath := filepath.Join(tempDir, "catalog.json")
	viper.Set(constants.StreamsPath, tmpFilePath)

	streams := []*Stream{
		NewStream("users", "public", nil),
		NewStream("orders", "public", nil),
	}

	LogCatalog(streams, nil, "postgres")

	_, err := os.Stat(tmpFilePath)
	assert.NoError(t, err, "Logcatalog should create the streams file")

	content, err := os.ReadFile(tmpFilePath)
	assert.NoError(t, err, "Should be able to read the generate file")

	var savedCatalog Catalog

	err = json.Unmarshal(content, &savedCatalog)
	assert.NoError(t, err, "File content should be valid JSON")
	assert.Equal(t, 2, len(savedCatalog.Streams), "Saved catalog should contain 2 streams")

}
