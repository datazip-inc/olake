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

func TestNewStreamInitlization(t *testing.T) {
	stream := NewStream("users", "public", nil)

	assert.Equal(t, "users", stream.Name)
	assert.Equal(t, "public", stream.Namespace)

	assert.NotNil(t, stream.SupportedSyncModes, "SupportedSyncModes should be initialized")
	assert.NotNil(t, stream.SourceDefinedPrimaryKey, "SourceDefinedPrimaryKey should be initialized")
	assert.NotNil(t, stream.AvailableCursorFields, "AvailableCursorFields should be intialized")
	assert.NotNil(t, stream.Schema, "Schema is not defined")

	assert.NotEmpty(t, stream.DestinationDatabase, "DestinationDatabase should be generated")
	assert.NotEmpty(t, stream.DestinationTable, "DestinationTable should be generated")
}

func TestWithSyncMode(t *testing.T) {
	stream := NewStream("users", "public", nil)

	outputStream := stream.WithSyncMode(FULLREFRESH, INCREMENTAL)

	// check if it returns the exact same pointer
	assert.Same(t, stream, outputStream, "Should return the same instance")

	// check if the set contains added modes
	assert.True(t, outputStream.SupportedSyncModes.Exists(FULLREFRESH), "Should contain FULLREFRESH")
	assert.True(t, outputStream.SupportedSyncModes.Exists(INCREMENTAL), "Should contain INCREMENTAL")

	// check if the set contains other nodes
	assert.False(t, outputStream.SupportedSyncModes.Exists(CDC), "Should no contain CDC")
}

func TestWithPrimaryKey(t *testing.T) {
	stream := NewStream("users", "public", nil)

	returnedStream := stream.WithPrimaryKey("id", "user_uuid")

	assert.Same(t, stream, returnedStream, "Should return the same instance")

	assert.True(t, stream.SourceDefinedPrimaryKey.Exists("id"), "Should contain 'id'")
	assert.True(t, stream.SourceDefinedPrimaryKey.Exists("user_uuid"), "Should contain 'user_uuid'")

	assert.False(t, stream.SourceDefinedPrimaryKey.Exists("created_at"), "Should not contain 'created_at'")

}

func TestWithCursorField(t *testing.T) {
	stream := NewStream("users", "public", nil)

	outputStream := stream.WithCursorField("updated_at", "inserted_at")

	assert.Same(t, stream, outputStream, "Should return the same instance")

	assert.True(t, stream.AvailableCursorFields.Exists("updated_at"), "Should contain 'updated_at'")
	assert.True(t, stream.AvailableCursorFields.Exists("inserted_at"), "Should contain 'inserted_at'")

	assert.False(t, stream.AvailableCursorFields.Exists("id"), "Should not contain 'id'")
}

func TestWithSchema(t *testing.T) {

	stream := NewStream("users", "public", nil)
	newSchema := NewTypeSchema()
	returnedStream := stream.WithSchema(newSchema)

	assert.Same(t, stream, returnedStream, "Should return the same stream instance")
	assert.Same(t, newSchema, stream.Schema, "Stream.Schema should point to the input schema")
}

func TestWrap(t *testing.T) {
	stream := NewStream("users", "public", nil)

	configuredStream := stream.Wrap(1)

	// check if it is existing
	assert.NotNil(t, configuredStream, "Should return a configuredStream")

	assert.Same(t, stream, configuredStream.Stream, "Should wrap the exact same stream instance")
}

func TestWithUpsertField(t *testing.T) {
	stream := NewStream("products", "inventory", nil)

	stream.UpsertField("type", String, false)
	stream.UpsertField("price", Int64, false)

	// For type
	valType, okType := stream.Schema.Properties.Load("type")
	assert.True(t, okType, "Schema should contain 'type'")

	if okType {
		prop, ok := valType.(*Property)
		assert.True(t, ok, "Value should be of type *Property")

		assert.True(t, prop.Type.Exists(String), "type should not contain String type")
		assert.False(t, prop.Type.Exists(Null), "type should not contain NULL type")
	}

	// For price
	valPrice, okPrice := stream.Schema.Properties.Load("price")
	assert.True(t, okPrice, "Schem should contain 'price'")

	if okPrice {
		prop, ok := valPrice.(*Property)
		assert.True(t, ok, "Value should be of *Property")

		assert.True(t, prop.Type.Exists(Int64), "price should contain Integer type")
		assert.True(t, prop.Type.Exists(Null), "price should contain Null type because nullable=true")
	}
}

func TestUnmarshalJSON(t *testing.T) {
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

func TestStreamToMap(t *testing.T) {
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
