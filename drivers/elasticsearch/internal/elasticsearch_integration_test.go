package driver

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

// TestElasticsearchIntegration is a placeholder for integration tests
// These tests require a running Elasticsearch instance and are typically run manually
// To run: ES_INTEGRATION_TEST=1 go test -v ./drivers/elasticsearch/...
func TestElasticsearchIntegration(t *testing.T) {
	t.Skip("Integration tests require a running Elasticsearch instance")
}

// TestStreamConfiguration verifies that streams are properly configured
func TestStreamConfiguration(t *testing.T) {
	stream := types.NewStream("test_index", "elasticsearch", nil)

	// Verify stream is created
	require.NotNil(t, stream)
	require.Equal(t, "test_index", stream.Name)
	require.Equal(t, "elasticsearch", stream.Namespace)

	// Verify schema is initialized
	require.NotNil(t, stream.Schema)

	// Add fields to stream
	stream.UpsertField("id", types.String, false)
	stream.UpsertField("name", types.String, true)
	stream.UpsertField("age", types.Int64, true)
	stream.UpsertField("score", types.Float64, true)
	stream.UpsertField("is_active", types.Bool, true)
	stream.UpsertField("created_date", types.Timestamp, true)

	// Verify fields were added
	idFound, _ := stream.Schema.GetProperty("id")
	require.True(t, idFound, "id field should be in schema")

	nameFound, _ := stream.Schema.GetProperty("name")
	require.True(t, nameFound, "name field should be in schema")

	// Verify datatypes
	idType, err := stream.Schema.GetType("id")
	require.NoError(t, err)
	require.Equal(t, types.String, idType)

	ageType, err := stream.Schema.GetType("age")
	require.NoError(t, err)
	require.Equal(t, types.Int64, ageType)

	scoreType, err := stream.Schema.GetType("score")
	require.NoError(t, err)
	require.Equal(t, types.Float64, scoreType)

	isActiveType, err := stream.Schema.GetType("is_active")
	require.NoError(t, err)
	require.Equal(t, types.Bool, isActiveType)

	createdDateType, err := stream.Schema.GetType("created_date")
	require.NoError(t, err)
	require.Equal(t, types.Timestamp, createdDateType)
}

// TestElasticsearchTypeMapping verifies all Elasticsearch types are correctly mapped
func TestElasticsearchTypeMapping(t *testing.T) {
	tests := []struct {
		name     string
		esType   string
		expected types.DataType
	}{
		{"text", "text", types.String},
		{"keyword", "keyword", types.String},
		{"long", "long", types.Int64},
		{"integer", "integer", types.Int64},
		{"short", "short", types.Int64},
		{"byte", "byte", types.Int64},
		{"double", "double", types.Float64},
		{"float", "float", types.Float64},
		{"half_float", "half_float", types.Float64},
		{"scaled_float", "scaled_float", types.Float64},
		{"date", "date", types.Timestamp},
		{"boolean", "boolean", types.Bool},
		{"binary", "binary", types.String},
		{"object", "object", types.String},
		{"nested", "nested", types.String},
		{"geo_point", "geo_point", types.String},
		{"geo_shape", "geo_shape", types.String},
		{"ip", "ip", types.String},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, exists := esTypeToDataTypes[tt.esType]
			require.True(t, exists, "Type %s not found in mapping", tt.esType)
			require.Equal(t, tt.expected, got, "Type mapping mismatch for %s", tt.esType)
		})
	}
}

// TestIncrementalQueryBuilder verifies query building for incremental syncs
func TestIncrementalQueryBuilder(t *testing.T) {
	driver := &Elasticsearch{}

	tests := []struct {
		name        string
		cursorField string
		lastCursor  interface{}
		expectRange bool
	}{
		{
			name:        "with string cursor",
			cursorField: "@timestamp",
			lastCursor:  "2024-01-18T14:30:00Z",
			expectRange: true,
		},
		{
			name:        "with numeric cursor",
			cursorField: "user_id",
			lastCursor:  1001,
			expectRange: true,
		},
		{
			name:        "without cursor",
			cursorField: "@timestamp",
			lastCursor:  nil,
			expectRange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := driver.buildIncrementalQuery(tt.cursorField, tt.lastCursor)
			require.NotNil(t, query, "Query should not be nil")

			queryObj, ok := query["query"].(map[string]interface{})
			require.True(t, ok, "Query should have 'query' field")

			if tt.expectRange {
				_, hasRange := queryObj["range"]
				require.True(t, hasRange, "Query should have 'range' clause")
			} else {
				_, hasMatchAll := queryObj["match_all"]
				require.True(t, hasMatchAll, "Query should have 'match_all' clause")
			}
		})
	}
}

// TestProcessPropertiesWithNestedObjects verifies nested object handling
func TestProcessPropertiesWithNestedObjects(t *testing.T) {
	driver := &Elasticsearch{}
	stream := types.NewStream("test_index", "elasticsearch", nil)

	properties := map[string]interface{}{
		"id": map[string]interface{}{
			"type": "keyword",
		},
		"metadata": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"department": map[string]interface{}{
					"type": "keyword",
				},
				"level": map[string]interface{}{
					"type": "integer",
				},
			},
		},
		"addresses": map[string]interface{}{
			"type": "nested",
			"properties": map[string]interface{}{
				"street": map[string]interface{}{
					"type": "text",
				},
				"city": map[string]interface{}{
					"type": "keyword",
				},
			},
		},
	}

	driver.processProperties(stream, properties, "")

	// Verify all fields were added including nested ones
	expectedFields := []string{
		"id", "metadata.department", "metadata.level",
		"addresses.street", "addresses.city",
	}

	for _, field := range expectedFields {
		found, _ := stream.Schema.GetProperty(field)
		require.True(t, found, "Field %s not found in schema", field)
	}

	// Verify datatypes
	idType, _ := stream.Schema.GetType("id")
	require.Equal(t, types.String, idType)

	deptType, _ := stream.Schema.GetType("metadata.department")
	require.Equal(t, types.String, deptType)

	levelType, _ := stream.Schema.GetType("metadata.level")
	require.Equal(t, types.Int64, levelType)

	streetType, _ := stream.Schema.GetType("addresses.street")
	require.Equal(t, types.String, streetType)

	cityType, _ := stream.Schema.GetType("addresses.city")
	require.Equal(t, types.String, cityType)
}

// TestCursorFieldSelection verifies cursor field handling
func TestCursorFieldSelection(t *testing.T) {
	stream := types.NewStream("test_index", "elasticsearch", nil)

	// Add various field types that could be used as cursors
	stream.UpsertField("id", types.String, false)
	stream.UpsertField("user_id", types.Int64, true)
	stream.UpsertField("created_at", types.Timestamp, true)
	stream.UpsertField("updated_at", types.Timestamp, true)

	// Verify all fields are available as cursor candidates
	idFound, _ := stream.Schema.GetProperty("id")
	require.True(t, idFound)

	userIDFound, _ := stream.Schema.GetProperty("user_id")
	require.True(t, userIDFound)

	createdAtFound, _ := stream.Schema.GetProperty("created_at")
	require.True(t, createdAtFound)

	updatedAtFound, _ := stream.Schema.GetProperty("updated_at")
	require.True(t, updatedAtFound)
}

// TestSyncModeSupport verifies sync mode configuration
func TestSyncModeSupport(t *testing.T) {
	stream := types.NewStream("test_index", "elasticsearch", nil)

	// Add supported sync modes
	stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)

	// Verify sync modes are supported
	require.True(t, stream.SupportedSyncModes.Len() > 0)
}

// TestPrimaryKeyConfiguration verifies primary key setup
func TestPrimaryKeyConfiguration(t *testing.T) {
	stream := types.NewStream("test_index", "elasticsearch", nil)

	// Add primary key
	stream.WithPrimaryKey("_id")

	// Verify primary key is set
	require.True(t, stream.SourceDefinedPrimaryKey.Len() > 0)
}
