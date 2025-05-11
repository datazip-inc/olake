package typeutils

import (
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	testCases := []struct {
		name     string
		objects  []map[string]interface{}
		expected map[string]struct {
			dataType types.DataType
			nullable bool
		}
	}{
		{
			name: "basic types",
			objects: []map[string]interface{}{
				{
					"string_field": "test string",
					"int_field":    int32(42),
					"float_field":  float64(3.14),
					"bool_field":   true,
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"string_field": {dataType: types.String, nullable: false},
				"int_field":    {dataType: types.Int32, nullable: false},
				"float_field":  {dataType: types.Float64, nullable: false},
				"bool_field":   {dataType: types.Bool, nullable: false},
			},
		},
		{
			name: "nullable fields",
			objects: []map[string]interface{}{
				{
					"field1": "value1",
					"field2": int32(123),
				},
				{
					"field1": "value2",
					// field2 is missing, should be marked as nullable
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"field1": {dataType: types.String, nullable: false},
				"field2": {dataType: types.Int32, nullable: true},
			},
		},
		{
			name: "complex types",
			objects: []map[string]interface{}{
				{
					"array_field":  []int{1, 2, 3},
					"object_field": map[string]interface{}{"key": "value"},
					"time_field":   time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			expected: map[string]struct {
				dataType types.DataType
				nullable bool
			}{
				"array_field":  {dataType: types.Array, nullable: false},
				"object_field": {dataType: types.Object, nullable: false},
				"time_field":   {dataType: types.Timestamp, nullable: false},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a new stream for testing
			stream := types.NewStream("test_stream", "test_namespace")

			err := Resolve(stream, tc.objects...)

			require.NoError(t, err)

			for fieldName, expected := range tc.expected {
				// Check if the field exists in the stream's schema
				property, found := stream.Schema.Properties.Load(fieldName)
				assert.True(t, found, "Field %s should exist in schema", fieldName)

				if found {
					prop := property.(*types.Property)
					typeSet := *(prop.Type)

					// Check field type
					assert.True(t, typeSet.Exists(expected.dataType),
						"Field %s should have type %s, got %v",
						fieldName, expected.dataType, typeSet)

					// Check nullability
					if expected.nullable {
						assert.True(t, typeSet.Exists(types.Null),
							"Field %s should be nullable", fieldName)
					} else {
						assert.False(t, typeSet.Exists(types.Null),
							"Field %s should not be nullable", fieldName)
					}
				}
			}
		})
	}
}

// TestResolveWithEmptyInput tests the edge case of resolving with no objects
func TestResolveWithEmptyInput(t *testing.T) {
	stream := types.NewStream("empty_stream", "test_namespace")

	err := Resolve(stream, []map[string]interface{}{}...)

	assert.NoError(t, err)

	assert.NotNil(t, stream.Schema)

	count := 0
	stream.Schema.Properties.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "Expected empty schema")
}
