package typeutils

import (
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

const (
	testUserID  = 12345
	testName    = "John Doe"
	testAge     = 30
	testEnabled = true
	testScore   = 92.5
)

var testTimestamp = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

// TestFlattenerFlatten tests the Flatten method of Flattener interface
func TestFlattenerFlatten(t *testing.T) {
	tests := []struct {
		name        string
		input       types.Record
		expected    types.Record
		expectError bool
	}{
		{
			name: "flatten nested map",
			input: types.Record{
				"user": map[string]interface{}{
					"name":  testName,
					"age":   testAge,
					"admin": testEnabled,
				},
				"stats": []int{1, 2, 3, 4, 5},
				"metadata": map[string]interface{}{
					"created_at": testTimestamp,
				},
			},
			expected: types.Record{
				"user":     `{"admin":true,"age":30,"name":"John Doe"}`,
				"stats":    `[1,2,3,4,5]`,
				"metadata": `{"created_at":"2023-01-01T00:00:00Z"}`,
			},
			expectError: false,
		},
		{
			name: "flatten simple values",
			input: types.Record{
				"name":    testName,
				"age":     testAge,
				"enabled": testEnabled,
				"score":   testScore,
			},
			expected: types.Record{
				"name":    testName,
				"age":     testAge,
				"enabled": testEnabled,
				"score":   testScore,
			},
			expectError: false,
		},
		{
			name: "flatten with special characters in keys",
			input: types.Record{
				"User@Name": testName,
				"user-id":   testUserID,
				"is.admin":  testEnabled,
			},
			expected: types.Record{
				"user_name": testName,
				"user_id":   testUserID,
				"is_admin":  testEnabled,
			},
			expectError: false,
		},
		{
			name: "flatten with time value",
			input: types.Record{
				"timestamp": testTimestamp,
			},
			expected: types.Record{
				"timestamp": testTimestamp,
			},
			expectError: false,
		},
		{
			name: "flatten with nil value",
			input: types.Record{
				"nullable": nil,
			},
			expected:    types.Record{},
			expectError: false,
		},
		{
			name: "flatten deeply nested maps with mixed types",
			input: types.Record{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"personal": map[string]interface{}{
							"name": testName,
							"age":  testAge,
							"address": map[string]interface{}{
								"city":    "New York",
								"zipcode": 10001,
								"details": map[string]interface{}{
									"street": "test street",
									"apt":    "5B",
								},
							},
						},
						"preferences": map[string]interface{}{
							"notifications": true,
							"theme":         "dark",
						},
					},
					"stats": map[string]interface{}{
						"visits": []int{1, 2, 3},
						"score":  testScore,
					},
				},
			},
			expected: types.Record{
				"user": `{"profile":{"personal":{"address":{"city":"New York","details":{"apt":"5B","street":"test street"},"zipcode":10001},"age":30,"name":"John Doe"},"preferences":{"notifications":true,"theme":"dark"}},"stats":{"score":92.5,"visits":[1,2,3]}}`,
			},
			expectError: false,
		},
		{
			name: "flatten nested maps with arrays and timestamps",
			input: types.Record{
				"data": map[string]interface{}{
					"events": []map[string]interface{}{
						{
							"timestamp": testTimestamp,
							"type":      "login",
							"details": map[string]interface{}{
								"ip":      "192.168.1.1",
								"success": true,
							},
						},
						{
							"timestamp": testTimestamp.Add(time.Hour),
							"type":      "logout",
							"details": map[string]interface{}{
								"duration": 3600,
							},
						},
					},
					"metadata": map[string]interface{}{
						"version": "1.0",
						"tags":    []string{"test", "nested"},
					},
				},
			},
			expected: types.Record{
				"data": `{"events":[{"details":{"ip":"192.168.1.1","success":true},"timestamp":"2023-01-01T00:00:00Z","type":"login"},{"details":{"duration":3600},"timestamp":"2023-01-01T01:00:00Z","type":"logout"}],"metadata":{"tags":["test","nested"],"version":"1.0"}}`,
			},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flattener := NewFlattener()

			result, err := flattener.Flatten(tc.input)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestFlattenerImplFlatten tests the internal flatten method
func TestFlattenerImplFlatten(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		value       any
		expected    types.Record
		expectError bool
	}{
		{
			name:  "test slice flattening",
			key:   "testArray",
			value: []string{"a", "b", "c"},
			expected: types.Record{
				"testarray": `["a","b","c"]`,
			},
			expectError: false,
		},
		{
			name: "test map flattening",
			key:  "testMap",
			value: map[string]interface{}{
				"nested_1": "value",
				"nested_2": "value2",
			},
			expected: types.Record{
				"testmap": `{"nested_1":"value","nested_2":"value2"}`,
			},
			expectError: false,
		},
		{
			name: "test primitive types",
			key:  "testPrimitive",
			value: map[string]interface{}{
				"string":  "hello",
				"int":     42,
				"float":   3.14,
				"boolean": true,
			},
			expected: types.Record{
				"testprimitive": `{"boolean":true,"float":3.14,"int":42,"string":"hello"}`,
			},
			expectError: false,
		},
		{
			name:  "test time value",
			key:   "testTime",
			value: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: types.Record{
				"testtime": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			expectError: false,
		},
		{
			name:        "test nil value with omitNilValues true",
			key:         "testNil",
			value:       nil,
			expected:    types.Record{},
			expectError: false,
		},
		{
			name:  "test special characters in key",
			key:   "Test@K-e_y#123",
			value: "value",
			expected: types.Record{
				"test_k_e_y_123": "value",
			},
			expectError: false,
		},
		{
			name: "test deeply nested map with multiple levels",
			key:  "deeplyNested",
			value: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": map[string]interface{}{
							"level4": map[string]interface{}{
								"final": "value",
								"array": []int{1, 2, 3},
								"nested": map[string]interface{}{
									"key": "value",
								},
							},
						},
					},
				},
			},
			expected: types.Record{
				"deeplynested": `{"level1":{"level2":{"level3":{"level4":{"array":[1,2,3],"final":"value","nested":{"key":"value"}}}}}}`,
			},
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flattener := NewFlattener()
			destination := make(types.Record)

			err := flattener.(*FlattenerImpl).flatten(tc.key, tc.value, destination)

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, destination)
			}
		})
	}
}
