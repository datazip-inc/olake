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
	testScore   = 92.52361
)

var testTimestamp = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

// TestFlatten tests the public Flatten method of FlattenerImpl
func TestFlatten(t *testing.T) {
	tests := []struct {
		name        string
		input       types.Record
		expected    types.Record
		expectError bool
	}{
		// empty record, nothing to flatten
		{
			name:        "empty record",
			input:       types.Record{},
			expected:    types.Record{},
			expectError: false,
		},
		// basic values like string, int, bool, float, time, etc.
		{
			name: "simple values",
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
		// map contains values of different types
		{
			name: "nested map",
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
		// values with nil are omitted from the result
		{
			name: "nil values are omitted",
			input: types.Record{
				"valid":   "value",
				"invalid": nil,
				"number":  42,
			},
			expected: types.Record{
				"valid":  "value",
				"number": 42,
			},
			expectError: false,
		},
		// if key contains special characters, it is replaced with an underscore
		{
			name: "special characters in keys",
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
		// time values are present in record
		{
			name: "time values",
			input: types.Record{
				"timestamp":  testTimestamp,
				"created_at": time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
				"zero_time":  time.Time{},
			},
			expected: types.Record{
				"timestamp":  testTimestamp,
				"created_at": time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
				"zero_time":  time.Time{},
			},
			expectError: false,
		},
		// values nesterd inside map, the whole map is stringified
		{
			name: "deeply nested maps",
			input: types.Record{
				"user": map[string]interface{}{
					"profile": map[string]interface{}{
						"personal": map[string]interface{}{
							"name": testName,
							"age":  testAge,
						},
						"preferences": map[string]interface{}{
							"notifications": true,
							"theme":         "dark",
						},
					},
				},
			},
			expected: types.Record{
				"user": `{"profile":{"personal":{"age":30,"name":"John Doe"},"preferences":{"notifications":true,"theme":"dark"}}}`,
			},
			expectError: false,
		},
		// array which contains data of various data types is stringified
		{
			name: "arrays with mixed types",
			input: types.Record{
				"mixed_array": []interface{}{
					"string",
					42,
					3.14,
					true,
					nil,
					[]int{1, 2, 3},
					map[string]interface{}{"key": "value"},
					time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			expected: types.Record{
				"mixed_array": `["string",42,3.14,true,null,[1,2,3],{"key":"value"},"2023-01-01T00:00:00Z"]`,
			},
			expectError: false,
		},
		// unicode and special characters in column names are replaced with an underscore
		{
			name: "unicode and special characters",
			input: types.Record{
				"🚀rocket🎯target":  "🌟success",
				"key with spaces": "value with\nnewlines\tand\ttabs",
			},
			expected: types.Record{
				"_rocket_target":  "🌟success",
				"key_with_spaces": "value with\nnewlines\tand\ttabs",
			},
			expectError: false,
		},
		//  this is to check if lager numbers are loosing precision and scale.
		{
			name: "large numbers",
			input: types.Record{
				"numbers": map[string]interface{}{
					"max_int64":  int64(9223372036854775807),
					"min_int64":  int64(-9223372036854775808),
					"max_uint64": uint64(18446744073709551615),
					"very_large": float64(1e20),
					"very_small": float64(1e-20),
					"pi":         3.141592653589793,
				},
			},
			expected: types.Record{
				"numbers": `{"max_int64":9223372036854775807,"max_uint64":18446744073709551615,"min_int64":-9223372036854775808,"pi":3.141592653589793,"very_large":100000000000000000000,"very_small":1e-20}`,
			},
			expectError: false,
		},
		// various types of values but all are empty are stringified as empty strings, slices, maps, etc.
		{
			name: "all empty values",
			input: types.Record{
				"empty_string": "",
				"empty_slice":  []interface{}{},
				"empty_map":    map[string]interface{}{},
				"zero":         0,
				"false":        false,
			},
			expected: types.Record{
				"empty_string": "",
				"empty_slice":  `[]`,
				"empty_map":    `{}`,
				"zero":         0,
				"false":        false,
			},
			expectError: false,
		},
		// complex pointer scenarios are stringified as null, map, slice, struct
		{
			name: "complex pointer scenarios",
			input: types.Record{
				"pointers": map[string]interface{}{
					"nil_pointer":       (*string)(nil),
					"pointer_to_struct": &struct{ Name string }{"test"},
					"pointer_to_slice":  &[]int{1, 2, 3},
					"pointer_to_map":    &map[string]int{"a": 1, "b": 2},
				},
			},
			expected: types.Record{
				"pointers": `{"nil_pointer":null,"pointer_to_map":{"a":1,"b":2},"pointer_to_slice":[1,2,3],"pointer_to_struct":{"Name":"test"}}`,
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
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestFlattenInternal tests the private flatten method of FlattenerImpl
func TestFlattenInternal(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		value       interface{}
		expected    types.Record
		expectError bool
	}{
		{
			name:  "slice of strings",
			key:   "testArray",
			value: []string{"a", "b", "c"},
			expected: types.Record{
				"testarray": `["a","b","c"]`,
			},
			expectError: false,
		},
		{
			name:  "slice of integers",
			key:   "numbers",
			value: []int{1, 2, 3, 4, 5},
			expected: types.Record{
				"numbers": `[1,2,3,4,5]`,
			},
			expectError: false,
		},
		{
			name:  "empty slice",
			key:   "empty",
			value: []interface{}{},
			expected: types.Record{
				"empty": `[]`,
			},
			expectError: false,
		},
		{
			name:  "nil slice",
			key:   "nil_slice",
			value: []interface{}(nil),
			expected: types.Record{
				"nil_slice": `null`,
			},
			expectError: false,
		},
		{
			name:  "multi dimensional slice",
			key:   "nested",
			value: [][]int{{1, 2}, {3, 4}},
			expected: types.Record{
				"nested": `[[1,2],[3,4]]`,
			},
			expectError: false,
		},
		{
			name: "simple map",
			key:  "testMap",
			value: map[string]interface{}{
				"nested_1": "value1",
				"nested_2": "value2",
			},
			expected: types.Record{
				"testmap": `{"nested_1":"value1","nested_2":"value2"}`,
			},
			expectError: false,
		},
		{
			name:  "empty map",
			key:   "empty_map",
			value: map[string]interface{}{},
			expected: types.Record{
				"empty_map": `{}`,
			},
			expectError: false,
		},
		{
			name: "nested map",
			key:  "nested_map",
			value: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"value": "deep",
					},
				},
			},
			expected: types.Record{
				"nested_map": `{"level1":{"level2":{"value":"deep"}}}`,
			},
			expectError: false,
		},
		{
			name:  "string",
			key:   "str",
			value: "hello world",
			expected: types.Record{
				"str": "hello world",
			},
			expectError: false,
		},
		{
			name:  "boolean true",
			key:   "bool_true",
			value: true,
			expected: types.Record{
				"bool_true": true,
			},
			expectError: false,
		},
		{
			name:  "boolean false",
			key:   "bool_false",
			value: false,
			expected: types.Record{
				"bool_false": false,
			},
			expectError: false,
		},
		{
			name:  "int",
			key:   "integer",
			value: 42,
			expected: types.Record{
				"integer": 42,
			},
			expectError: false,
		},
		{
			name:  "int64",
			key:   "big_int",
			value: int64(-9223372036854775807),
			expected: types.Record{
				"big_int": int64(-9223372036854775807),
			},
			expectError: false,
		},
		{
			name:  "float64",
			key:   "float",
			value: 343562342.141594523545434341,
			expected: types.Record{
				"float": 343562342.1415945235454341,
			},
			expectError: false,
		},
		{
			name:  "uint64",
			key:   "unsigned",
			value: uint64(18446744073709551615),
			expected: types.Record{
				"unsigned": uint64(18446744073709551615),
			},
			expectError: false,
		},
		{
			name:  "time value",
			key:   "timestamp",
			value: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: types.Record{
				"timestamp": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			expectError: false,
		},
		{
			name:  "zero time",
			key:   "zero_time",
			value: time.Time{},
			expected: types.Record{
				"zero_time": time.Time{},
			},
			expectError: false,
		},
		{
			name:        "nil value is omitted",
			key:         "nil_value",
			value:       nil,
			expected:    types.Record{},
			expectError: false,
		},
		{
			name:  "special characters in key",
			key:   "Test@K-e_y#123",
			value: "value",
			expected: types.Record{
				"test_k_e_y_123": "value",
			},
			expectError: false,
		},
		{
			name:  "unicode in key",
			key:   "用户@名称#123",
			value: "test user",
			expected: types.Record{
				"______123": "test user",
			},
			expectError: false,
		},
		{
			name:  "spaces in key",
			key:   "key with spaces",
			value: "value",
			expected: types.Record{
				"key_with_spaces": "value",
			},
			expectError: false,
		},
		{
			name:  "interface with string",
			key:   "interface_str",
			value: interface{}("test"),
			expected: types.Record{
				"interface_str": "test",
			},
			expectError: false,
		},
		{
			name:  "interface with number",
			key:   "interface_num",
			value: interface{}(42),
			expected: types.Record{
				"interface_num": 42,
			},
			expectError: false,
		},
		{
			name: "struct value",
			key:  "struct",
			value: struct {
				Name string
				Age  int
			}{"John", 30},
			expected: types.Record{
				"struct": "{John 30}",
			},
			expectError: false,
		},
		{
			name: "all numeric types",
			key:  "all_numbers",
			value: map[string]interface{}{
				"int":     int(42),
				"int8":    int8(-42),
				"int16":   int16(-42),
				"int32":   int32(423425),
				"int64":   int64(42425463733234),
				"uint":    uint(42),
				"uint8":   uint8(42),
				"uint16":  uint16(42),
				"uint32":  uint32(42),
				"uint64":  uint64(42),
				"float32": float32(3.14),
				"float64": float64(3.14),
			},
			expected: types.Record{
				"all_numbers": `{"float32":3.14,"float64":3.14,"int":42,"int16":-42,"int32":423425,"int64":42425463733234,"int8":-42,"uint":42,"uint16":42,"uint32":42,"uint64":42,"uint8":42}`,
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
