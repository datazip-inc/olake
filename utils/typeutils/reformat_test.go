package typeutils

import (
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReformat_GetFirstNotNullType(t *testing.T) {
	tests := []struct {
		name   string
		input  []types.DataType
		output types.DataType
	}{
		{
			name:   "single non-null type",
			input:  []types.DataType{types.String},
			output: types.String,
		},
		{
			name:   "first non-null type mixed array",
			input:  []types.DataType{types.Null, types.Int32, types.String},
			output: types.Int32,
		},
		{
			name:   "all null types",
			input:  []types.DataType{types.Null, types.Null, types.Null},
			output: types.Null,
		},
		{
			name:   "empty array",
			input:  []types.DataType{},
			output: types.Null,
		},
		{
			name:   "null followed by multiple values",
			input:  []types.DataType{types.Null, types.Bool, types.Int64, types.Float64},
			output: types.Bool,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getFirstNotNullType(tc.input)
			assert.Equal(t, tc.output, result)
		})
	}
}

func TestReformat_ReformatValue(t *testing.T) {
	tests := []struct {
		name         string
		datatypes    types.DataType
		value        any
		output       any
		outputErr    bool
		outputErrMsg string
	}{
		{
			name:         "null type returns errors",
			datatypes:    types.Null,
			value:        "any value",
			output:       nil,
			outputErr:    true,
			outputErrMsg: "null value",
		},
		{
			name:      "nil type returns nil",
			datatypes: types.String,
			value:     nil,
			output:    nil,
			outputErr: false,
		},
		// Bool type tests
		{
			name:      "bool type from bool",
			datatypes: types.Bool,
			value:     true,
			output:    true,
			outputErr: false,
		},
		{
			name:      "bool type from string true",
			datatypes: types.Bool,
			value:     true,
			output:    true,
			outputErr: false,
		},
		{
			name:      "bool type from string false",
			datatypes: types.Bool,
			value:     false,
			output:    false,
			outputErr: false,
		},
		{
			name:      "bool type from int 0",
			datatypes: types.Bool,
			value:     0,
			output:    false,
			outputErr: false,
		},
		{
			name:      "bool type from int 1",
			datatypes: types.Bool,
			value:     1,
			output:    true,
			outputErr: false,
		},
		// Int 64 type tests
		{
			name:      "int64 type from int64",
			datatypes: types.Int64,
			value:     int64(42),
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from int32",
			datatypes: types.Int64,
			value:     int64(42),
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from int",
			datatypes: types.Int64,
			value:     int64(42),
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from int64",
			datatypes: types.Int64,
			value:     int64(42),
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from int32",
			datatypes: types.Int64,
			value:     int32(42),
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from string",
			datatypes: types.Int64,
			value:     "42",
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from float64",
			datatypes: types.Int64,
			value:     float64(42.7),
			output:    int64(42),
			outputErr: false,
		},
		{
			name:      "int64 type from bool true",
			datatypes: types.Int64,
			value:     true,
			output:    int64(1),
			outputErr: false,
		},
		// Int32 type tests
		{
			name:      "int32 type from int32",
			datatypes: types.Int32,
			value:     int32(42),
			output:    int32(42),
			outputErr: false,
		},
		{
			name:      "int32 type from int64",
			datatypes: types.Int32,
			value:     int64(42),
			output:    int32(42),
			outputErr: false,
		},
		{
			name:      "int32 type from string",
			datatypes: types.Int32,
			value:     "42",
			output:    int32(42),
			outputErr: false,
		},
		{
			name:      "int32 type from []uint8 single byte",
			datatypes: types.Int32,
			value:     []uint8{42},
			output:    int32(42),
			outputErr: false,
		},
		// float64 type tests
		{
			name:      "float64 type from float64",
			datatypes: types.Float64,
			value:     float64(3.14),
			output:    float64(3.14),
			outputErr: false,
		},
		{
			name:      "float64 type from float32",
			datatypes: types.Float64,
			value:     float32(3.14),
			output:    float64(float32(3.14)),
			outputErr: false,
		},
		{
			name:      "float64 type from int",
			datatypes: types.Float64,
			value:     42,
			output:    float64(42),
			outputErr: false,
		},
		{
			name:      "float64 type from string",
			datatypes: types.Float64,
			value:     "3.14",
			output:    float64(3.14),
			outputErr: false,
		},
		{
			name:      "float64 type from bool true",
			datatypes: types.Float64,
			value:     true,
			output:    float64(1.0),
			outputErr: false,
		},
		// float32 type tests
		{
			name:      "float32 type from float32",
			datatypes: types.Float32,
			value:     float32(3.14),
			output:    float32(3.14),
			outputErr: false,
		},
		{
			name:      "float32 type from float64",
			datatypes: types.Float32,
			value:     float64(3.14),
			output:    float32(3.14),
			outputErr: false,
		},
		{
			name:      "float32 type from string",
			datatypes: types.Float32,
			value:     "3.14",
			output:    float32(3.14),
			outputErr: false,
		},
		// string type tests
		{
			name:      "string type from string",
			datatypes: types.String,
			value:     "hello",
			output:    "hello",
			outputErr: false,
		},
		{
			name:      "string type from int",
			datatypes: types.String,
			value:     42,
			output:    "42",
			outputErr: false,
		},
		{
			name:      "string type from bool",
			datatypes: types.String,
			value:     true,
			output:    "true",
			outputErr: false,
		},
		{
			name:      "string type from []byte",
			datatypes: types.String,
			value:     []byte("hello"),
			output:    "hello",
			outputErr: false,
		},
		// Timestamp type tests
		{
			name:      "timestamps type from time.Time",
			datatypes: types.Timestamp,
			value:     time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			output:    time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			outputErr: false,
		},
		{
			name:      "timestamps type from String",
			datatypes: types.Timestamp,
			value:     "2023-01-01T12:00:00Z",
			output:    time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			outputErr: false,
		},
		{
			name:      "timestamps type from int64 unix",
			datatypes: types.Timestamp,
			value:     int64(1672574400),
			output:    time.Unix(1672574400, 0),
			outputErr: false,
		},
		// Array type tests
		{
			name:      "array type from []any",
			datatypes: types.Array,
			value:     []any{1, 2, 3},
			output:    []any{1, 2, 3},
			outputErr: false,
		},
		{
			name:      "array type from single value",
			datatypes: types.Array,
			value:     42,
			output:    []any{42},
			outputErr: false,
		},
		// Unknown type tests
		{
			name:      "unkown type passes through",
			datatypes: types.Unknown,
			value:     "some value",
			output:    "some value",
			outputErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatValue(tc.datatypes, tc.value)

			if tc.outputErr {
				assert.Error(t, err)
				if tc.outputErrMsg != "" {
					assert.Contains(t, err.Error(), tc.outputErrMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.output, result)
			}
		})
	}
}

func TestReformat_ReformatValueOnDataTypes(t *testing.T) {
	tests := []struct {
		name      string
		datatypes []types.DataType
		value     any
		output    any
	}{
		{
			name:      "uses first null type",
			datatypes: []types.DataType{types.Null, types.Int64, types.String},
			value:     "42",
			output:    int64(42),
		},
		{
			name:      "all null types",
			datatypes: []types.DataType{types.Null, types.Null},
			value:     "23",
			output:    nil,
		},
		{
			name:      "single type",
			datatypes: []types.DataType{types.String},
			value:     42,
			output:    "42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatValueOnDataTypes(tc.datatypes, tc.value)
			if tc.output == nil {
				assert.Error(t, err)
				assert.Equal(t, ErrNullValue, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.output, result)
			}
		})
	}
}

func TestReformat_ReformatRecord(t *testing.T) {
	tests := []struct {
		name         string
		fields       Fields
		record       types.Record
		output       types.Record
		outputErr    bool
		outputErrMsg string
	}{
		{
			name: "sucessful reformat",
			fields: Fields{
				"id":    NewField(types.Int64),
				"name":  NewField(types.String),
				"score": NewField(types.Float64),
			},
			record: types.Record{
				"id":    "42",
				"name":  "test",
				"score": "42.33",
			},
			output: types.Record{
				"id":    int64(42),
				"name":  "test",
				"score": float64(42.33),
			},
			outputErr: false,
		},
		{
			name: "missing field error",
			fields: Fields{
				"id": NewField(types.Int64),
			},
			record: types.Record{
				"id":   "42",
				"name": "test",
			},
			outputErr:    true,
			outputErrMsg: "missing field",
		},
		{
			name: "empty record",
			fields: Fields{
				"id": NewField(types.Int64),
			},
			record:    types.Record{},
			output:    types.Record{},
			outputErr: false,
		},
		{
			name: "bool conversion",
			fields: Fields{
				"active": NewField(types.Bool),
			},
			record: types.Record{
				"active": true,
			},
			output: types.Record{
				"active": true,
			},
			outputErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ReformatRecord(tc.fields, tc.record)
			if tc.outputErr {
				assert.Error(t, err)
				if tc.outputErrMsg != "" {
					assert.Contains(t, err.Error(), tc.outputErrMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.output, tc.record)
			}
		})
	}
}

func TestReformat_ReformatBool(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		expected    any
		expectedErr bool
	}{
		// Bool values
		{
			name:        "bool true",
			value:       true,
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "bool false",
			value:       false,
			expected:    false,
			expectedErr: false,
		},
		// String values
		{
			name:        "string TRUE",
			value:       "TRUE",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string FALSE",
			value:       "FALSE",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string True",
			value:       "True",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string False",
			value:       "False",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string true",
			value:       "true",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string false",
			value:       "false",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string 1",
			value:       "1",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string 0",
			value:       "0",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string t",
			value:       "t",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string f",
			value:       "f",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string T",
			value:       "T",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string F",
			value:       "F",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string YES",
			value:       "YES",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string NO",
			value:       "NO",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string yes",
			value:       "yes",
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "string no",
			value:       "no",
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "string invalid",
			value:       "maybe",
			expectedErr: true,
		},
		// Integer values
		{
			name:        "int 1",
			value:       1,
			expected:    true,
			expectedErr: false,
		},
		{
			name:        "int 0",
			value:       0,
			expected:    false,
			expectedErr: false,
		},
		{
			name:        "int 2",
			value:       2,
			expectedErr: true,
		},
		// Invalid types
		{
			name:        "float64",
			value:       float64(1.0),
			expectedErr: true,
		},
		{
			name:        "nil",
			value:       nil,
			expectedErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatBool(tc.value)
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestReformat_ReformatInt64(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  int64
		expectErr bool
	}{
		// Integer types
		{
			name:      "int64 value",
			value:     int64(42),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "int32 value",
			value:     int32(42),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "int8 value",
			value:     int8(42),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "int value",
			value:     42,
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "int16 value",
			value:     int16(42),
			expected:  int64(42),
			expectErr: false,
		},
		// unsigned int
		{
			name:      "uint16 value",
			value:     uint16(42),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "uint32 value",
			value:     uint32(42),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "uint64 value",
			value:     uint64(42),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "uint8 value",
			value:     uint8(42),
			expected:  int64(42),
			expectErr: false,
		},
		// float types
		{
			name:      "float32 value",
			value:     float32(42.3),
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "float64 value",
			value:     float64(42.3),
			expected:  int64(42),
			expectErr: false,
		},
		// bool types
		{
			name:      "bool true",
			value:     true,
			expected:  int64(1),
			expectErr: false,
		},
		{
			name:      "bool false",
			value:     false,
			expected:  int64(0),
			expectErr: false,
		},
		// string types
		{
			name:      "string positive numbers",
			value:     "42",
			expected:  int64(42),
			expectErr: false,
		},
		{
			name:      "string with negative numbers",
			value:     "-42",
			expected:  int64(-42),
			expectErr: false,
		},
		{
			name:      "string invalid",
			value:     "no number",
			expectErr: true,
		},
		// pointer types
		{
			name: "pointer to any",
			value: func() *any {
				v := any(42)
				return &v
			}(),
			expected:  int64(42),
			expectErr: false,
		},
		// invalid types
		{
			name:      "nil",
			value:     nil,
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatInt64(tc.value)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestReformat_ReformatInt32(t *testing.T) {
	tests := []struct {
		name      string
		value     any
		expected  int32
		expectErr bool
	}{
		// Integer types
		{
			name:      "int32 value",
			value:     int32(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "int64 value",
			value:     int64(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "int8 value",
			value:     int8(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "int value",
			value:     42,
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "int16 value",
			value:     int16(42),
			expected:  int32(42),
			expectErr: false,
		},
		// unsigned int
		{
			name:      "uint value",
			value:     uint(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "uint16 value",
			value:     uint16(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "uint32 value",
			value:     uint32(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "uint64 value",
			value:     uint64(42),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "uint8 value",
			value:     uint8(42),
			expected:  int32(42),
			expectErr: false,
		},
		// float types
		{
			name:      "float32 value",
			value:     float32(42.3),
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "float64 value",
			value:     float64(42.3),
			expected:  int32(42),
			expectErr: false,
		},
		// bool types
		{
			name:      "bool true",
			value:     true,
			expected:  int32(1),
			expectErr: false,
		},
		{
			name:      "bool false",
			value:     false,
			expected:  int32(0),
			expectErr: false,
		},
		// string types
		{
			name:      "string positive numbers",
			value:     "42",
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "string with negative numbers",
			value:     "-42",
			expected:  int32(-42),
			expectErr: false,
		},
		{
			name:      "string invalid",
			value:     "no number",
			expectErr: true,
		},
		// []uint8
		{
			name:      "[]uint8 single byte",
			value:     []uint8{42},
			expected:  int32(42),
			expectErr: false,
		},
		{
			name:      "[]uint8 multiple bytes",
			value:     []uint8{42, 43},
			expectErr: true,
		},
		// pointer types
		{
			name: "pointer to any",
			value: func() *any {
				v := any(int32(42))
				return &v
			}(),
			expected:  int32(42),
			expectErr: false,
		},
		// invalid types
		{
			name:      "nil",
			value:     nil,
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatInt32(tc.value)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestReformat_ReformatFloat64(t *testing.T) {
	testCases := []struct {
		name        string
		value       any
		expected    float64
		expectError bool
	}{
		// Float types
		{
			name:        "float64 value",
			value:       float64(3.14),
			expected:    float64(3.14),
			expectError: false,
		},
		{
			name:        "float32 value",
			value:       float32(3.14),
			expected:    float64(3.14),
			expectError: false,
		},
		// Integer types
		{
			name:        "int value",
			value:       42,
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "int8 value",
			value:       int8(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "int16 value",
			value:       int16(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "int32 value",
			value:       int32(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "int64 value",
			value:       int64(42),
			expected:    float64(42),
			expectError: false,
		},
		// Unsigned integer types
		{
			name:        "uint value",
			value:       uint(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "uint8 value",
			value:       uint8(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "uint16 value",
			value:       uint16(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "uint32 value",
			value:       uint32(42),
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "uint64 value",
			value:       uint64(42),
			expected:    float64(42),
			expectError: false,
		},
		// Bool types
		{
			name:        "bool true",
			value:       true,
			expected:    float64(1.0),
			expectError: false,
		},
		{
			name:        "bool false",
			value:       false,
			expected:    float64(0.0),
			expectError: false,
		},
		// String types
		{
			name:        "string positive number",
			value:       "3.14",
			expected:    float64(3.14),
			expectError: false,
		},
		{
			name:        "string negative number",
			value:       "-3.14",
			expected:    float64(-3.14),
			expectError: false,
		},
		{
			name:        "string integer",
			value:       "42",
			expected:    float64(42),
			expectError: false,
		},
		{
			name:        "string invalid",
			value:       "not a number",
			expectError: true,
		},
		// []uint8
		{
			name:        "[]uint8 number string",
			value:       []uint8("3.14"),
			expected:    float64(3.14),
			expectError: false,
		},
		{
			name:        "[]uint8 invalid",
			value:       []uint8("invalid"),
			expectError: true,
		},
		// Invalid types
		{
			name:        "nil",
			value:       nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ReformatFloat64(tc.value)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tc.expected, result, 0.0001, "Float values should be approximately equal")
			}
		})
	}
}

