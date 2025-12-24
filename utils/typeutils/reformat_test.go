package typeutils

import (
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
)

func TestGetFirstNotNullType(t *testing.T) {
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

func TestReformatValue(t *testing.T) {
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
			output:    true,
			outputErr: false,
		},
		{
			name:      "bool type from int 1",
			datatypes: types.Bool,
			value:     1,
			output:    false,
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
			output:    float64(3.14),
			outputErr: false,
		},
		{
			name:      "float64 type from int",
			datatypes: types.Float64,
			value:     42,
			output:    float64(3.14),
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
