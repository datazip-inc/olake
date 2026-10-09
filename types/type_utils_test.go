package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedBinaryOf(t *testing.T) {
	require.Equal(t, DataType("fixed_binary(16)"), FixedBinaryOf(16))
	require.Equal(t, FixedBinary, BaseOf(FixedBinaryOf(16)))
}

func TestFixedBinaryGrammar(t *testing.T) {
	testCases := []struct {
		dataType DataType
		length   int
		ok       bool
	}{
		{
			dataType: FixedBinaryOf(1),
			length:   1,
			ok:       true,
		},
		{
			dataType: FixedBinaryOf(16),
			length:   16,
			ok:       true,
		},
		{
			dataType: DataType("fixed_binary(4096)"),
			length:   4096,
			ok:       true,
		},
		{
			dataType: FixedBinary,
			length:   0,
			ok:       false,
		},
		{
			dataType: Binary,
			length:   0,
			ok:       false,
		},
		{
			dataType: DataType("fixed_binary()"),
			length:   0,
			ok:       false,
		},
		{
			dataType: DataType("fixed_binary(0)"),
			length:   0,
			ok:       false,
		},
		{
			dataType: DataType("fixed_binary(-1)"),
			length:   0,
			ok:       false,
		},
		{
			dataType: DataType("fixed_binary(1.5)"),
			length:   0,
			ok:       false,
		},
		{
			dataType: DataType("fixed_binary(99999999999999999999)"),
			length:   0,
			ok:       false,
		},
		{
			dataType: DataType(" fixed_binary(16)"),
			length:   0,
			ok:       false,
		},
		{
			dataType: String,
			length:   0,
			ok:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.dataType), func(t *testing.T) {
			width, ok := FixedBinaryWidth(tc.dataType)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.length, width)
		})
	}
}

func TestBaseOf(t *testing.T) {
	require.Equal(t, FixedBinary, BaseOf(FixedBinaryOf(16)))
	require.Equal(t, FixedBinary, BaseOf(FixedBinary))
	for _, dataType := range append(declaredDataTypes(t), Null, Unknown, DataType("undeclared_type")) {
		require.Equal(t, dataType, BaseOf(dataType), "%s must be its own base", dataType)
	}
}

func TestDataTypeOf(t *testing.T) {
	testCases := []struct {
		pattern DataType
		params  []any
		want    DataType
	}{
		{
			pattern: FixedBinary,
			params:  []any{16},
			want:    FixedBinaryOf(16),
		},
		{
			pattern: DataType("decimal(%d,%d)"),
			params:  []any{9, 2},
			want:    DataType("decimal(9,2)"),
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.want), func(t *testing.T) {
			require.Equal(t, tc.want, tc.pattern.Of(tc.params...))
		})
	}
}

func TestValueType(t *testing.T) {
	testCases := []struct {
		dataType  DataType
		valueType DataType
	}{
		{
			dataType:  FixedBinaryOf(16),
			valueType: Binary,
		},
		{
			dataType:  FixedBinary,
			valueType: Binary,
		},
		{
			dataType:  Binary,
			valueType: Binary,
		},
		{
			dataType:  String,
			valueType: String,
		},
		{
			dataType:  DataType("fixed_binary(0)"),
			valueType: Binary,
		},
		{
			dataType:  DataType("undeclared(1)"),
			valueType: DataType("undeclared(1)"),
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.dataType), func(t *testing.T) {
			require.Equal(t, tc.valueType, tc.dataType.ValueType())
		})
	}
}

func TestIsIcebergBytes(t *testing.T) {
	icebergCases := []struct {
		icebergType string
		isBytes     bool
		isFixed     bool
		width       int
	}{
		{
			icebergType: "fixed[16]",
			isBytes:     true,
			isFixed:     true,
			width:       16,
		},
		{
			icebergType: "binary",
			isBytes:     true,
			isFixed:     false,
			width:       0,
		},
		{
			icebergType: "string",
			isBytes:     false,
			isFixed:     false,
			width:       0,
		},
		{
			icebergType: "fixed[0]",
			isBytes:     true,
			isFixed:     true,
			width:       0,
		},
		{
			icebergType: "fixed[-4]",
			isBytes:     true,
			isFixed:     true,
			width:       0,
		},
		{
			icebergType: "fixed[16",
			isBytes:     true,
			isFixed:     true,
			width:       0,
		},
	}

	for _, tc := range icebergCases {
		t.Run("iceberg "+tc.icebergType, func(t *testing.T) {
			isBytes, isFixed := IsIcebergBytes(tc.icebergType)
			require.Equal(t, tc.isBytes, isBytes)
			require.Equal(t, tc.isFixed, isFixed)
			width, ok := IcebergFixedWidth(tc.icebergType)
			require.Equal(t, tc.width, width)
			require.Equal(t, tc.width > 0, ok)
		})
	}
}

func TestTypeFamilyTwoParameters(t *testing.T) {
	decimal := newTypeFamily(DataType("decimal(%d,%d)"),
		func(p []int) bool { return p[0] > 0 && p[1] >= 0 && p[1] <= p[0] })
	require.Equal(t, 2, decimal.parity)
	require.Equal(t, DataType("decimal(9,2)"), decimal.instance([]int{9, 2}))

	parseCases := []struct {
		input  string
		params []int
		ok     bool
	}{
		{
			input:  "decimal(9,2)",
			params: []int{9, 2},
			ok:     true,
		},
		{
			input:  "decimal(9, 2)",
			params: nil,
			ok:     false,
		},
		{
			input:  "decimal(38,0)",
			params: []int{38, 0},
			ok:     true,
		},
		{
			input:  "decimal(9)",
			params: nil,
			ok:     false,
		},
		{
			input:  "decimal(2,9)",
			params: nil,
			ok:     false,
		},
		{
			input:  "decimal(0,0)",
			params: nil,
			ok:     false,
		},
		{
			input:  "decimal(9,-1)",
			params: nil,
			ok:     false,
		},
		{
			input:  "decimal(9,2,3)",
			params: nil,
			ok:     false,
		},
		{
			input:  "decimal",
			params: nil,
			ok:     false,
		},
	}

	for _, tc := range parseCases {
		t.Run(tc.input, func(t *testing.T) {
			params, ok := decimal.parse(tc.input)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.params, params)
		})
	}
}

func TestFixedBinaryWidthsMeetAtBinary(t *testing.T) {
	require.Equal(t, Binary, GetCommonAncestorType(FixedBinaryOf(16), FixedBinaryOf(32)))
	require.Equal(t, FixedBinaryOf(16), GetCommonAncestorType(FixedBinaryOf(16), FixedBinaryOf(16)))
}
