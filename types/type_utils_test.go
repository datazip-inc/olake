package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedBinaryOf(t *testing.T) {
	require.Equal(t, DataType("fixed_binary(16)"), FixedBinaryOf(16))
	require.Equal(t, FixedBinary, FixedBinaryOf(16).Base())
}

// TestDataTypeFixedLength pins the fixed_binary(n) grammar: only a positive decimal length in
// parentheses parses; anything else is not a fixed binary.
func TestDataTypeFixedLength(t *testing.T) {
	testCases := []struct {
		dataType DataType
		length   int
		ok       bool
	}{
		{FixedBinaryOf(1), 1, true},
		{FixedBinaryOf(16), 16, true},
		{DataType("fixed_binary(4096)"), 4096, true},
		{FixedBinary, 0, false},
		{Binary, 0, false},
		{DataType("fixed_binary()"), 0, false},
		{DataType("fixed_binary(0)"), 0, false},
		{DataType("fixed_binary(-1)"), 0, false},
		{DataType("fixed_binary(16"), 0, false},
		{DataType("fixed_binary(1.5)"), 0, false},
		{DataType("fixed_binary(99999999999999999999)"), 0, false},
		{DataType(" fixed_binary(16)"), 0, false},
		{String, 0, false},
	}

	for _, tc := range testCases {
		t.Run(string(tc.dataType), func(t *testing.T) {
			length, ok := tc.dataType.FixedLength()
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.length, length)
		})
	}
}

// TestDataTypeBase asserts that only the parameterised fixed binary has a distinct base;
// every other DataType, declared or not, is its own base.
func TestDataTypeBase(t *testing.T) {
	require.Equal(t, FixedBinary, FixedBinaryOf(16).Base())
	require.Equal(t, FixedBinary, FixedBinary.Base())
	for _, dataType := range append(declaredDataTypes(t), Null, Unknown, DataType("undeclared_type")) {
		require.Equal(t, dataType, dataType.Base(), "%s must be its own base", dataType)
	}
}

func TestDataTypeIsBinary(t *testing.T) {
	require.True(t, Binary.IsBinary())
	require.True(t, FixedBinary.IsBinary())
	require.True(t, FixedBinaryOf(16).IsBinary())
	for _, dataType := range []DataType{String, Int64, Object, Null, Unknown, DataType("undeclared_type")} {
		require.False(t, dataType.IsBinary(), "%s must not be binary", dataType)
	}
}
