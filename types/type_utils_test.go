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
			dataType: DataType("fixed_binary(16"),
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
			params, ok := tc.dataType.Params()
			require.Equal(t, tc.ok, ok)
			if ok {
				require.Equal(t, tc.length, params[0])
			}
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

func TestDataTypeParams(t *testing.T) {
	testCases := []struct {
		dataType DataType
		params   []int
		ok       bool
	}{
		{
			dataType: FixedBinaryOf(16),
			params:   []int{16},
			ok:       true,
		},
		{
			dataType: FixedBinary,
			params:   nil,
			ok:       false,
		},
		{
			dataType: Binary,
			params:   nil,
			ok:       false,
		},
		{
			dataType: String,
			params:   nil,
			ok:       false,
		},
		{
			dataType: DataType("fixed_binary(0)"),
			params:   nil,
			ok:       false,
		},
		{
			dataType: DataType("undeclared(1)"),
			params:   nil,
			ok:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.dataType), func(t *testing.T) {
			params, ok := tc.dataType.Params()
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.params, params)
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
			params: []int{9, 2},
			ok:     true,
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

func TestDataTypeAccepts(t *testing.T) {
	testCases := []struct {
		column, detected DataType
		accepts          bool
	}{
		{
			column:   FixedBinaryOf(16),
			detected: FixedBinaryOf(16),
			accepts:  true,
		},
		{
			column:   FixedBinaryOf(16),
			detected: Binary,
			accepts:  false,
		},
		{
			column:   FixedBinaryOf(16),
			detected: FixedBinaryOf(32),
			accepts:  false,
		},
		{
			column:   FixedBinaryOf(16),
			detected: String,
			accepts:  false,
		},
		{
			column:   Binary,
			detected: FixedBinaryOf(16), // fixed bytes are bytes
			accepts:  true,
		},
		{
			column:   Binary,
			detected: String, // text is bytes
			accepts:  true,
		},
		{
			column:   String,
			detected: Binary,
			accepts:  false,
		},
		{
			column:   Int64,
			detected: Int32,
			accepts:  true,
		},
		{
			column:   Int32,
			detected: Int64,
			accepts:  false,
		},
		{
			column:   String,
			detected: Float64,
			accepts:  true,
		},
		{
			column:   Float64,
			detected: String,
			accepts:  false,
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.accepts, tc.column.Accepts(tc.detected), "%s accepts %s", tc.column, tc.detected)
	}
}

func TestSameType(t *testing.T) {
	testCases := []struct {
		name string
		a    DataType
		b    DataType
		same bool
	}{
		{
			name: "a type is itself",
			a:    Binary,
			b:    Binary,
			same: true,
		},
		{
			name: "an instance is itself",
			a:    FixedBinaryOf(16),
			b:    FixedBinaryOf(16),
			same: true,
		},
		{
			name: "binary and fixed with binary",
			a:    FixedBinaryOf(16),
			b:    Binary,
			same: false,
		},
		{
			name: "and the relation is symmetric",
			a:    Binary,
			b:    FixedBinaryOf(16),
			same: false,
		},
		{
			name: "different parameters but same type",
			a:    FixedBinaryOf(16),
			b:    FixedBinaryOf(32),
			same: true,
		},
		{
			name: "unparameterised and parameterised are the same type",
			a:    FixedBinaryOf(16),
			b:    FixedBinary,
			same: true,
		},
		{
			name: "binary is not string",
			a:    Binary,
			b:    String,
			same: false,
		},
		{
			name: "string is not int64",
			a:    String,
			b:    Int64,
			same: false,
		},
		{
			name: "the family pattern is not binary",
			a:    FixedBinary,
			b:    Binary,
			same: false,
		},
		{
			name: "an int is not binary",
			a:    Int64,
			b:    Binary,
			same: false,
		},
		{
			name: "an object is not binary",
			a:    Object,
			b:    Binary,
			same: false,
		},
		{
			name: "null is not binary",
			a:    Null,
			b:    Binary,
			same: false,
		},
		{
			name: "unknown is not binary",
			a:    Unknown,
			b:    Binary,
			same: false,
		},
		{
			name: "an undeclared type is not binary",
			a:    DataType("undeclared_type"),
			b:    Binary,
			same: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.same, SameType(tc.a, tc.b))
		})
	}
}
