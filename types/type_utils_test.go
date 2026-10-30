package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedBinaryOf(t *testing.T) {
	require.Equal(t, DataType("fixed_binary(16)"), FixedBinaryOf(16))
	require.Equal(t, FixedBinary, baseOf(FixedBinaryOf(16)))
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
	require.Equal(t, FixedBinary, baseOf(FixedBinaryOf(16)))
	require.Equal(t, FixedBinary, baseOf(FixedBinary))
	for _, dataType := range append(declaredDataTypes(t), Null, Unknown, DataType("undeclared_type")) {
		require.Equal(t, dataType, baseOf(dataType), "%s must be its own base", dataType)
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
			dataType:  DecimalOf(10, 2),
			valueType: BigDecimal,
		},
		{
			dataType:  Decimal,
			valueType: BigDecimal,
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

func TestDecimalOf(t *testing.T) {
	testCases := []struct {
		name             string
		precision, scale int
		want             DataType
	}{
		{
			name:      "a decimal",
			precision: 9,
			scale:     2,
			want:      DataType("decimal(9,2)"),
		},
		{
			name:      "the narrowest decimal",
			precision: 1,
			scale:     0,
			want:      DataType("decimal(1,0)"),
		},
		{
			name:      "every digit after the point",
			precision: 38,
			scale:     38,
			want:      DataType("decimal(38,38)"),
		},
		{
			name:      "past 38 digits is a string",
			precision: 39,
			scale:     2,
			want:      String,
		},
		{
			name:      "no digits is a string",
			precision: 0,
			scale:     0,
			want:      String,
		},
		{
			name:      "a scale past the precision is a string",
			precision: 5,
			scale:     6,
			want:      String,
		},
		{
			name:      "a negative scale is a string",
			precision: 5,
			scale:     -1,
			want:      String,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, DecimalOf(tc.precision, tc.scale))
		})
	}
}

func TestDecimalParams(t *testing.T) {
	testCases := []struct {
		name             string
		dataType         DataType
		precision, scale int32
		ok               bool
	}{
		{
			name:      "a decimal",
			dataType:  DecimalOf(9, 2),
			precision: 9,
			scale:     2,
			ok:        true,
		},
		{
			name:      "every digit after the point",
			dataType:  DecimalOf(38, 38),
			precision: 38,
			scale:     38,
			ok:        true,
		},
		{
			name:     "the family pattern holds no parameters",
			dataType: Decimal,
			ok:       false,
		},
		{
			name:     "past 38 digits",
			dataType: DataType("decimal(39,2)"),
			ok:       false,
		},
		{
			name:     "the iceberg spelling",
			dataType: DataType("decimal(9, 2)"),
			ok:       false,
		},
		{
			name:     "another type",
			dataType: Float64,
			ok:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			precision, scale, ok := DecimalParams(tc.dataType)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.precision, precision)
			require.Equal(t, tc.scale, scale)
		})
	}
}

func TestIcebergDecimalParams(t *testing.T) {
	testCases := []struct {
		name             string
		icebergType      string
		precision, scale int32
		ok               bool
	}{
		{
			name:        "a decimal",
			icebergType: "decimal(9, 2)",
			precision:   9,
			scale:       2,
			ok:          true,
		},
		{
			name:        "the widest decimal",
			icebergType: "decimal(38, 10)",
			precision:   38,
			scale:       10,
			ok:          true,
		},
		{
			name:        "past 38 digits",
			icebergType: "decimal(39, 2)",
			ok:          false,
		},
		{
			name:        "the olake spelling",
			icebergType: "decimal(9,2)",
			ok:          false,
		},
		{
			name:        "another type",
			icebergType: "double",
			ok:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			precision, scale, ok := IcebergDecimalParams(tc.icebergType)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.precision, precision)
			require.Equal(t, tc.scale, scale)
		})
	}
}

func TestDecimalBytes(t *testing.T) {
	testCases := []struct {
		name      string
		precision int32
		bytes     int32
	}{
		{
			name:      "one digit fits a byte",
			precision: 1,
			bytes:     1,
		},
		{
			name:      "two digits fit a byte",
			precision: 2,
			bytes:     1,
		},
		{
			name:      "three digits need two bytes",
			precision: 3,
			bytes:     2,
		},
		{
			name:      "nine digits fit four bytes",
			precision: 9,
			bytes:     4,
		},
		{
			name:      "ten digits need five bytes",
			precision: 10,
			bytes:     5,
		},
		{
			name:      "eighteen digits fit eight bytes",
			precision: 18,
			bytes:     8,
		},
		{
			name:      "nineteen digits need nine bytes",
			precision: 19,
			bytes:     9,
		},
		{
			name:      "twenty digits fit nine bytes",
			precision: 20,
			bytes:     9,
		},
		{
			name:      "28 digits fit twelve bytes",
			precision: 28,
			bytes:     12,
		},
		{
			name:      "29 digits need thirteen bytes",
			precision: 29,
			bytes:     13,
		},
		{
			name:      "38 digits fit sixteen bytes",
			precision: 38,
			bytes:     16,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.bytes, DecimalBytes(tc.precision))
		})
	}
}
