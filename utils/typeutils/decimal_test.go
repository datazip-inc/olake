package typeutils

import (
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
)

// newDecimal builds a Decimal straight from its unscaled digits, so expectations skip the parser.
func newDecimal(dataType types.DataType, unscaled string) Decimal {
	_, _, scale := types.IsDecimal(dataType)
	return Decimal{unscaled: unscaledNum(unscaled), scale: scale, dataType: dataType}
}

func unscaledNum(digits string) decimal128.Num {
	n, _ := new(big.Int).SetString(digits, 10)
	return decimal128.FromBigInt(n)
}

func TestDecimalUnscaled(t *testing.T) {
	testCases := []struct {
		name             string
		value            any
		precision, scale int32
		want             string
		wantErr          bool
	}{
		{
			name:      "a decimal widens to the column's scale",
			value:     newDecimal(types.DecimalOf(10, 2), "-1234"),
			precision: 11,
			scale:     3,
			want:      "-12340",
		},
		{
			name:      "text at the column's scale",
			value:     "99999999.99",
			precision: 10,
			scale:     2,
			want:      "9999999999",
		},
		{
			name:      "the widest decimal",
			value:     "-1234567890123456789012345678.0123456789",
			precision: 38,
			scale:     10,
			want:      "-12345678901234567890123456780123456789",
		},
		{
			name:      "zero fits any column",
			value:     "0",
			precision: 1,
			scale:     0,
			want:      "0",
		},
		{
			name:      "a digit past the scale is rejected",
			value:     "12.345",
			precision: 10,
			scale:     2,
			wantErr:   true,
		},
		{
			name:      "a value past the precision is rejected",
			value:     "123456789",
			precision: 10,
			scale:     2,
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unscaled, err := DecimalUnscaled(tc.value, tc.precision, tc.scale)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, unscaledNum(tc.want), unscaled)
		})
	}
}

func TestDecimalText(t *testing.T) {
	testCases := []struct {
		name     string
		unscaled string
		scale    int32
		want     string
	}{
		{
			name:     "pads the scale",
			unscaled: "-12340",
			scale:    3,
			want:     "-12.340",
		},
		{
			name:     "below one",
			unscaled: "5",
			scale:    2,
			want:     "0.05",
		},
		{
			name:     "negative below one",
			unscaled: "-5",
			scale:    2,
			want:     "-0.05",
		},
		{
			name:     "zero keeps its scale",
			unscaled: "0",
			scale:    2,
			want:     "0.00",
		},
		{
			name:     "no scale",
			unscaled: "7",
			scale:    0,
			want:     "7",
		},
		{
			name:     "the widest decimal",
			unscaled: "-12345678901234567890123456780123456789",
			scale:    10,
			want:     "-1234567890123456789012345678.0123456789",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, DecimalText(unscaledNum(tc.unscaled), tc.scale))
		})
	}
}

func TestDecimalValue(t *testing.T) {
	testCases := []struct {
		name  string
		value Decimal
		want  driver.Value
	}{
		{
			name:  "binds every digit a float64 would round",
			value: newDecimal(types.DecimalOf(20, 4), "99999999999999999999"),
			want:  "9999999999999999.9999",
		},
		{
			name:  "binds the column's full scale",
			value: newDecimal(types.DecimalOf(18, 4), "1234500"),
			want:  "123.4500",
		},
		{
			name:  "binds a negative value",
			value: newDecimal(types.DecimalOf(18, 2), "-6789"),
			want:  "-67.89",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// the converter every database/sql driver falls back to, which rejects a plain struct
			got, err := driver.DefaultParameterConverter.ConvertValue(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDecimalMarshalJSON(t *testing.T) {
	testCases := []struct {
		name  string
		value Decimal
		want  string
	}{
		{
			name:  "an exact json number",
			value: newDecimal(types.DecimalOf(38, 10), "-12345678901234567890123456780123456789"),
			want:  "-1234567890123456789012345678.0123456789",
		},
		{
			name:  "zero keeps its scale",
			value: newDecimal(types.DecimalOf(10, 2), "0"),
			want:  "0.00",
		},
		{
			name:  "a whole number",
			value: newDecimal(types.DecimalOf(5, 0), "42"),
			want:  "42",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(encoded))
		})
	}
}
