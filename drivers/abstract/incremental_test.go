package abstract

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/stretchr/testify/require"
)

func TestAbstractDriverFormatCursorValue(t *testing.T) {
	decimalCursor := func(dataType types.DataType, text string) typeutils.Decimal {
		value, err := typeutils.ReformatDecimal(dataType, text)
		require.NoError(t, err)
		return value
	}

	testCases := []struct {
		name   string
		cursor any
		want   any
	}{
		{
			name:   "a decimal cursor keeps every digit a float64 would round",
			cursor: decimalCursor(types.DecimalOf(20, 4), "12345678901234.5679"),
			want:   "12345678901234.5679",
		},
		{
			name:   "a decimal cursor keeps its full scale",
			cursor: decimalCursor(types.DecimalOf(18, 4), "123.45"),
			want:   "123.4500",
		},
		{
			name:   "a float cursor stays a number",
			cursor: float64(123.45),
			want:   float64(123.45),
		},
	}

	driver := NewAbstractDriver(context.Background(), stubDriver{typ: "postgres"})
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, driver.FormatCursorValue(tc.cursor))
		})
	}
}
