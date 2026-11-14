package waljs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNumericScale(t *testing.T) {
	testCases := []struct {
		name   string
		packed int64
		want   int64
	}{
		{
			name:   "a positive scale",
			packed: 4,
			want:   4,
		},
		{
			name:   "a zero scale",
			packed: 0,
			want:   0,
		},
		{
			name:   "a negative scale postgres 15 packs as two's complement",
			packed: 2046,
			want:   -2,
		},
		{
			name:   "the most negative scale postgres allows",
			packed: 1048,
			want:   -1000,
		},
		{
			name:   "the largest scale postgres allows",
			packed: 1000,
			want:   1000,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NumericScale(tc.packed))
		})
	}
}
