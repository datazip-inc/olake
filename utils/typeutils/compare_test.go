package typeutils

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Hour)

	testCases := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		// nil cases
		{"nil_vs_nil", nil, nil, 0},
		{"nil_vs_value", nil, 1, -1},
		{"value_vs_nil", 1, nil, 1},

		// signed integers
		{"signed_int_equal", int64(5), int(5), 0},
		{"signed_int_less", int(-1), int(1), -1},
		{"signed_int_greater", int32(10), int8(2), 1},

		// unsigned integers
		{"unsigned_int_equal", uint64(5), uint(5), 0},
		{"unsigned_int_less", uint32(1), uint8(2), -1},
		{"unsigned_int_greater", uint(8), uint16(1), 1},

		// floats
		{"float_equal", float64(3.3), float32(3.3), 0},
		{"float_less", float32(1.1), float64(2.2), -1},
		{"float_greater", float64(5.5), float64(1.1), 1},

		// float edge cases
		{"nan_vs_nan", math.NaN(), math.NaN(), 0},
		{"nan_vs_value", math.NaN(), 1.0, -1},
		{"value_vs_nan", 1.0, math.NaN(), 1},
		{"inf_vs_inf", math.Inf(1), math.Inf(1), 1},
		{"inf_vs_value", math.Inf(1), 1.0, 1},
		{"neg_inf_vs_value", math.Inf(-1), 1.0, -1},

		// time
		{"time_equal", now, now, 0},
		{"time_less", now, later, -1},
		{"time_greater", later, now, 1},

		// bool
		{"bool_false_vs_false", false, false, 0},
		{"bool_true_vs_true", true, true, 0},
		{"bool_false_vs_true", false, true, -1},
		{"bool_true_vs_false", true, false, 1},

		// fallback
		{"fallback_string_vs_int", "123", 123, 0},
		{"fallback_string_less", "apple", "banana", -1},
		{"fallback_struct_greater", struct{ A int }{2}, struct{ A int }{1}, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Compare(tc.a, tc.b)
			assert.Equal(t, tc.expected, result)
		})
	}
}
