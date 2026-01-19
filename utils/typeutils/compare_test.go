package typeutils

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	baseTime := time.Now()
	laterTime := baseTime.Add(time.Hour)
	leftPointerTarget, rightPointerTarget := 1, 1

	testCases := []struct {
		name          string
		leftArgument  interface{}
		rightArgument interface{}
		expected      int
	}{
		// nil cases
		{"nil_vs_nil", nil, nil, 0},
		{"nil_vs_value", nil, 1, -1},
		{"value_vs_nil", 1, nil, 1},

		// signed integers
		{"signed_int_equal", int64(5), int(5), 0},
		{"signed_int_less", int64(-1), int(1), -1},
		{"signed_int_greater", int32(10), int8(2), 1},

		// unsigned integers
		{"unsigned_int_equal", uint64(5), uint(5), 0},
		{"unsigned_int_less", uint32(1), uint8(2), -1},
		{"unsigned_int_greater", uint(8), uint16(1), 1},

		// int8 boundaries
		{"int8_max_vs_min", int8(127), int8(-128), 1},

		// int32 boundaries
		{"int32_max_vs_max", int32(math.MaxInt32), int32(math.MaxInt32), 0},
		{"int32_min_vs_min", int32(math.MinInt32), int32(math.MinInt32), 0},
		{"int32_min_vs_max", int32(math.MinInt32), int32(math.MaxInt32), -1},
		{"int32_max_vs_min", int32(math.MaxInt32), int32(math.MinInt32), 1},

		// int64 boundaries
		{"int64_max_vs_max", int64(math.MaxInt64), int64(math.MaxInt64), 0},
		{"int64_min_vs_min", int64(math.MinInt64), int64(math.MinInt64), 0},
		{"int64_min_vs_max", int64(math.MinInt64), int64(math.MaxInt64), -1},
		{"int64_max_vs_min", int64(math.MaxInt64), int64(math.MinInt64), 1},

		// uint32 boundaries
		{"uint32_max_vs_max", uint32(math.MaxUint32), uint32(math.MaxUint32), 0},
		{"uint32_zero_vs_max", uint32(0), uint32(math.MaxUint32), -1},
		{"uint32_max_vs_zero", uint32(math.MaxUint32), uint32(0), 1},

		// uint64 boundaries
		{"uint64_max_vs_max", uint64(math.MaxUint64), uint64(math.MaxUint64), 0},
		{"uint64_zero_vs_max", uint64(0), uint64(math.MaxUint64), -1},
		{"uint64_max_vs_zero", uint64(math.MaxUint64), uint64(0), 1},

		// floats
		{"float_equal", float64(3.3), float64(3.3), 0},
		{"float_less", float32(1.1), float32(2.2), -1},
		{"float_greater", float64(5.5), float64(1.1), 1},
		{"positive_inf_vs_positive_inf", math.Inf(1), math.Inf(1), 0},
		{"negative_inf_vs_positive_inf", math.Inf(-1), math.Inf(1), -1},
		{"positive_inf_vs_negative_inf", math.Inf(1), math.Inf(-1), 1},
		{"positive_inf_vs_number", math.Inf(1), 10000000.0000, 1},
		{"negative_inf_vs_number", math.Inf(-1), -1100000000.00, -1},
		{"zero_vs_negative_zero", 0, -0, 0},

		// time
		{"time_equal", baseTime, baseTime, 0},
		{"time_less", baseTime, laterTime, -1},
		{"time_greater", laterTime, baseTime, 1},
		{"time_zero_vs_time_zero", time.Time{}, time.Time{}, 0},
		{"time_zero_vs_now", time.Time{}, baseTime, -1},
		{"time_nanosecond_diff", baseTime.Add(time.Nanosecond), baseTime, 1},
		{"time_difference", baseTime.UTC(), baseTime.In(time.Local), 0},

		// custom time
		{"custom_time_zero", Time{}, Time{}, 0},
		{"custom_time_equal", Time{Time: baseTime}, Time{Time: baseTime}, 0},
		{"custom_time_less", Time{Time: baseTime}, Time{Time: laterTime}, -1},
		{"custom_time_greater", Time{Time: laterTime}, Time{Time: baseTime}, 1},

		// bool
		{"bool_false_vs_false", false, false, 0},
		{"bool_true_vs_true", true, true, 0},
		{"bool_false_vs_true", false, true, -1},
		{"bool_true_vs_false", true, false, 1},

		// strings
		{"empty_string_vs_empty_string", "", "", 0},
		{"empty_string_vs_non_empty_string", "", "1", -1},
		{"non_empty_string_vs_empty_string", "a", "", 1},
		{"case_sensitive_comparision", "Apple", "apple", -1},
		{"numeric_string_lex_order_1", "10", "9", -1},
		{"numeric_string_lex_order_2", "2", "100", 1},
		{"unicode_comparision_less", "α", "β", -1},

		// fallback
		{"fallback_string_vs_int", "123", 123, 0},
		{"fallback_string_less", "apple", "banana", -1},
		{"fallback_struct_greater", struct{ A int }{2}, struct{ A int }{1}, 1},
		{"fallback_slice", []int{1, 2, 56}, []float32{2, 3}, -1},
		{"fallback_map", map[string]int{"z": 1}, map[string]int{"b": 34}, 1},
		{"fallback_errors", errors.New("a"), errors.New("A"), 1},
		{"fallback_pointers_lesser", &leftPointerTarget, &rightPointerTarget, -1},
		{"fallback_pointers_greater", &rightPointerTarget, &leftPointerTarget, 1},
		{"fallback_interface_wrapped_int", interface{}(int(5)), interface{}(int(5)), 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Compare(tc.leftArgument, tc.rightArgument)
			assert.Equal(t, tc.expected, result)
		})
	}
}
