package typeutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCompare tests the public Compare method for various data types
func TestCompare(t *testing.T) {
	tests := []struct {
		name     string
		a        any
		b        any
		expected int
	}{
		// both values are nil, should return 0 (equal)
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: 0,
		},
		// first value is nil, second is not, nil is considered less than any value
		{
			name:     "a is nil, b is not",
			a:        nil,
			b:        42,
			expected: -1,
		},
		// first value is not nil, second is nil, any value is considered greater than nil
		{
			name:     "a is not nil, b is nil",
			a:        42,
			b:        nil,
			expected: 1,
		},

		// unsigned integers with same value should return 0
		{
			name:     "uint equal",
			a:        uint(42),
			b:        uint(42),
			expected: 0,
		},
		// smaller uint compared to larger uint should return -1
		{
			name:     "uint a < b",
			a:        uint(10),
			b:        uint(20),
			expected: -1,
		},
		// larger uint compared to smaller uint should return 1
		{
			name:     "uint a > b",
			a:        uint(100),
			b:        uint(50),
			expected: 1,
		},
		// uint8 values that are equal
		{
			name:     "uint8 equal",
			a:        uint8(255),
			b:        uint8(255),
			expected: 0,
		},
		// uint8 less than comparison
		{
			name:     "uint8 a < b",
			a:        uint8(100),
			b:        uint8(200),
			expected: -1,
		},
		// uint16 greater than comparison
		{
			name:     "uint16 a > b",
			a:        uint16(5000),
			b:        uint16(3000),
			expected: 1,
		},
		// uint32 equal comparison
		{
			name:     "uint32 equal",
			a:        uint32(12345),
			b:        uint32(12345),
			expected: 0,
		},
		// uint64 less than comparison
		{
			name:     "uint64 a < b",
			a:        uint64(9999999),
			b:        uint64(10000000),
			expected: -1,
		},
		// mixed unsigned integer types are converted to uint64 for comparison
		{
			name:     "mixed uint types comparison",
			a:        uint8(50),
			b:        uint64(50),
			expected: 0,
		},
		// zero values for unsigned integers should be equal
		{
			name:     "uint zero values",
			a:        uint(0),
			b:        uint(0),
			expected: 0,
		},

		// signed integers with same value should return 0
		{
			name:     "int equal",
			a:        int(42),
			b:        int(42),
			expected: 0,
		},
		// negative int compared to positive int should return -1
		{
			name:     "int a < b",
			a:        int(-10),
			b:        int(10),
			expected: -1,
		},
		// positive int compared to negative int should return 1
		{
			name:     "int a > b",
			a:        int(100),
			b:        int(-100),
			expected: 1,
		},
		// comparing two negative int8 values
		{
			name:     "int8 negative comparison",
			a:        int8(-50),
			b:        int8(-30),
			expected: -1,
		},
		// int16 equal comparison
		{
			name:     "int16 equal",
			a:        int16(3000),
			b:        int16(3000),
			expected: 0,
		},
		// int32 greater than comparison
		{
			name:     "int32 a > b",
			a:        int32(1000000),
			b:        int32(999999),
			expected: 1,
		},
		// int64 less than comparison with negative values
		{
			name:     "int64 a < b",
			a:        int64(-9999999),
			b:        int64(-9999998),
			expected: -1,
		},
		// mixed signed integer types are converted to int64 for comparison
		{
			name:     "mixed int types comparison",
			a:        int8(-10),
			b:        int64(-10),
			expected: 0,
		},
		// zero values for signed integers should be equal
		{
			name:     "int zero values",
			a:        int(0),
			b:        int(0),
			expected: 0,
		},
		// negative value compared to zero should return -1
		{
			name:     "int negative zero comparison",
			a:        int(-1),
			b:        int(0),
			expected: -1,
		},

		// float32 values that are equal
		{
			name:     "float32 equal",
			a:        float32(3.14),
			b:        float32(3.14),
			expected: 0,
		},
		// float32 less than comparison
		{
			name:     "float32 a < b",
			a:        float32(2.5),
			b:        float32(3.5),
			expected: -1,
		},
		// float32 greater than comparison
		{
			name:     "float32 a > b",
			a:        float32(10.99),
			b:        float32(10.98),
			expected: 1,
		},
		// float64 values that are equal
		{
			name:     "float64 equal",
			a:        float64(2.718281828),
			b:        float64(2.718281828),
			expected: 0,
		},
		// negative float64 compared to positive float64
		{
			name:     "float64 a < b",
			a:        float64(-100.5),
			b:        float64(100.5),
			expected: -1,
		},
		// float64 greater than comparison with small difference
		{
			name:     "float64 a > b",
			a:        float64(999.999),
			b:        float64(999.998),
			expected: 1,
		},
		// mixed float types are converted to float64 for comparison
		{
			name:     "mixed float types comparison",
			a:        float32(5.5),
			b:        float64(5.5),
			expected: 0,
		},
		// zero values for floats should be equal
		{
			name:     "float zero values",
			a:        float64(0.0),
			b:        float64(0.0),
			expected: 0,
		},
		// negative float comparison
		{
			name:     "float negative comparison",
			a:        float64(-1.5),
			b:        float64(-1.0),
			expected: -1,
		},

		// time.Time values representing the same instant should be equal
		{
			name:     "time.Time equal",
			a:        time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC),
			b:        time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC),
			expected: 0,
		},
		// earlier time compared to later time should return -1
		{
			name:     "time.Time a before b",
			a:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			b:        time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			expected: -1,
		},
		// later time compared to earlier time should return 1
		{
			name:     "time.Time a after b",
			a:        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			b:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: 1,
		},
		// time comparison with nanosecond precision
		{
			name:     "time.Time with nanoseconds precision",
			a:        time.Date(2024, 3, 19, 15, 30, 0, 123456789, time.UTC),
			b:        time.Date(2024, 3, 19, 15, 30, 0, 123456788, time.UTC),
			expected: 1,
		},
		// same instant in different timezones should be equal
		{
			name:     "time.Time different timezones same instant",
			a:        time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC),
			b:        time.Date(2024, 3, 19, 10, 30, 0, 0, time.FixedZone("EST", -5*60*60)),
			expected: 0,
		},

		// both boolean values are false, should be equal
		{
			name:     "bool both false",
			a:        false,
			b:        false,
			expected: 0,
		},
		// both boolean values are true, should be equal
		{
			name:     "bool both true",
			a:        true,
			b:        true,
			expected: 0,
		},
		// false is considered less than true
		{
			name:     "bool false < true",
			a:        false,
			b:        true,
			expected: -1,
		},
		// true is considered greater than false
		{
			name:     "bool true > false",
			a:        true,
			b:        false,
			expected: 1,
		},

		// custom Time type with equal values
		{
			name:     "custom Time equal",
			a:        Time{time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC)},
			b:        Time{time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC)},
			expected: 0,
		},
		// custom Time type with a before b
		{
			name:     "custom Time a before b",
			a:        Time{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			b:        Time{time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)},
			expected: -1,
		},
		// custom Time type with a after b
		{
			name:     "custom Time a after b",
			a:        Time{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
			b:        Time{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			expected: 1,
		},

		// strings that are equal should return 0
		{
			name:     "string equal",
			a:        "hello",
			b:        "hello",
			expected: 0,
		},
		// lexicographic comparison: "apple" < "banana"
		{
			name:     "string a < b alphabetically",
			a:        "apple",
			b:        "banana",
			expected: -1,
		},
		// lexicographic comparison: "zebra" > "apple"
		{
			name:     "string a > b alphabetically",
			a:        "zebra",
			b:        "apple",
			expected: 1,
		},
		// empty strings should be equal
		{
			name:     "string empty comparison",
			a:        "",
			b:        "",
			expected: 0,
		},
		// empty string is less than non-empty string
		{
			name:     "string empty vs non-empty",
			a:        "",
			b:        "hello",
			expected: -1,
		},
		// numeric strings are compared lexicographically, not numerically
		{
			name:     "string numeric strings",
			a:        "100",
			b:        "99",
			expected: -1, // "100" < "99" lexicographically (1 < 9)
		},

		// struct values are converted to string for comparison (fallback behavior)
		{
			name:     "struct comparison via string fallback",
			a:        struct{ Value int }{Value: 10},
			b:        struct{ Value int }{Value: 10},
			expected: 0,
		},
		// slice values are converted to string for comparison (fallback behavior)
		{
			name:     "slice comparison via string fallback",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 3},
			expected: 0,
		},
		// map values are converted to string for comparison (fallback behavior)
		{
			name:     "map comparison via string fallback",
			a:        map[string]int{"a": 1},
			b:        map[string]int{"a": 1},
			expected: 0,
		},

		// maximum uint64 value compared to one less than maximum
		{
			name:     "large uint64 value",
			a:        uint64(18446744073709551615), // max uint64
			b:        uint64(18446744073709551614),
			expected: 1,
		},
		// minimum int64 value compared to maximum int64 value
		{
			name:     "min and max int64",
			a:        int64(-9223372036854775808), // min int64
			b:        int64(9223372036854775807),  // max int64
			expected: -1,
		},

		// very large float64 values comparison
		{
			name:     "float large values",
			a:        float64(1e308),
			b:        float64(1e307),
			expected: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := Compare(tc.a, tc.b)
			assert.Equal(t, tc.expected, result,
				"Compare(%v, %v) should return %d, got %d",
				tc.a, tc.b, tc.expected, result)
		})
	}
}

// TestCompareConsistency verifies mathematical comparison properties
func TestCompareConsistency(t *testing.T) {
	// reflexivity: for any value a, a == a should always be true
	t.Run("reflexivity: a == a", func(t *testing.T) {
		values := []any{
			42,
			uint(100),
			float64(3.14),
			true,
			"hello",
			time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC),
			Time{time.Date(2024, 3, 19, 15, 30, 0, 0, time.UTC)},
		}

		for _, v := range values {
			result := Compare(v, v)
			assert.Equal(t, 0, result, "Compare(%v, %v) should return 0 (reflexivity)", v, v)
		}
	})

	// antisymmetry: if a < b, then b > a (sign should be opposite)
	t.Run("antisymmetry: if a < b then b > a", func(t *testing.T) {
		testCases := []struct {
			a any
			b any
		}{
			{10, 20},
			{uint(5), uint(15)},
			{float64(1.5), float64(2.5)},
			{false, true},
			{"apple", "banana"},
			{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)},
		}

		for _, tc := range testCases {
			resultAB := Compare(tc.a, tc.b)
			resultBA := Compare(tc.b, tc.a)
			assert.Equal(t, -resultAB, resultBA,
				"Compare(%v, %v) = %d should be opposite of Compare(%v, %v) = %d",
				tc.a, tc.b, resultAB, tc.b, tc.a, resultBA)
		}
	})

	// transitivity: if a < b and b < c, then a < c
	t.Run("transitivity: if a < b and b < c then a < c", func(t *testing.T) {
		// integer transitivity test
		a, b, c := 10, 20, 30
		assert.Equal(t, -1, Compare(a, b))
		assert.Equal(t, -1, Compare(b, c))
		assert.Equal(t, -1, Compare(a, c))

		// float transitivity test
		af, bf, cf := float64(1.5), float64(2.5), float64(3.5)
		assert.Equal(t, -1, Compare(af, bf))
		assert.Equal(t, -1, Compare(bf, cf))
		assert.Equal(t, -1, Compare(af, cf))

		// time transitivity test
		at := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		bt := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
		ct := time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, -1, Compare(at, bt))
		assert.Equal(t, -1, Compare(bt, ct))
		assert.Equal(t, -1, Compare(at, ct))
	})
}

// TestCompareNilEdgeCases tests nil handling with different types
func TestCompareNilEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		a        any
		b        any
		expected int
	}{
		// nil compared to zero integer (nil is always less than any value)
		{
			name:     "nil vs zero int",
			a:        nil,
			b:        0,
			expected: -1,
		},
		// zero integer compared to nil (any value is always greater than nil)
		{
			name:     "zero int vs nil",
			a:        0,
			b:        nil,
			expected: 1,
		},
		// nil compared to empty string
		{
			name:     "nil vs empty string",
			a:        nil,
			b:        "",
			expected: -1,
		},
		// empty string compared to nil
		{
			name:     "empty string vs nil",
			a:        "",
			b:        nil,
			expected: 1,
		},
		// nil compared to false boolean
		{
			name:     "nil vs false",
			a:        nil,
			b:        false,
			expected: -1,
		},
		// nil compared to zero float
		{
			name:     "nil vs zero float",
			a:        nil,
			b:        float64(0.0),
			expected: -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := Compare(tc.a, tc.b)
			assert.Equal(t, tc.expected, result,
				"Compare(%v, %v) should return %d, got %d",
				tc.a, tc.b, tc.expected, result)
		})
	}
}

// TestCompareTypeConversions tests cross-type comparisons within same category
func TestCompareTypeConversions(t *testing.T) {
	tests := []struct {
		name     string
		a        any
		b        any
		expected int
	}{
		// uint8 and uint64 with same value should be equal after conversion
		{
			name:     "uint8 vs uint64 equal",
			a:        uint8(255),
			b:        uint64(255),
			expected: 0,
		},
		// uint16 less than uint32 after conversion
		{
			name:     "uint16 vs uint32 less than",
			a:        uint16(100),
			b:        uint32(200),
			expected: -1,
		},
		// uint32 greater than uint after conversion
		{
			name:     "uint32 vs uint greater than",
			a:        uint32(1000),
			b:        uint(500),
			expected: 1,
		},

		// int8 and int64 with same value should be equal after conversion
		{
			name:     "int8 vs int64 equal",
			a:        int8(-128),
			b:        int64(-128),
			expected: 0,
		},
		// int16 less than int32 after conversion
		{
			name:     "int16 vs int32 less than",
			a:        int16(-1000),
			b:        int32(1000),
			expected: -1,
		},
		// int64 greater than int after conversion
		{
			name:     "int64 vs int greater than",
			a:        int64(999999),
			b:        int(100),
			expected: 1,
		},

		// float32 and float64 with same value should be equal after conversion
		// using a value that can be exactly represented in both float32 and float64
		{
			name:     "float32 vs float64 equal",
			a:        float32(2.5),
			b:        float64(2.5),
			expected: 0,
		},
		// float32 less than float64 after conversion
		{
			name:     "float32 vs float64 less than",
			a:        float32(1.5),
			b:        float64(2.5),
			expected: -1,
		},
		// float64 greater than float32 after conversion
		{
			name:     "float64 vs float32 greater than",
			a:        float64(100.99),
			b:        float32(50.5),
			expected: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := Compare(tc.a, tc.b)
			assert.Equal(t, tc.expected, result,
				"Compare(%v, %v) should return %d, got %d",
				tc.a, tc.b, tc.expected, result)
		})
	}
}
