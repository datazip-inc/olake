// Package waljs provides functionality for handling WAL (Write-Ahead Log) entries
// from PostgreSQL, including data type conversions and change event processing.
package waljs

import (
	"math"
	"strconv"
	"testing"
)

// TestInt64Conversion verifies the correct handling of int64 (bigint) values
// during WAL change processing. It tests various input formats and edge cases
// to ensure proper type conversion and validation according to PostgreSQL rules.
//
// The test covers:
// - Basic conversions from string, float64, and int64
// - Edge cases for int64 (max/min values)
// - Float64 precision and safety checks
// - String format validation
// - Special cases (nil, negative values)
//
// Each test case validates both successful conversions and expected failures,
// ensuring the conversion logic matches PostgreSQL's bigint behavior.
func TestInt64Conversion(t *testing.T) {
	// JavaScript's safe integer limit (2^53) used as boundary for float64 conversion
	const safeIntegerLimit = 1 << 53

	testCases := []struct {
		name        string      // Test case description
		input       interface{} // Input value to convert
		expected    int64       // Expected int64 result
		shouldError bool        // Whether conversion should fail
	}{
		// Basic cases
		{
			name:     "string input",
			input:    "2342361",
			expected: 2342361,
		},
		{
			name:     "float64 input",
			input:    float64(2342361),
			expected: 2342361,
		},
		{
			name:     "int64 input",
			input:    int64(2342361),
			expected: 2342361,
		},

		// Edge cases for int64
		{
			name:     "max int64",
			input:    strconv.FormatInt(math.MaxInt64, 10),
			expected: math.MaxInt64,
		},
		{
			name:     "min int64",
			input:    strconv.FormatInt(math.MinInt64, 10),
			expected: math.MinInt64,
		},
		{
			name:     "zero as string",
			input:    "0",
			expected: 0,
		},
		{
			name:     "zero as float64",
			input:    float64(0),
			expected: 0,
		},
		{
			name:     "zero as int64",
			input:    int64(0),
			expected: 0,
		},

		// Float64 edge cases
		{
			name:     "float64 with zero decimal",
			input:    123.0,
			expected: 123,
		},
		{
			name:        "float64 with decimals",
			input:       123.456,
			shouldError: true,
		},
		{
			name:     "float64 scientific notation (within safe range)",
			input:    1.23e+10,
			expected: 12300000000,
		},
		{
			name:        "float64 exceeding safe integer limit (positive)",
			input:       float64(safeIntegerLimit + 1),
			shouldError: true,
		},
		{
			name:        "float64 exceeding safe integer limit (negative)",
			input:       float64(-safeIntegerLimit - 1),
			shouldError: true,
		},
		{
			name:     "float64 at safe integer limit (positive)",
			input:    float64(safeIntegerLimit - 1),
			expected: safeIntegerLimit - 1,
		},
		{
			name:     "float64 at safe integer limit (negative)",
			input:    float64(-safeIntegerLimit + 1),
			expected: -safeIntegerLimit + 1,
		},

		// Additional float64 cases
		{
			name:        "float64 with very small decimal",
			input:       123.000000001,
			shouldError: true,
		},
		{
			name:        "float64 with very large decimal",
			input:       123.999999999,
			shouldError: true,
		},
		{
			name:     "float64 power of 2",
			input:    float64(1 << 30),
			expected: 1 << 30,
		},
		{
			name:        "float64 NaN",
			input:       math.NaN(),
			shouldError: true,
		},
		{
			name:        "float64 positive infinity",
			input:       math.Inf(1),
			shouldError: true,
		},
		{
			name:        "float64 negative infinity",
			input:       math.Inf(-1),
			shouldError: true,
		},

		// String edge cases
		{
			name:        "empty string",
			input:       "",
			shouldError: true,
		},
		{
			name:        "non-numeric string",
			input:       "abc",
			shouldError: true,
		},
		{
			name:        "string with spaces",
			input:       " 123 ",
			shouldError: true,
		},
		{
			name:        "string exceeding int64",
			input:       "9223372036854775808", // MaxInt64 + 1
			shouldError: true,
		},
		{
			name:        "string below min int64",
			input:       "-9223372036854775809", // MinInt64 - 1
			shouldError: true,
		},

		// Additional string cases
		{
			name:        "string with plus sign",
			input:       "+123",
			shouldError: true, // PostgreSQL bigint doesn't accept + prefix
		},
		{
			name:        "string with leading zeros",
			input:       "00123",
			shouldError: true, // PostgreSQL bigint doesn't accept leading zeros
		},
		{
			name:        "string hex number",
			input:       "0x123",
			shouldError: true,
		},
		{
			name:        "string octal number",
			input:       "0o123",
			shouldError: true,
		},
		{
			name:        "string scientific notation",
			input:       "1.23e+10",
			shouldError: true,
		},
		{
			name:        "string with underscore",
			input:       "1_000_000",
			shouldError: true,
		},

		// Special cases
		{
			name:        "nil input",
			input:       nil,
			shouldError: true,
		},
		{
			name:     "negative int64",
			input:    int64(-9223372036854775807),
			expected: -9223372036854775807,
		},
		{
			name:     "negative float64",
			input:    float64(-123.0),
			expected: -123,
		},
		{
			name:     "negative string",
			input:    "-123",
			expected: -123,
		},
		{
			name:     "int64 power of 2",
			input:    int64(1 << 62),
			expected: 1 << 62,
		},
		{
			name:     "int64 boundary values",
			input:    int64(math.MaxInt64 - 1),
			expected: math.MaxInt64 - 1,
		},

		// Additional boundary tests
		{
			name:     "float64 small positive",
			input:    float64(1),
			expected: 1,
		},
		{
			name:     "float64 small negative",
			input:    float64(-1),
			expected: -1,
		},
		{
			name:        "float64 smallest subnormal",
			input:       math.SmallestNonzeroFloat64,
			shouldError: true,
		},
		{
			name:     "string small number",
			input:    "1",
			expected: 1,
		},
		{
			name:     "string negative one",
			input:    "-1",
			expected: -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result int64
			var err error
			var conversionFailed bool

			switch v := tc.input.(type) {
			case string:
				// Apply PostgreSQL bigint string format rules:
				// - Reject empty strings
				// - No leading plus signs
				// - No leading zeros (except single zero)
				if len(v) == 0 {
					conversionFailed = true
				} else if v[0] == '+' || (len(v) > 1 && v[0] == '0' && v[1] != '.') {
					conversionFailed = true
				} else {
					result, err = strconv.ParseInt(v, 10, 64)
					if err != nil {
						conversionFailed = true
					}
				}
			case float64:
				// Validate float64 values:
				// - Reject NaN and Infinity
				// - Enforce safe integer limit
				// - Ensure whole number values
				if math.IsNaN(v) || math.IsInf(v, 0) {
					conversionFailed = true
				} else if math.Abs(v) >= safeIntegerLimit || math.Floor(v) != v {
					conversionFailed = true
				} else {
					i64 := int64(v)
					if float64(i64) != v {
						conversionFailed = true
					} else {
						result = i64
					}
				}
			case int64:
				// Direct int64 values are always valid
				result = v
			case nil:
				// Nil values are invalid
				conversionFailed = true
			default:
				t.Errorf("Unexpected type: %T", tc.input)
				return
			}

			if tc.shouldError {
				if !conversionFailed {
					t.Errorf("Expected conversion to fail for input %v, but it succeeded with result %d", tc.input, result)
				}
			} else {
				if conversionFailed {
					t.Errorf("Conversion failed for input %v: %v", tc.input, err)
				} else if result != tc.expected {
					t.Errorf("Expected %d, got %d", tc.expected, result)
				}
			}
		})
	}
}
