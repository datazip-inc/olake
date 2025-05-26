package olake_test

import (
	"strconv"
	"testing"
)

func TestInt64Conversion(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected int64
	}{
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result int64
			var err error

			switch v := tc.input.(type) {
			case string:
				result, err = strconv.ParseInt(v, 10, 64)
				if err != nil {
					t.Errorf("Failed to convert string to int64: %v", err)
				}
			case float64:
				if float64(int64(v)) == v {
					result = int64(v)
				} else {
					t.Errorf("Float64 value %v cannot be safely converted to int64", v)
				}
			case int64:
				result = v
			default:
				t.Errorf("Unexpected type: %T", tc.input)
			}

			if result != tc.expected {
				t.Errorf("Expected %d, got %d", tc.expected, result)
			}
		})
	}
}
