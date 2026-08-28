package driver

import (
	"testing"
	"time"
)

// TestFormatCursorValue tests db2 specific cursor value formatting for state
func TestFormatCursorValue(t *testing.T) {
	ist := time.FixedZone("IST", 19800)

	tests := []struct {
		name     string
		value    any
		expected any
	}{
		{
			// wall time must be preserved: db2 timestamps carry no timezone, UTC conversion would shift the value
			name:     "time formatted without utc conversion",
			value:    time.Date(2023, 10, 5, 12, 30, 45, 123456000, ist),
			expected: "2023-10-05 12:30:45.123456",
		},
		{
			name:     "utc time formatted",
			value:    time.Date(2023, 10, 5, 12, 30, 45, 0, time.UTC),
			expected: "2023-10-05 12:30:45.000000",
		},
		{
			name:     "string passes through",
			value:    "2023-10-05",
			expected: "2023-10-05",
		},
		{
			name:     "int passes through",
			value:    int64(42),
			expected: int64(42),
		},
		{
			name:     "nil passes through",
			value:    nil,
			expected: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := (&DB2{}).FormatCursorValue(tc.value); got != tc.expected {
				t.Errorf("FormatCursorValue(%v) = %v, want %v", tc.value, got, tc.expected)
			}
		})
	}
}
