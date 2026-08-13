package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestFormatCursorValue tests mongo specific cursor value normalization for state
func TestFormatCursorValue(t *testing.T) {
	now := time.Now().UTC()
	objectID := primitive.NewObjectID()

	tests := []struct {
		name     string
		value    any
		expected any
	}{
		{
			name:     "object id",
			value:    objectID,
			expected: objectID.Hex(),
		},
		{
			name:     "primitive datetime",
			value:    primitive.NewDateTimeFromTime(now),
			expected: primitive.NewDateTimeFromTime(now).Time(),
		},
		{
			name:     "primitive datetime year zero",
			value:    primitive.NewDateTimeFromTime(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)),
			expected: time.Unix(0, 0).UTC(),
		},
		{
			name:     "primitive datetime negative year",
			value:    primitive.NewDateTimeFromTime(time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)),
			expected: time.Unix(0, 0).UTC(),
		},
		{
			name:  "primitive datetime year above max",
			value: primitive.NewDateTimeFromTime(time.Date(22000, 5, 10, 0, 0, 0, 0, time.UTC)),
			expected: func() time.Time {
				parsed := primitive.NewDateTimeFromTime(time.Date(22000, 5, 10, 0, 0, 0, 0, time.UTC)).Time()
				return parsed.AddDate(-(parsed.Year() - 9999), 0, 0)
			}(),
		},
		{
			name:     "time passes through",
			value:    now,
			expected: now,
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
			assert.Equal(t, tc.expected, (&Mongo{}).FormatCursorValue(tc.value))
		})
	}
}
