package typeutils

import (
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeFromValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected types.DataType
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: types.Null,
		},
		{
			name:     "nil pointer",
			input:    (*int)(nil),
			expected: types.Null,
		},
		{
			name:     "pointer to int",
			input:    func() *int { i := 10; return &i }(),
			expected: types.Int32,
		},
		{
			name:     "bool",
			input:    true,
			expected: types.Bool,
		},
		{
			name:     "int32",
			input:    int32(10),
			expected: types.Int32,
		},
		{
			name:     "int64",
			input:    int64(10),
			expected: types.Int64,
		},
		{
			name:     "float32",
			input:    float32(10.5),
			expected: types.Float32,
		},
		{
			name:     "float64",
			input:    float64(10.5),
			expected: types.Float64,
		},
		{
			name:     "string (plain)",
			input:    "hello",
			expected: types.String,
		},
		{
			name:     "string (date)",
			input:    "2023-01-01T00:00:00Z",
			expected: types.Timestamp,
		},
		{
			name:     "slice",
			input:    []int{1, 2},
			expected: types.Array,
		},
		{
			name:     "map",
			input:    map[string]int{"a": 1},
			expected: types.Object,
		},
		{
			name:     "time.Time",
			input:    time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: types.Timestamp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TypeFromValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMaximumOnDataType(t *testing.T) {
	t.Run("Timestamp", func(t *testing.T) {
		t1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		t2 := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)

		// Test with time.Time
		max, err := MaximumOnDataType(types.Timestamp, t1, t2)
		require.NoError(t, err)
		assert.Equal(t, t2, max)

		// Test with strings
		s1 := "2023-01-01T00:00:00Z"
		s2 := "2023-01-02T00:00:00Z"
		maxStr, err := MaximumOnDataType(types.Timestamp, s1, s2)
		require.NoError(t, err)
		assert.Equal(t, s2, maxStr)

		// Error case
		_, err = MaximumOnDataType(types.Timestamp, "invalid", "invalid")
		assert.Error(t, err)
	})

	t.Run("Int64", func(t *testing.T) {
		v1 := int64(10)
		v2 := int64(20)

		max, err := MaximumOnDataType(types.Int64, v1, v2)
		require.NoError(t, err)
		assert.Equal(t, v2, max)

		// Error case
		_, err = MaximumOnDataType(types.Int64, "invalid", "20")
		assert.Error(t, err)
	})

	t.Run("Unsupported Type", func(t *testing.T) {
		_, err := MaximumOnDataType(types.String, "a", "b")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "comparison not available")
	})
}

func TestDetectTimestampPrecision(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected types.DataType
	}{
		{
			name:     "seconds",
			input:    time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: types.Timestamp,
		},
		{
			name:     "milliseconds",
			input:    time.Date(2023, 1, 1, 0, 0, 0, 123000000, time.UTC),
			expected: types.TimestampMilli,
		},
		{
			name:     "microseconds",
			input:    time.Date(2023, 1, 1, 0, 0, 0, 123456000, time.UTC),
			expected: types.TimestampMicro,
		},
		{
			name:     "nanoseconds",
			input:    time.Date(2023, 1, 1, 0, 0, 0, 123456789, time.UTC),
			expected: types.TimestampNano,
		},
	}

	// Test internal function directly since we are in the same package

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectTimestampPrecision(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractAndMapColumnType(t *testing.T) {
	mapping := map[string]types.DataType{
		"varchar": types.String,
		"int":     types.Int32,
	}

	tests := []struct {
		name     string
		colType  string
		expected types.DataType
	}{
		{
			name:     "simple exact match",
			colType:  "int",
			expected: types.Int32,
		},
		{
			name:     "case insensitive",
			colType:  "VARCHAR",
			expected: types.String,
		},
		{
			name:     "with parameters",
			colType:  "varchar(50)",
			expected: types.String,
		},
		{
			name:     "unknown",
			colType:  "blob",
			expected: types.Unknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractAndMapColumnType(tt.colType, mapping)
			assert.Equal(t, tt.expected, result)
		})
	}
}
