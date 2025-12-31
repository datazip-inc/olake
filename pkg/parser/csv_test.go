package parser

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVParser_InferSchema_TypeDetection(t *testing.T) {
	tests := []struct {
		name          string
		csvData       string
		expectedTypes map[string]types.DataType
		description   string
	}{
		{
			name: "timestamp with millisecond precision",
			csvData: `created_at
2024-01-15 10:30:45.123
2024-01-16 11:31:46.456`,
			expectedTypes: map[string]types.DataType{
				"created_at": types.TimestampMilli,
			},
			description: "Should detect TimestampMilli for timestamps with millisecond precision",
		},
		{
			name: "timestamp with microsecond precision",
			csvData: `created_at
2024-01-15 10:30:45.123456
2024-01-16 11:31:46.789012`,
			expectedTypes: map[string]types.DataType{
				"created_at": types.TimestampMicro,
			},
			description: "Should detect TimestampMicro for timestamps with microsecond precision",
		},
		{
			name: "timestamp with nanosecond precision",
			csvData: `created_at
2024-01-15T10:30:45.123456789Z
2024-01-16T11:31:46.987654321Z`,
			expectedTypes: map[string]types.DataType{
				"created_at": types.TimestampNano,
			},
			description: "Should detect TimestampNano for timestamps with nanosecond precision",
		},
		{
			name: "timestamp without time precision",
			csvData: `created_at
2024-01-15
2024-01-16`,
			expectedTypes: map[string]types.DataType{
				"created_at": types.Timestamp,
			},
			description: "Should return Timestamp when date has no time precision",
		},
		{
			name: "integer values",
			csvData: `id
123
456
789`,
			expectedTypes: map[string]types.DataType{
				"id": types.Int64,
			},
			description: "Should detect Int64 for integer values",
		},
		{
			name: "float values",
			csvData: `price
123.45
456.78
789.12`,
			expectedTypes: map[string]types.DataType{
				"price": types.Float64,
			},
			description: "Should detect Float64 for float values",
		},
		{
			name: "boolean values",
			csvData: `active
true
false
TRUE`,
			expectedTypes: map[string]types.DataType{
				"active": types.Bool,
			},
			description: "Should detect Bool for boolean values",
		},
		{
			name: "mixed types in column",
			csvData: `mixed_column
123
hello
true`,
			expectedTypes: map[string]types.DataType{
				"mixed_column": types.String, // Resolver handles mixed types differently
			},
			description: "Should handle mixed types in column (resolver behavior)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := CSVConfig{
				Delimiter: ",",
				HasHeader: true,
				SkipRows:  0,
			}

			stream := types.NewStream("test", "test", nil)
			parser := NewCSVParser(config, stream)

			ctx := context.Background()
			reader := strings.NewReader(tt.csvData)

			result, err := parser.InferSchema(ctx, reader)
			require.NoError(t, err)

			for fieldName, expectedType := range tt.expectedTypes {
				fieldType, err := result.Schema.GetType(fieldName)
				require.NoError(t, err)
				assert.Equal(t, expectedType, fieldType, tt.description)
			}
		})
	}
}

func TestConvertValue_Int32Int64(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		fieldType   types.DataType
		expected    interface{}
		expectError bool
		description string
	}{
		{
			name:        "Int32 conversion",
			value:       "12345",
			fieldType:   types.Int32,
			expected:    int32(12345),
			expectError: false,
			description: "Should convert string to int32",
		},
		{
			name:        "Int64 conversion",
			value:       "9223372036854775807",
			fieldType:   types.Int64,
			expected:    int64(9223372036854775807),
			expectError: false,
			description: "Should convert string to int64",
		},
		{
			name:        "Int32 out of range",
			value:       "2147483648", // Max int32 + 1
			fieldType:   types.Int32,
			expected:    nil,
			expectError: true,
			description: "Should error when value exceeds int32 range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.value, tt.fieldType)
			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				assert.Equal(t, tt.expected, result, tt.description)
				// Verify the actual type
				if tt.fieldType == types.Int32 {
					_, ok := result.(int32)
					assert.True(t, ok, "Result should be int32 type")
				} else if tt.fieldType == types.Int64 {
					_, ok := result.(int64)
					assert.True(t, ok, "Result should be int64 type")
				}
			}
		})
	}
}

func TestConvertValue_Float32Float64(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		fieldType   types.DataType
		expected    interface{}
		expectError bool
		description string
	}{
		{
			name:        "Float32 conversion",
			value:       "123.456",
			fieldType:   types.Float32,
			expected:    float32(123.456),
			expectError: false,
			description: "Should convert string to float32",
		},
		{
			name:        "Float64 conversion",
			value:       "123.456789012345",
			fieldType:   types.Float64,
			expected:    float64(123.456789012345),
			expectError: false,
			description: "Should convert string to float64",
		},
		{
			name:        "Float32 precision",
			value:       "3.14159",
			fieldType:   types.Float32,
			expected:    float32(3.14159),
			expectError: false,
			description: "Should handle float32 precision correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.value, tt.fieldType)
			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				// Use approximate comparison for floats
				if tt.fieldType == types.Float32 {
					assert.InDelta(t, tt.expected.(float32), result.(float32), 0.0001, tt.description)
					_, ok := result.(float32)
					assert.True(t, ok, "Result should be float32 type")
				} else if tt.fieldType == types.Float64 {
					assert.InDelta(t, tt.expected.(float64), result.(float64), 0.0000001, tt.description)
					_, ok := result.(float64)
					assert.True(t, ok, "Result should be float64 type")
				}
			}
		})
	}
}

func TestConvertValue_Timestamp(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		fieldType   types.DataType
		expectError bool
		description string
	}{
		{
			name:        "Timestamp conversion - ISO format",
			value:       "2024-01-15T10:30:45Z",
			fieldType:   types.Timestamp,
			expectError: false,
			description: "Should convert ISO timestamp string to time.Time",
		},
		{
			name:        "TimestampMilli conversion",
			value:       "2024-01-15 10:30:45.123",
			fieldType:   types.TimestampMilli,
			expectError: false,
			description: "Should convert timestamp with milliseconds",
		},
		{
			name:        "TimestampMicro conversion",
			value:       "2024-01-15 10:30:45.123456",
			fieldType:   types.TimestampMicro,
			expectError: false,
			description: "Should convert timestamp with microseconds",
		},
		{
			name:        "TimestampNano conversion",
			value:       "2024-01-15T10:30:45.123456789Z",
			fieldType:   types.TimestampNano,
			expectError: false,
			description: "Should convert timestamp with nanoseconds",
		},
		{
			name:        "Invalid timestamp",
			value:       "not a timestamp",
			fieldType:   types.Timestamp,
			expectError: true,
			description: "Should error on invalid timestamp format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.value, tt.fieldType)
			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				_, ok := result.(time.Time)
				assert.True(t, ok, "Result should be time.Time type")
			}
		})
	}
}

func TestConvertValue_ObjectArray(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		fieldType   types.DataType
		expectError bool
		checkType   func(interface{}) bool
		description string
	}{
		{
			name:        "Object conversion - valid JSON",
			value:       `{"key": "value", "number": 123}`,
			fieldType:   types.Object,
			expectError: false,
			checkType: func(v interface{}) bool {
				_, ok := v.(map[string]interface{})
				return ok
			},
			description: "Should convert valid JSON object string to map",
		},
		{
			name:        "Object conversion - invalid JSON",
			value:       `{invalid json}`,
			fieldType:   types.Object,
			expectError: false, // Should not error, but return as string
			checkType: func(v interface{}) bool {
				_, ok := v.(string)
				return ok
			},
			description: "Should return as string when JSON parsing fails",
		},
		{
			name:        "Array conversion - valid JSON",
			value:       `[1, 2, 3, "four"]`,
			fieldType:   types.Array,
			expectError: false,
			checkType: func(v interface{}) bool {
				_, ok := v.([]interface{})
				return ok
			},
			description: "Should convert valid JSON array string to slice",
		},
		{
			name:        "Array conversion - invalid JSON",
			value:       `[invalid json`,
			fieldType:   types.Array,
			expectError: false, // Should not error, but return as string
			checkType: func(v interface{}) bool {
				_, ok := v.(string)
				return ok
			},
			description: "Should return as string when JSON parsing fails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.value, tt.fieldType)
			if tt.expectError {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				assert.True(t, tt.checkType(result), tt.description)
			}
		})
	}
}

func TestCSVParser_InferSchema_WithTimestamps(t *testing.T) {
	csvData := `id,name,created_at,price
1,Alice,2024-01-15 10:30:45.123456,99.99
2,Bob,2024-01-16 11:31:46.789012,149.50
3,Charlie,2024-01-17 12:32:47.345678,79.99`

	config := CSVConfig{
		Delimiter: ",",
		HasHeader: true,
		SkipRows:  0,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewCSVParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(csvData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Check that timestamp was detected
	fieldType, err := result.Schema.GetType("created_at")
	require.NoError(t, err)
	assert.Contains(t, []types.DataType{
		types.Timestamp,
		types.TimestampMilli,
		types.TimestampMicro,
		types.TimestampNano,
	}, fieldType, "created_at should be detected as a timestamp type")

	// Check that float was detected
	fieldType, err = result.Schema.GetType("price")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, fieldType, "price should be detected as Float64")
}

func TestCSVParser_StreamRecords_TypeConversion(t *testing.T) {
	csvData := `id,name,age,score,is_active
1,Alice,25,95.5,true
2,Bob,30,87.25,false`

	config := CSVConfig{
		Delimiter: ",",
		HasHeader: true,
		SkipRows:  0,
	}

	stream := types.NewStream("test", "test", nil)
	stream.UpsertField("id", types.Int64, true)
	stream.UpsertField("name", types.String, true)
	stream.UpsertField("age", types.Int32, true)
	stream.UpsertField("score", types.Float32, true)
	stream.UpsertField("is_active", types.Bool, true)

	parser := NewCSVParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(csvData)

	records := []map[string]any{}
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}

	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 2)

	// Check first record
	firstRecord := records[0]
	assert.IsType(t, int64(0), firstRecord["id"], "id should be int64")
	assert.IsType(t, int32(0), firstRecord["age"], "age should be int32")
	assert.IsType(t, float32(0), firstRecord["score"], "score should be float32")
	assert.IsType(t, false, firstRecord["is_active"], "is_active should be bool")
}
