package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValueBytes_ScalarTypes(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		input    any
		expected int64
	}{
		{name: "nil", input: nil, expected: 0},
		{name: "string", input: "hello world", expected: 11},
		{name: "empty string", input: "", expected: 0},
		{name: "bytes", input: []byte("olake"), expected: 5},
		{name: "bool true", input: true, expected: 1},
		{name: "bool false", input: false, expected: 1},
		{name: "int8", input: int8(42), expected: 1},
		{name: "uint8", input: uint8(255), expected: 1},
		{name: "int16", input: int16(1000), expected: 2},
		{name: "uint16", input: uint16(50000), expected: 2},
		{name: "int32", input: int32(100000), expected: 4},
		{name: "uint32", input: uint32(100000), expected: 4},
		{name: "float32", input: float32(3.14), expected: 4},
		{name: "int", input: 123456789, expected: 8},
		{name: "int64", input: int64(123456789), expected: 8},
		{name: "uint", input: uint(123456789), expected: 8},
		{name: "uint64", input: uint64(123456789), expected: 8},
		{name: "float64", input: 3.1415926535, expected: 8},
		{name: "time.Time", input: now, expected: 8},
		{name: "struct fallback", input: struct{ A int }{A: 1}, expected: int64(len("{1}"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := valueBytes(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestValueBytes_NestedAndSlices(t *testing.T) {
	// Nested map and slices
	data := map[string]any{
		"name": "Alice",           // 5
		"age":  int64(30),         // 8
		"tags": []any{"go", "s3"}, // 2 + 2 = 4
		"meta": map[string]any{
			"active": true,        // 1
			"score":  float64(99), // 8
			"inner": []any{
				map[string]any{
					"key": "val", // 3
				},
			},
		},
	}

	// Expected: 5 + 8 + 4 + 1 + 8 + 3 = 29
	assert.Equal(t, int64(29), valueBytes(data))
	assert.Equal(t, int64(29), recordDataBytes(data))
}

func TestValueBytes_DeeplyNestedStructures(t *testing.T) {
	// Build a deeply nested structure (10,000 levels deep)
	// Recursive implementation would risk call-stack overflow, iterative handles it smoothly.
	const depth = 10000
	var root any = "leaf" // 4 bytes

	for i := 0; i < depth; i++ {
		if i%2 == 0 {
			root = map[string]any{"child": root}
		} else {
			root = []any{root}
		}
	}

	got := valueBytes(root)
	assert.Equal(t, int64(4), got)
}

func BenchmarkRecordDataBytes(b *testing.B) {
	record := map[string]any{
		"id":        "123e4567-e89b-12d3-a456-426614174000",
		"user_id":   int64(987654321),
		"is_active": true,
		"rate":      float64(1234.56),
		"tags":      []any{"database", "lakehouse", "iceberg", "s3"},
		"attributes": map[string]any{
			"env":     "production",
			"retries": int32(3),
			"details": map[string]any{
				"city":    "San Francisco",
				"zipcode": "94107",
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = recordDataBytes(record)
	}
}

func BenchmarkRecordDataBytes_DeeplyNested(b *testing.B) {
	const depth = 500
	var root any = "leaf_value"

	for i := 0; i < depth; i++ {
		if i%2 == 0 {
			root = map[string]any{"child": root}
		} else {
			root = []any{root}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = valueBytes(root)
	}
}
