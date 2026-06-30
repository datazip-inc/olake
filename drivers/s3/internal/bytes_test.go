package driver

import (
	"testing"
	"time"
)

func TestRecordDataBytes(t *testing.T) {
	cases := []struct {
		name   string
		record map[string]any
		want   int64
	}{
		{"empty", map[string]any{}, 0},
		{"string", map[string]any{"name": "hello"}, 5}, // len("hello")
		{"int+float+bool", map[string]any{"a": 7, "b": 3.5, "c": true}, 8 + 8 + 1},
		{"nil value not counted", map[string]any{"x": nil, "y": "ab"}, 2},
		{"bytes", map[string]any{"blob": []byte{1, 2, 3, 4}}, 4},
		{"nested map", map[string]any{"m": map[string]any{"s": "abc", "n": int32(1)}}, 3 + 4},
		{"slice", map[string]any{"arr": []any{"a", "bb", 5}}, 1 + 2 + 8},
		{"time", map[string]any{"t": time.Now()}, 8},
		// keys are NOT counted (schema, not data): two string values total their lengths only
		{"keys excluded", map[string]any{"longcolumnname": "x", "another": "yy"}, 1 + 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := recordDataBytes(c.record); got != c.want {
				t.Fatalf("recordDataBytes(%v) = %d, want %d", c.record, got, c.want)
			}
		})
	}
}
