package waljs

import (
	"encoding/json"
	"testing"

	"github.com/datazip-inc/olake/types"
)

func TestBigIntHandling(t *testing.T) {
	// Mock converter function
	converter := func(value interface{}, columnType string) (interface{}, error) {
		return value, nil
	}

	// Create a mock stream
	mockStream := &mockStream{
		name:      "test_table",
		namespace: "public",
		schema:    types.NewTypeSchema(),
	}
	filter := NewChangeFilter(converter, mockStream)

	// Test cases for different bigint representations
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
			// Create WAL message
			msg := map[string]interface{}{
				"change": []map[string]interface{}{
					{
						"kind":         "insert",
						"schema":       "public",
						"table":        "test_table",
						"columnnames":  []string{"id"},
						"columntypes":  []string{"bigint"},
						"columnvalues": []interface{}{tc.input},
					},
				},
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				t.Fatalf("Failed to marshal test message: %v", err)
			}

			// Process the message
			var result map[string]interface{}
			err = filter.FilterChange(0, msgBytes, func(change CDCChange) error {
				result = change.Data
				return nil
			})

			// Verify results
			if err != nil {
				t.Errorf("FilterChange failed: %v", err)
			}

			val, ok := result["id"].(int64)
			if !ok {
				t.Errorf("Expected int64, got %T", result["id"])
			}
			if val != tc.expected {
				t.Errorf("Expected %d, got %d", tc.expected, val)
			}
		})
	}
}

// Mock stream implementation
type mockStream struct {
	name      string
	namespace string
	schema    *types.TypeSchema
}

func (m *mockStream) Name() string      { return m.name }
func (m *mockStream) Namespace() string { return m.namespace }
func (m *mockStream) ID() string        { return m.namespace + "." + m.name }
func (m *mockStream) Self() *types.ConfiguredStream {
	return &types.ConfiguredStream{
		Stream: &types.Stream{
			Name:      m.name,
			Namespace: m.namespace,
			Schema:    m.schema,
		},
	}
}
func (m *mockStream) GetStream() *types.Stream    { return nil }
func (m *mockStream) Schema() *types.TypeSchema   { return m.schema }
func (m *mockStream) Cursor() string              { return "" }
func (m *mockStream) GetSyncMode() types.SyncMode { return types.CDC }
func (m *mockStream) SupportedSyncModes() *types.Set[types.SyncMode] {
	return types.NewSet(types.CDC)
}
func (m *mockStream) Validate(source *types.Stream) error { return nil }
