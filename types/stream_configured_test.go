package types

import (
	"testing"
)

func TestConfiguredStream_GetFilter(t *testing.T) {
	tests := []struct {
		name           string
		filter         string
		expectedFilter Filter
		expectError    bool
	}{
		{
			name:   "empty filter",
			filter: "",
			expectedFilter: Filter{
				Conditions:      nil,
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "simple unquoted column",
			filter: "status = active",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "status", Operator: "=", Value: "active"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "double quoted column name",
			filter: `"user-id" > 5`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user-id", Operator: ">", Value: "5"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "backtick quoted column name",
			filter: "`order date` = \"2024-01-01\"",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "order date", Operator: "=", Value: "\"2024-01-01\""},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "double quoted column with spaces",
			filter: `"column name" != "some value"`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "column name", Operator: "!=", Value: "\"some value\""},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "backtick quoted column with special chars",
			filter: "`item.price` >= 100.50",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "item.price", Operator: ">=", Value: "100.50"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "two conditions with AND - mixed quotes",
			filter: `"user-id" > 5 and status = "active"`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user-id", Operator: ">", Value: "5"},
					{Column: "status", Operator: "=", Value: "\"active\""},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "two conditions with OR - backticks and quotes",
			filter: "`order date` = \"2024-01-01\" or \"user-type\" != admin",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "order date", Operator: "=", Value: "\"2024-01-01\""},
					{Column: "user-type", Operator: "!=", Value: "admin"},
				},
				LogicalOperator: "or",
			},
			expectError: false,
		},
		{
			name:   "complex example from comment",
			filter: `"user-id" > 5 and ` + "`item.price` <= 100",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user-id", Operator: ">", Value: "5"},
					{Column: "item.price", Operator: "<=", Value: "100"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "all operators test",
			filter: "age >= 18",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "age", Operator: ">=", Value: "18"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:        "invalid filter format",
			filter:      "invalid filter format",
			expectError: true,
		},
		{
			name:        "unclosed quotes",
			filter:      `"unclosed > 5`,
			expectError: true,
		},
		{
			name:        "unclosed backticks",
			filter:      "`unclosed > 5",
			expectError: true,
		},
		{
			name:        "too many conditions",
			filter:      "a > 5 and b < 10 and c = 3",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a ConfiguredStream with the test filter
			cs := &ConfiguredStream{
				StreamMetadata: StreamMetadata{
					Filter: tt.filter,
				},
			}

			result, err := cs.GetFilter()

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Check logical operator
			if result.LogicalOperator != tt.expectedFilter.LogicalOperator {
				t.Errorf("LogicalOperator mismatch: expected %q, got %q", tt.expectedFilter.LogicalOperator, result.LogicalOperator)
			}

			// Check number of conditions
			if len(result.Conditions) != len(tt.expectedFilter.Conditions) {
				t.Errorf("Conditions length mismatch: expected %d, got %d", len(tt.expectedFilter.Conditions), len(result.Conditions))
				return
			}

			// Check each condition
			for i, expectedCondition := range tt.expectedFilter.Conditions {
				if i >= len(result.Conditions) {
					t.Errorf("Missing condition at index %d", i)
					continue
				}

				actualCondition := result.Conditions[i]

				if actualCondition.Column != expectedCondition.Column {
					t.Errorf("Condition[%d] Column mismatch: expected %q, got %q", i, expectedCondition.Column, actualCondition.Column)
				}

				if actualCondition.Operator != expectedCondition.Operator {
					t.Errorf("Condition[%d] Operator mismatch: expected %q, got %q", i, expectedCondition.Operator, actualCondition.Operator)
				}

				if actualCondition.Value != expectedCondition.Value {
					t.Errorf("Condition[%d] Value mismatch: expected %q, got %q", i, expectedCondition.Value, actualCondition.Value)
				}
			}
		})
	}
}

// Helper function to run a quick manual test
func TestGetFilterManual(t *testing.T) {
	testCases := []string{
		`status = "active"`,
		`"user-id" > 5`,
		"`order date` = \"2024-01-01\"",
		`"user-id" > 5 and ` + "`item.price` <= 100",
		`age >= 18 or status != "inactive"`,
	}

	for _, filter := range testCases {
		cs := &ConfiguredStream{
			StreamMetadata: StreamMetadata{
				Filter: filter,
			},
		}

		result, err := cs.GetFilter()
		if err != nil {
			t.Logf("Filter: %s -> ERROR: %v", filter, err)
		} else {
			t.Logf("Filter: %s -> Conditions: %+v, LogicalOp: %s", filter, result.Conditions, result.LogicalOperator)
		}
	}
}
