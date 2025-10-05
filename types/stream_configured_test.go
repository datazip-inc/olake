package types

import (
	"strings"
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
		{
			name:   "test case from user: a>b",
			filter: "a>b",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "b"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "test case from user: quoted a with spaces and logical operator",
			filter: `"a" >b and a < c`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "b"},
					{Column: "a", Operator: "<", Value: "c"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "test case from user: backtick a with not equal",
			filter: "`a` != b",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: "!=", Value: "b"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:        "test case from user: invalid operator",
			filter:      `"a" >>>= b`,
			expectError: true,
		},
		// Additional tricky test cases
		{
			name:   "negative number value",
			filter: "temperature < -10",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "temperature", Operator: "<", Value: "-10"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "decimal number with leading dot",
			filter: "ratio >= .5",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "ratio", Operator: ">=", Value: ".5"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:        "decimal number with trailing dot (invalid)",
			filter:      "count = 5.",
			expectError: true,
		},
		{
			name:   "quoted empty string value",
			filter: `name != ""`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "name", Operator: "!=", Value: `""`},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "backtick quoted column with underscores",
			filter: "`user_full_name` = john",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user_full_name", Operator: "=", Value: "john"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "mixed case logical operator - lowercase and",
			filter: "a > 1 and b < 2",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "mixed case logical operator - lowercase or",
			filter: "x = 1 or y = 2",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "x", Operator: "=", Value: "1"},
					{Column: "y", Operator: "=", Value: "2"},
				},
				LogicalOperator: "or",
			},
			expectError: false,
		},
		{
			name:   "column with numbers",
			filter: "column123 = value456",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "column123", Operator: "=", Value: "value456"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "excessive whitespace",
			filter: "  a   >   b   and   c   <   d  ",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "b"},
					{Column: "c", Operator: "<", Value: "d"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "no spaces around operators",
			filter: "a>5and b<10",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "5"},
					{Column: "b", Operator: "<", Value: "10"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "quoted value with spaces",
			filter: `description = "hello world"`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "description", Operator: "=", Value: `"hello world"`},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "backtick value (unusual but valid)",
			filter: "status = `active`",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "status", Operator: "=", Value: "`active`"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "all different operators in sequence",
			filter: "a = 1 and b != 2",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: "=", Value: "1"},
					{Column: "b", Operator: "!=", Value: "2"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},
		{
			name:   "greater than or equal with decimal",
			filter: "price >= 99.99",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "price", Operator: ">=", Value: "99.99"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "less than or equal with integer",
			filter: "age <= 100",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "age", Operator: "<=", Value: "100"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "quoted column with dot notation",
			filter: `"user.email" = "test@example.com"`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user.email", Operator: "=", Value: `"test@example.com"`},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "backtick column with hyphen and dot",
			filter: "`user-data.field` != null",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user-data.field", Operator: "!=", Value: "null"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		// Error cases - more tricky failures
		{
			name:   "uppercase AND operator",
			filter: "a > 1 AND b < 2",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
				LogicalOperator: "AND",
			},
			expectError: false,
		},
		{
			name:   "uppercase OR operator",
			filter: "a > 1 OR b < 2",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
				LogicalOperator: "OR",
			},
			expectError: false,
		},
		{
			name:        "missing operator",
			filter:      "column value",
			expectError: true,
		},
		{
			name:        "only column name",
			filter:      "column",
			expectError: true,
		},
		{
			name:        "only operator",
			filter:      ">",
			expectError: true,
		},
		{
			name:        "missing value",
			filter:      "column >",
			expectError: true,
		},
		{
			name:        "missing column",
			filter:      "> value",
			expectError: true,
		},
		{
			name:        "double quotes mismatch",
			filter:      `"column = value`,
			expectError: true,
		},
		{
			name:        "backtick mismatch",
			filter:      "`column = value",
			expectError: true,
		},
		{
			name:        "three conditions (not supported)",
			filter:      "a = 1 and b = 2 and c = 3",
			expectError: true,
		},
		{
			name:        "mixed logical operators",
			filter:      "a = 1 and b = 2 or c = 3",
			expectError: true,
		},
		{
			name:        "invalid operator combination",
			filter:      "a >< b",
			expectError: true,
		},
		{
			name:        "equals with double equals (invalid)",
			filter:      "a == b",
			expectError: true,
		},
		{
			name:   "SQL-style not equal (valid)",
			filter: "a <> b",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: "<>", Value: "b"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:        "logical operator without second condition",
			filter:      "a = 1 and",
			expectError: true,
		},
		{
			name:        "logical operator without first condition",
			filter:      "and b = 2",
			expectError: true,
		},
		{
			name:        "special characters in unquoted column",
			filter:      "user-name = john",
			expectError: true,
		},
		{
			name:        "space in unquoted column name",
			filter:      "user name = john",
			expectError: true,
		},
		{
			name:        "dot in unquoted column name",
			filter:      "user.name = john",
			expectError: true,
		},
		{
			name:        "triple equals with greater than",
			filter:      "a >=== b",
			expectError: true,
		},
		{
			name:        "four equals with greater than",
			filter:      "a ====> b",
			expectError: true,
		},
		{
			name:        "four equals with less than",
			filter:      "a ====< b",
			expectError: true,
		},
		{
			name:        "triple equals with not equal",
			filter:      "a ===!= b",
			expectError: true,
		},
		{
			name:        "multiple equals signs",
			filter:      "a === b",
			expectError: true,
		},
		{
			name:        "quadruple equals",
			filter:      "a ==== b",
			expectError: true,
		},
		// Tricky edge cases
		{
			name:        "positive number with + sign",
			filter:      "score > +10",
			expectError: true,
		},
		{
			name:   "scientific notation in value",
			filter: "temperature >= 1e3",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "temperature", Operator: ">=", Value: "1e3"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "NULL keyword as value",
			filter: "status = NULL",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "status", Operator: "=", Value: "NULL"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:   "empty quoted column",
			filter: `"" = value`,
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "", Operator: "=", Value: "value"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:        "escaped quote in quoted value (unsupported)",
			filter:      `"col\"name" = "val\"ue"`,
			expectError: true,
		},
		{
			name:        "escaped backtick in backtick value (unsupported)",
			filter:      "`col` = `val``",
			expectError: true,
		},
		{
			name:        "non-ASCII column name unquoted",
			filter:      "café = oui",
			expectError: true,
		},
		{
			name:        "trailing garbage after valid condition",
			filter:      "a = b junk",
			expectError: true,
		},
		{
			name:   "very long column name (perf test)",
			filter: strings.Repeat("longcol", 100) + " = 1", // ~600 chars
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: strings.Repeat("longcol", 100), Operator: "=", Value: "1"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		{
			name:        "logical operator with excessive spaces and trailing comma",
			filter:      "a > 1   and   , b < 2",
			expectError: true,
		},
		{
			name:   "uppercase logical with mixed case",
			filter: "a > 1 And b < 2",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
				LogicalOperator: "And",
			},
			expectError: false,
		},
		{
			name:        "empty value after operator",
			filter:      "col = ",
			expectError: true,
		},
		{
			name:   "unquoted column with trailing space before op",
			filter: "col  = 5",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "col", Operator: "=", Value: "5"},
				},
				LogicalOperator: "",
			},
			expectError: false,
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
