package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguredStream_GetFilter(t *testing.T) {
	tests := []struct {
		name           string
		filter         string
		expectedFilter Filter
		expectError    bool
	}{
		// Checks that an empty filter string yields an empty Filter (no conditions).
		{
			name:   "empty filter",
			filter: "",
			expectedFilter: Filter{
				Conditions:      nil,
				LogicalOperator: "",
			},
			expectError: false,
		},
		// Parses a simple unquoted column and value separated by =.
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
		// Supports double-quoted column identifiers (e.g., contains hyphen).
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
		// Unquoted column names with underscores are valid and commonly used.
		{
			name:   "unquoted column with underscores",
			filter: "user_id != 0 and user_name = john_doe",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "user_id", Operator: "!=", Value: "0"},
					{Column: "user_name", Operator: "=", Value: "john_doe"},
				},
				LogicalOperator: "and",
			},
			expectError: false,
		},

		// Double-quoted column names may contain spaces; quoted values preserved.
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

		// Two conditions joined by 'and' with mixed quoting styles.
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

		// Verifies parsing of comparison operators like >=.
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
		// Rejects nonsense filter strings that don't match expected pattern.
		{
			name:        "invalid filter format",
			filter:      "invalid filter format",
			expectError: true,
		},
		// Error when a quoted value or column has an unclosed double-quote.
		{
			name:        "unclosed quotes",
			filter:      `"unclosed > 5`,
			expectError: true,
		},

		// Reject filters with more than two conditions (not supported).
		{
			name:        "too many conditions",
			filter:      "a > 5 and b < 10 and c = 3",
			expectError: true,
		},
		// Simple comparison without spaces around operator.
		{
			name:   "compact comparison without spaces",
			filter: "a>b",
			expectedFilter: Filter{
				Conditions: []Condition{
					{Column: "a", Operator: ">", Value: "b"},
				},
				LogicalOperator: "",
			},
			expectError: false,
		},
		// Quoted column name and multiple conditions joined by 'and'.
		{
			name:   "mixed quoted and unquoted columns with logical operator",
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

		// Rejects invalid operator sequences like >>>=.
		{
			name:        "invalid operator sequence",
			filter:      `"a" >>>= b`,
			expectError: true,
		},
		// Additional tricky test cases
		// Handles negative numeric values on the right-hand side.
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
		// Accept decimal values that start with a leading dot (e.g. .5).
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
		// Trailing dot in a numeric literal is invalid and should error.
		{
			name:        "decimal number with trailing dot (invalid)",
			filter:      "count = 5.",
			expectError: true,
		},
		// Quoted empty string should be treated as a distinct value.
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

		// Lowercase 'and' should be treated as the logical operator joining two conditions.
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
		// Lowercase 'or' should be parsed as the logical operator.
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
		// Column names that include digits should parse correctly.
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
		// Parser should tolerate and normalize excessive whitespace between tokens.
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
		// No spaces around operators should still parse correctly.
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
		// Quoted value containing spaces should be preserved as a single value.
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

		// Multiple operator types across two conditions.
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
		// >= operator with decimal values.
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
		// <= operator with integer values.
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
		// Quoted column may include dot notation (dot allowed inside quotes).
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

		// Error cases - more tricky failures
		// Uppercase 'AND' should be accepted and preserved.
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
		// Uppercase 'OR' should be accepted and preserved.
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
		// Missing operator between column and value should error.
		{
			name:        "missing operator",
			filter:      "column value",
			expectError: true,
		},
		// Only a column name with no operator/value is invalid.
		{
			name:        "only column name",
			filter:      "column",
			expectError: true,
		},
		// Only an operator token with no operands is invalid.
		{
			name:        "only operator",
			filter:      ">",
			expectError: true,
		},
		// Missing value after operator should error.
		{
			name:        "missing value",
			filter:      "column >",
			expectError: true,
		},
		// Missing column before operator should error.
		{
			name:        "missing column",
			filter:      "> value",
			expectError: true,
		},
		// Mixed logical operators within the same expression are invalid.
		{
			name:        "mixed logical operators",
			filter:      "a = 1 and b = 2 or c = 3",
			expectError: true,
		},
		// Invalid operator token combinations should error (e.g., '><').
		{
			name:        "invalid operator combination",
			filter:      "a >< b",
			expectError: true,
		},
		// '==' is not a supported operator in this filter grammar.
		{
			name:        "equals with double equals (invalid)",
			filter:      "a == b",
			expectError: true,
		},
		// SQL-style not-equal operator '<>' is unsupported and should error.
		{
			name:        "SQL-style not equal (<> not supported)",
			filter:      "a <> b",
			expectError: true,
		},
		// Trailing logical operator without a second condition is invalid.
		{
			name:        "logical operator without second condition",
			filter:      "a = 1 and",
			expectError: true,
		},
		// Leading logical operator without a preceding condition is invalid.
		{
			name:        "logical operator without first condition",
			filter:      "and b = 2",
			expectError: true,
		},
		// Unquoted column names with special characters (hyphen) are invalid.
		{
			name:        "special characters in unquoted column",
			filter:      "user-name = john",
			expectError: true,
		},
		// Spaces in unquoted column names are invalid.
		{
			name:        "space in unquoted column name",
			filter:      "user name = john",
			expectError: true,
		},
		// Dot in an unquoted column name is invalid (use quotes for dot-containing names).
		{
			name:        "dot in unquoted column name",
			filter:      "user.name = john",
			expectError: true,
		},
		// Multiple equals signs are invalid in this grammar.
		{
			name:        "multiple equals signs (invalid)",
			filter:      "a === b",
			expectError: true,
		},
		// Tricky edge cases
		// '+' sign in positive numeric literal is not supported and errors.
		{
			name:        "positive number with + sign",
			filter:      "score > +10",
			expectError: true,
		},
		// Scientific notation values (1e3) should be accepted as numeric values.
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
		// The literal NULL should be recognized as a valid value.
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
		// Edge-case: empty quoted column name should be accepted.
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
		// Escaped quotes inside quoted identifiers/values are not supported here.
		{
			name:        "escaped quote in quoted value (unsupported)",
			filter:      `"col\"name" = "val\"ue"`,
			expectError: true,
		},
		// Non-ASCII characters in unquoted column names are invalid.
		{
			name:        "non-ASCII column name unquoted",
			filter:      "café = oui",
			expectError: true,
		},
		// Trailing garbage after a valid condition should cause an error.
		{
			name:        "trailing garbage after valid condition",
			filter:      "a = b junk",
			expectError: true,
		},
		// Performance/edge-case: very long column names should be handled.
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
		// Comma after logical operator (or stray punctuation) should error.
		{
			name:        "logical operator with excessive spaces and trailing comma",
			filter:      "a > 1   and   , b < 2",
			expectError: true,
		},
		// Mixed-case logical operators (e.g., 'And') are preserved as provided.
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
		// Extra spaces before operator in unquoted column should be tolerated.
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
				assert.Error(t, err, "expected an error but got none")
				return
			}

			require.NoError(t, err, "unexpected error while parsing filter")

			// Check logical operator
			assert.Equal(t, tt.expectedFilter.LogicalOperator, result.LogicalOperator, "LogicalOperator mismatch")

			// Check number of conditions
			require.Len(t, result.Conditions, len(tt.expectedFilter.Conditions), "Conditions length mismatch")

			// Check each condition
			for i, expectedCondition := range tt.expectedFilter.Conditions {
				assert.Equal(t, expectedCondition.Column, result.Conditions[i].Column, "Condition[%d] Column mismatch", i)
				assert.Equal(t, expectedCondition.Operator, result.Conditions[i].Operator, "Condition[%d] Operator mismatch", i)
				assert.Equal(t, expectedCondition.Value, result.Conditions[i].Value, "Condition[%d] Value mismatch", i)
			}
		})
	}
}
