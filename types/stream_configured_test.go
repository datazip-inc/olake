package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguredStream_GetFilter(t *testing.T) {
	tests := []struct {
		name        string
		filter      string
		expected    FilterInput
		expectError bool
	}{
		{
			name:     "empty filter",
			filter:   "",
			expected: FilterInput{},
		},
		{
			name:   "simple unquoted column",
			filter: "status = active",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "status", Operator: "=", Value: "active"},
				},
			},
		},
		{
			name:   "double quoted column name",
			filter: `"user-id" > 5`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "user-id", Operator: ">", Value: "5"},
				},
			},
		},
		{
			name:   "unquoted column with underscores",
			filter: "user_id != 0 and user_name = john_doe",
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "user_id", Operator: "!=", Value: "0"},
					{Column: "user_name", Operator: "=", Value: "john_doe"},
				},
			},
		},
		{
			name:   "double quoted column with spaces",
			filter: `"column name" != "some value"`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "column name", Operator: "!=", Value: `"some value"`},
				},
			},
		},
		{
			name:   "two conditions with AND - mixed quotes",
			filter: `"user-id" > 5 and status = "active"`,
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "user-id", Operator: ">", Value: "5"},
					{Column: "status", Operator: "=", Value: `"active"`},
				},
			},
		},
		{
			name:   "all operators test",
			filter: "age >= 18",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "age", Operator: ">=", Value: "18"},
				},
			},
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
			name:        "too many conditions",
			filter:      "a > 5 and b < 10 and c = 3",
			expectError: true,
		},
		{
			name:   "compact comparison without spaces",
			filter: "a>b",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "b"},
				},
			},
		},
		{
			name:   "mixed quoted and unquoted columns with logical operator",
			filter: `"a" >b and a < c`,
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "b"},
					{Column: "a", Operator: "<", Value: "c"},
				},
			},
		},
		{
			name:        "invalid operator sequence",
			filter:      `"a" >>>= b`,
			expectError: true,
		},
		{
			name:   "negative number value",
			filter: "temperature < -10",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "temperature", Operator: "<", Value: "-10"},
				},
			},
		},
		{
			name:   "decimal number with leading dot",
			filter: "ratio >= .5",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "ratio", Operator: ">=", Value: ".5"},
				},
			},
		},
		{
			name:        "decimal number with trailing dot invalid",
			filter:      "count = 5.",
			expectError: true,
		},
		{
			name:   "quoted empty string value",
			filter: `name != ""`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "name", Operator: "!=", Value: `""`},
				},
			},
		},
		{
			name:   "lowercase and operator",
			filter: "a > 1 and b < 2",
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
			},
		},
		{
			name:   "lowercase or operator",
			filter: "x = 1 or y = 2",
			expected: FilterInput{
				LogicalOperator: "or",
				Conditions: []FilterCondition{
					{Column: "x", Operator: "=", Value: "1"},
					{Column: "y", Operator: "=", Value: "2"},
				},
			},
		},
		{
			name:   "column with numbers",
			filter: "column123 = value456",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "column123", Operator: "=", Value: "value456"},
				},
			},
		},
		{
			name:   "excessive whitespace",
			filter: "  a   >   b   and   c   <   d  ",
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "b"},
					{Column: "c", Operator: "<", Value: "d"},
				},
			},
		},
		{
			name:   "no spaces around operators",
			filter: "a>5and b<10",
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "5"},
					{Column: "b", Operator: "<", Value: "10"},
				},
			},
		},
		{
			name:   "quoted value with spaces",
			filter: `description = "hello world"`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "description", Operator: "=", Value: `"hello world"`},
				},
			},
		},
		{
			name:   "all different operators in sequence",
			filter: "a = 1 and b != 2",
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "a", Operator: "=", Value: "1"},
					{Column: "b", Operator: "!=", Value: "2"},
				},
			},
		},
		{
			name:   "greater than or equal with decimal",
			filter: "price >= 99.99",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "price", Operator: ">=", Value: "99.99"},
				},
			},
		},
		{
			name:   "less than or equal with integer",
			filter: "age <= 100",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "age", Operator: "<=", Value: "100"},
				},
			},
		},
		{
			name:   "quoted column with dot notation",
			filter: `"user.email" = "test@example.com"`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "user.email", Operator: "=", Value: `"test@example.com"`},
				},
			},
		},
		{
			name:   "uppercase AND operator",
			filter: "a > 1 AND b < 2",
			expected: FilterInput{
				LogicalOperator: "AND",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
			},
		},
		{
			name:   "uppercase OR operator",
			filter: "a > 1 OR b < 2",
			expected: FilterInput{
				LogicalOperator: "OR",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
			},
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
			name:        "double equals invalid",
			filter:      "a == b",
			expectError: true,
		},
		{
			name:        "sql style not equal unsupported",
			filter:      "a <> b",
			expectError: true,
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
			name:        "space in unquoted column",
			filter:      "user name = john",
			expectError: true,
		},
		{
			name:        "dot in unquoted column",
			filter:      "user.name = john",
			expectError: true,
		},
		{
			name:        "multiple equals invalid",
			filter:      "a === b",
			expectError: true,
		},
		{
			name:        "positive number with plus sign",
			filter:      "score > +10",
			expectError: true,
		},
		{
			name:   "scientific notation",
			filter: "temperature >= 1e3",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "temperature", Operator: ">=", Value: "1e3"},
				},
			},
		},
		{
			name:   "NULL literal",
			filter: "status = NULL",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "status", Operator: "=", Value: "NULL"},
				},
			},
		},
		{
			name:   "empty quoted column",
			filter: `"" = value`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "", Operator: "=", Value: "value"},
				},
			},
		},
		{
			name:        "escaped quotes unsupported",
			filter:      `"col\"name" = "val\"ue"`,
			expectError: true,
		},
		{
			name:        "non ascii column unquoted",
			filter:      "café = oui",
			expectError: true,
		},
		{
			name:        "trailing garbage",
			filter:      "a = b junk",
			expectError: true,
		},
		{
			name:   "very long column name",
			filter: strings.Repeat("longcol", 100) + " = 1",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: strings.Repeat("longcol", 100), Operator: "=", Value: "1"},
				},
			},
		},
		{
			name:        "logical operator with trailing comma",
			filter:      "a > 1   and   , b < 2",
			expectError: true,
		},
		{
			name:   "mixed case logical operator",
			filter: "a > 1 And b < 2",
			expected: FilterInput{
				LogicalOperator: "And",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: "1"},
					{Column: "b", Operator: "<", Value: "2"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := &ConfiguredStream{
				StreamMetadata: StreamMetadata{
					Filter: tt.filter,
				},
			}

			result, _, err := cs.GetFilter()

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			assert.Equal(t, tt.expected.LogicalOperator, result.LogicalOperator)
			require.Len(t, result.Conditions, len(tt.expected.Conditions))

			for i := range tt.expected.Conditions {
				assert.Equal(t, tt.expected.Conditions[i], result.Conditions[i])
			}
		})
	}
}
