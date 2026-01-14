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
			name:   "numeric value parsed as int64",
			filter: "age >= 18",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "age", Operator: ">=", Value: int64(18)},
				},
			},
		},
		{
			name:   "float value parsed as float64",
			filter: "price > 99.99",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "price", Operator: ">", Value: float64(99.99)},
				},
			},
		},
		{
			name:   "boolean value",
			filter: "is_active = true",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "is_active", Operator: "=", Value: true},
				},
			},
		},
		{
			name:   "quoted string preserved",
			filter: `name = "john doe"`,
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "name", Operator: "=", Value: "john doe"},
				},
			},
		},
		{
			name:   "two conditions with AND",
			filter: "a > 1 and b < 2",
			expected: FilterInput{
				LogicalOperator: "and",
				Conditions: []FilterCondition{
					{Column: "a", Operator: ">", Value: int64(1)},
					{Column: "b", Operator: "<", Value: int64(2)},
				},
			},
		},
		{
			name:   "two conditions with OR uppercase",
			filter: "a = 1 OR b = 2",
			expected: FilterInput{
				LogicalOperator: "OR",
				Conditions: []FilterCondition{
					{Column: "a", Operator: "=", Value: int64(1)},
					{Column: "b", Operator: "=", Value: int64(2)},
				},
			},
		},
		{
			name:        "invalid filter",
			filter:      "a === b",
			expectError: true,
		},
		{
			name:        "too many conditions",
			filter:      "a = 1 and b = 2 and c = 3",
			expectError: true,
		},
		{
			name:   "NULL literal",
			filter: "deleted_at = NULL",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "deleted_at", Operator: "=", Value: "NULL"},
				},
			},
		},
		{
			name:   "negative number",
			filter: "temp < -10",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "temp", Operator: "<", Value: int64(-10)},
				},
			},
		},
		{
			name:   "scientific notation kept as string",
			filter: "value >= 1e3",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{Column: "value", Operator: ">=", Value: "1e3"},
				},
			},
		},
		{
			name:   "very long column name",
			filter: strings.Repeat("col", 100) + " = 1",
			expected: FilterInput{
				Conditions: []FilterCondition{
					{
						Column:   strings.Repeat("col", 100),
						Operator: "=",
						Value:    int64(1),
					},
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
				exp := tt.expected.Conditions[i]
				act := result.Conditions[i]

				assert.Equal(t, exp.Column, act.Column)
				assert.Equal(t, exp.Operator, act.Operator)
				assert.Equal(t, exp.Value, act.Value)
			}
		})
	}
}
