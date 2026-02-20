package typeutils

import (
	"context"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helper functions
// ─────────────────────────────────────────────────────────────────────────────

func makeRecord(data map[string]any) types.RawRecord {
	return types.CreateRawRecord(data, map[string]any{
		constants.OpType: "c",
	})
}

func makeIcebergSchema(cols map[string]string) map[string]string {
	return cols
}

func makeParquetSchema(cols map[string]types.DataType) Fields {
	fields := Fields{}
	for name, dt := range cols {
		fields[name] = NewField(dt)
	}
	return fields
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Legacy Filter (should skip filtering)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_LegacyFilter(t *testing.T) {
	ctx := context.Background()
	records := []types.RawRecord{
		makeRecord(map[string]any{"id": int64(1), "name": "Alice"}),
		makeRecord(map[string]any{"id": int64(2), "name": "Bob"}),
	}

	// Even with conditions, legacy=true should skip filtering
	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions: []types.FilterCondition{
			{Column: "id", Operator: "=", Value: 1},
		},
	}
	result, err := FilterRecords(ctx, records, filter, true)
	require.NoError(t, err)
	assert.Len(t, result, 2, "legacy filter should return all records unchanged")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Empty Conditions (should return all records)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_EmptyConditions(t *testing.T) {
	ctx := context.Background()
	records := []types.RawRecord{
		makeRecord(map[string]any{"id": int64(1)}),
		makeRecord(map[string]any{"id": int64(2)}),
		makeRecord(map[string]any{"id": int64(3)}),
	}

	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions:      []types.FilterCondition{},
	}
	result, err := FilterRecords(ctx, records, filter, false)
	require.NoError(t, err)
	assert.Len(t, result, 3, "empty conditions should return all records")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Empty Records (should return empty)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_EmptyRecords(t *testing.T) {
	ctx := context.Background()
	records := []types.RawRecord{}

	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions: []types.FilterCondition{
			{Column: "id", Operator: "=", Value: 1},
		},
	}

	result, err := FilterRecords(ctx, records, filter, false)
	require.NoError(t, err)
	assert.Len(t, result, 0, "empty input should return empty output")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Column Name Case Variations (Iceberg)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_ColumnNameCases_Iceberg(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		filterColumn  string
		schemaColumn  string
		recordColumn  string
		expectedCount int
	}{
		{
			name:          "lowercase filter Column",
			filterColumn:  "user_id",
			schemaColumn:  "user_id",
			recordColumn:  "user_id",
			expectedCount: 1,
		},
		{
			name:          "UPPERCASE filter Column gets lowercased",
			filterColumn:  "USER_ID",
			schemaColumn:  "user_id",
			recordColumn:  "user_id",
			expectedCount: 1,
		},
		{
			name:          "MixedCase filter Column gets lowercased",
			filterColumn:  "UserId",
			schemaColumn:  "userid",
			recordColumn:  "userid",
			expectedCount: 1,
		},
		{
			name:          "CamelCase filter Column gets lowercased",
			filterColumn:  "userId",
			schemaColumn:  "userid",
			recordColumn:  "userid",
			expectedCount: 1,
		},
		{
			name:          "special chars replaced with underscore",
			filterColumn:  "user.id",
			schemaColumn:  "user_id",
			recordColumn:  "user_id",
			expectedCount: 1,
		},
		{
			name:          "multiple special chars",
			filterColumn:  "user@id#name",
			schemaColumn:  "user_id_name",
			recordColumn:  "user_id_name",
			expectedCount: 1,
		},
		{
			name:          "spaces replaced with underscore",
			filterColumn:  "user id",
			schemaColumn:  "user_id",
			recordColumn:  "user_id",
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := []types.RawRecord{
				makeRecord(map[string]any{tt.recordColumn: int64(100)}),
				makeRecord(map[string]any{tt.recordColumn: int64(200)}),
			}

			filter := types.FilterInput{
				LogicalOperator: "AND",
				Conditions: []types.FilterCondition{
					{Column: tt.filterColumn, Operator: "=", Value: 100},
				},
			}

			result, err := FilterRecords(ctx, records, filter, false)
			require.NoError(t, err)
			assert.Len(t, result, tt.expectedCount)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: All Operators with Integer Types (Iceberg)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_Operators_Integer_Iceberg(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"Value": int64(10)}),
		makeRecord(map[string]any{"Value": int64(20)}),
		makeRecord(map[string]any{"Value": int64(30)}),
		makeRecord(map[string]any{"Value": int64(40)}),
		makeRecord(map[string]any{"Value": int64(50)}),
	}

	tests := []struct {
		name          string
		Operator      string
		filterValue   any
		expectedCount int
	}{
		{"equals", "=", 30, 1},
		{"not equals", "!=", 30, 4},
		{"greater than", ">", 30, 2},
		{"greater than or equal", ">=", 30, 3},
		{"less than", "<", 30, 2},
		{"less than or equal", "<=", 30, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := types.FilterInput{
				LogicalOperator: "AND",
				Conditions: []types.FilterCondition{
					{Column: "Value", Operator: tt.Operator, Value: tt.filterValue},
				},
			}

			result, err := FilterRecords(ctx, records, filter, false)
			require.NoError(t, err)
			assert.Len(t, result, tt.expectedCount)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: All Iceberg Data Types
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_AllIcebergTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("boolean", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"active": true}),
			makeRecord(map[string]any{"active": false}),
			makeRecord(map[string]any{"active": true}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "active", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("int (int32)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"count": int32(5)}),
			makeRecord(map[string]any{"count": int32(10)}),
			makeRecord(map[string]any{"count": int32(15)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "count", Operator: ">=", Value: 10},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("long (int64)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"bignum": int64(1000000)}),
			makeRecord(map[string]any{"bignum": int64(2000000)}),
			makeRecord(map[string]any{"bignum": int64(3000000)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "bignum", Operator: "<", Value: 2500000},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("float (float32)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"price": float32(19.99)}),
			makeRecord(map[string]any{"price": float32(29.99)}),
			makeRecord(map[string]any{"price": float32(39.99)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "price", Operator: "<=", Value: 29.99},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("double (float64)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"amount": float64(100.50)}),
			makeRecord(map[string]any{"amount": float64(200.75)}),
			makeRecord(map[string]any{"amount": float64(300.25)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "amount", Operator: ">", Value: 150.0},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("string", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"name": "Alice"}),
			makeRecord(map[string]any{"name": "Bob"}),
			makeRecord(map[string]any{"name": "Charlie"}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "name", Operator: "=", Value: "Bob"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "Bob", result[0].Data["name"])
	})

	t.Run("timestamptz", func(t *testing.T) {
		now := time.Now().UTC()
		past := now.Add(-24 * time.Hour)
		future := now.Add(24 * time.Hour)

		records := []types.RawRecord{
			makeRecord(map[string]any{"created_at": past}),
			makeRecord(map[string]any{"created_at": now}),
			makeRecord(map[string]any{"created_at": future}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "created_at", Operator: ">=", Value: now.Format(time.RFC3339)},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: All Parquet Data Types
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_AllParquetTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("boolean", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"is_active": true}),
			makeRecord(map[string]any{"is_active": false}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "is_active", Operator: "=", Value: false},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("int32", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"age": int32(25)}),
			makeRecord(map[string]any{"age": int32(35)}),
			makeRecord(map[string]any{"age": int32(45)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "age", Operator: "!=", Value: 35},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("int64", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"id": int64(1)}),
			makeRecord(map[string]any{"id": int64(2)}),
			makeRecord(map[string]any{"id": int64(3)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "id", Operator: "<=", Value: 2},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("float32", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"temp": float32(36.5)}),
			makeRecord(map[string]any{"temp": float32(37.0)}),
			makeRecord(map[string]any{"temp": float32(38.5)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "temp", Operator: ">", Value: 37.0},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("float64", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"balance": float64(1000.00)}),
			makeRecord(map[string]any{"balance": float64(2500.50)}),
			makeRecord(map[string]any{"balance": float64(5000.75)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "balance", Operator: ">=", Value: 2000.0},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("string", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"status": "pending"}),
			makeRecord(map[string]any{"status": "active"}),
			makeRecord(map[string]any{"status": "inactive"}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "!=", Value: "pending"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("timestamp", func(t *testing.T) {
		baseTime := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

		records := []types.RawRecord{
			makeRecord(map[string]any{"updated_at": baseTime}),
			makeRecord(map[string]any{"updated_at": baseTime.Add(2 * time.Hour)}),
			makeRecord(map[string]any{"updated_at": baseTime.Add(4 * time.Hour)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "updated_at", Operator: "<", Value: baseTime.Add(3 * time.Hour).Format(time.RFC3339)},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: AND Logic with Multiple Conditions
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_ANDLogic(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"age": int64(25), "city": "NYC", "active": true}),
		makeRecord(map[string]any{"age": int64(30), "city": "LA", "active": true}),
		makeRecord(map[string]any{"age": int64(35), "city": "NYC", "active": false}),
		makeRecord(map[string]any{"age": int64(40), "city": "NYC", "active": true}),
		makeRecord(map[string]any{"age": int64(28), "city": "LA", "active": false}),
	}

	t.Run("two conditions AND", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "city", Operator: "=", Value: "NYC"},
				{Column: "active", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2) // NYC + active: record 0 and 3
	})

	t.Run("three conditions AND", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "city", Operator: "=", Value: "NYC"},
				{Column: "active", Operator: "=", Value: true},
				{Column: "age", Operator: ">=", Value: 30},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1) // Only record 3 matches all three
	})

	t.Run("empty logical Operator defaults to AND", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "", // empty should default to AND
			Conditions: []types.FilterCondition{
				{Column: "city", Operator: "=", Value: "NYC"},
				{Column: "active", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: OR Logic with Multiple Conditions
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_ORLogic(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"department": "Engineering", "level": int64(3)}),
		makeRecord(map[string]any{"department": "Marketing", "level": int64(2)}),
		makeRecord(map[string]any{"department": "Sales", "level": int64(4)}),
		makeRecord(map[string]any{"department": "HR", "level": int64(1)}),
		makeRecord(map[string]any{"department": "Engineering", "level": int64(5)}),
	}

	t.Run("two conditions OR", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "OR",
			Conditions: []types.FilterCondition{
				{Column: "department", Operator: "=", Value: "Engineering"},
				{Column: "level", Operator: ">=", Value: 4},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 3) // Engineering (2) + Sales level 4 (1, already unique)
	})

	t.Run("lowercase or", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "or",
			Conditions: []types.FilterCondition{
				{Column: "department", Operator: "=", Value: "HR"},
				{Column: "department", Operator: "=", Value: "Marketing"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("mixed case Or", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "Or",
			Conditions: []types.FilterCondition{
				{Column: "level", Operator: "=", Value: 1},
				{Column: "level", Operator: "=", Value: 5},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Null Value Handling
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_NullValues(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"name": "Alice", "age": int64(30)}),
		makeRecord(map[string]any{"name": nil, "age": int64(25)}),
		makeRecord(map[string]any{"name": "Bob", "age": nil}),
		makeRecord(map[string]any{"name": nil, "age": nil}),
	}

	t.Run("filter null equals null", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "name", Operator: "=", Value: nil},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2) // records with nil name
	})

	t.Run("filter null not equals", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "name", Operator: "!=", Value: nil},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2) // records with non-nil name
	})

	t.Run("comparison with null returns false", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "age", Operator: ">", Value: 20},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2) // only records with non-nil age that is > 20
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: CDC-Safe Behavior (Missing Columns)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_CDCMissingColumns(t *testing.T) {
	ctx := context.Background()

	// CDC records might have partial data
	records := []types.RawRecord{
		makeRecord(map[string]any{"id": int64(1), "name": "Alice", "status": "active"}),
		makeRecord(map[string]any{"id": int64(2), "name": "Bob"}),        // missing "status"
		makeRecord(map[string]any{"id": int64(3), "status": "inactive"}), // missing "name"
		makeRecord(map[string]any{"id": int64(4)}),                       // only id
	}

	t.Run("AND with missing Column returns false", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "active"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		// Record 0: has status=active → matches
		// Record 1: missing status → no match (missing Column = false)
		// Record 2: has status=inactive → doesn't match
		// Record 3: missing status → no match (missing Column = false)
		assert.Len(t, result, 1)
	})

	t.Run("OR with missing Column returns false for that condition", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "OR",
			Conditions: []types.FilterCondition{
				{Column: "name", Operator: "=", Value: "Alice"},
				{Column: "status", Operator: "=", Value: "inactive"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		// Record 0: name=Alice → matches OR
		// Record 1: name=Bob (fails), status missing (false) → no match
		// Record 2: name missing (false), status=inactive → matches OR
		// Record 3: both missing (false for both) → no match
		assert.Len(t, result, 2)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Unknown Column Type Error
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_UnknownColumnType(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"id": int64(1)}),
	}

	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions: []types.FilterCondition{
			{Column: "unknown_column", Operator: "=", Value: 1},
		},
	}
	// Schema doesn't have "unknown_column"

	_, err := FilterRecords(ctx, records, filter, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown datatype")
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: String Comparison Operators
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_StringComparison(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"name": "Alice"}),
		makeRecord(map[string]any{"name": "Bob"}),
		makeRecord(map[string]any{"name": "Charlie"}),
		makeRecord(map[string]any{"name": "David"}),
	}

	tests := []struct {
		name          string
		Operator      string
		filterValue   string
		expectedCount int
	}{
		{"string equals", "=", "Bob", 1},
		{"string not equals", "!=", "Bob", 3},
		{"string greater than", ">", "Bob", 2},      // Charlie, David
		{"string greater or equal", ">=", "Bob", 3}, // Bob, Charlie, David
		{"string less than", "<", "Charlie", 2},     // Alice, Bob
		{"string less or equal", "<=", "Bob", 2},    // Alice, Bob
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := types.FilterInput{
				LogicalOperator: "AND",
				Conditions: []types.FilterCondition{
					{Column: "name", Operator: tt.Operator, Value: tt.filterValue},
				},
			}

			result, err := FilterRecords(ctx, records, filter, false)
			require.NoError(t, err)
			assert.Len(t, result, tt.expectedCount)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Boolean Comparison
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_BooleanComparison(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"enabled": true}),
		makeRecord(map[string]any{"enabled": false}),
		makeRecord(map[string]any{"enabled": true}),
		makeRecord(map[string]any{"enabled": false}),
	}

	t.Run("boolean equals true", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "enabled", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("boolean equals false", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "enabled", Operator: "=", Value: false},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("boolean not equals", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "enabled", Operator: "!=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2)
	})

	t.Run("boolean greater than (false < true)", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "enabled", Operator: ">", Value: false},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 2) // true > false
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Type Coercion from Filter Values
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_TypeCoercion(t *testing.T) {
	ctx := context.Background()

	t.Run("string filter Value to int64", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"id": int64(100)}),
			makeRecord(map[string]any{"id": int64(200)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "id", Operator: "=", Value: "100"}, // string Value
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("float filter Value to int32", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"count": int32(10)}),
			makeRecord(map[string]any{"count": int32(20)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "count", Operator: "=", Value: 10.0}, // float Value
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("int filter Value to float64", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"price": float64(99.99)}),
			makeRecord(map[string]any{"price": float64(100.0)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "price", Operator: "=", Value: 100}, // int Value
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("string boolean Value", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"active": true}),
			makeRecord(map[string]any{"active": false}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "active", Operator: "=", Value: "true"}, // string "true"
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Unsupported Operator
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_UnsupportedOperator(t *testing.T) {
	ctx := context.Background()

	records := []types.RawRecord{
		makeRecord(map[string]any{"name": "Alice"}),
		makeRecord(map[string]any{"name": "Bob"}),
	}

	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions: []types.FilterCondition{
			{Column: "name", Operator: "LIKE", Value: "A%"}, // unsupported
		},
	}

	result, err := FilterRecords(ctx, records, filter, false)
	require.NoError(t, err)
	assert.Len(t, result, 0) // unsupported Operator returns false for all
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Complex Multi-Column Scenario
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_ComplexScenario(t *testing.T) {
	ctx := context.Background()

	// Simulating a real-world e-commerce order dataset
	records := []types.RawRecord{
		makeRecord(map[string]any{
			"order_id":    int64(1001),
			"customer_id": int64(501),
			"total":       float64(150.50),
			"status":      "completed",
			"priority":    int32(1),
			"is_express":  true,
		}),
		makeRecord(map[string]any{
			"order_id":    int64(1002),
			"customer_id": int64(502),
			"total":       float64(75.00),
			"status":      "pending",
			"priority":    int32(2),
			"is_express":  false,
		}),
		makeRecord(map[string]any{
			"order_id":    int64(1003),
			"customer_id": int64(501),
			"total":       float64(200.00),
			"status":      "completed",
			"priority":    int32(1),
			"is_express":  true,
		}),
		makeRecord(map[string]any{
			"order_id":    int64(1004),
			"customer_id": int64(503),
			"total":       float64(50.25),
			"status":      "canceled",
			"priority":    int32(3),
			"is_express":  false,
		}),
		makeRecord(map[string]any{
			"order_id":    int64(1005),
			"customer_id": int64(501),
			"total":       float64(300.00),
			"status":      "pending",
			"priority":    int32(1),
			"is_express":  true,
		}),
	}

	t.Run("high Value express orders", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "total", Operator: ">=", Value: 100},
				{Column: "is_express", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 3) // orders 1001, 1003, 1005
	})

	t.Run("completed or canceled orders", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "OR",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "completed"},
				{Column: "status", Operator: "=", Value: "canceled"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 3) // orders 1001, 1003, 1004
	})

	t.Run("customer 501 high priority", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "customer_id", Operator: "=", Value: 501},
				{Column: "priority", Operator: "=", Value: 1},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 3) // orders 1001, 1003, 1005
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: evaluate function
// ─────────────────────────────────────────────────────────────────────────────

func TestEvaluate(t *testing.T) {
	t.Run("nil handling", func(t *testing.T) {
		assert.True(t, evaluate(nil, nil, "="))
		assert.False(t, evaluate(nil, nil, "!="))
		assert.True(t, evaluate("Value", nil, "!="))
		assert.True(t, evaluate(nil, "Value", "!="))
		assert.False(t, evaluate(nil, "Value", "="))
		assert.False(t, evaluate(nil, "Value", ">"))
	})

	t.Run("integer comparison", func(t *testing.T) {
		assert.True(t, evaluate(int64(10), int64(10), "="))
		assert.True(t, evaluate(int64(10), int64(5), "!="))
		assert.True(t, evaluate(int64(10), int64(5), ">"))
		assert.True(t, evaluate(int64(10), int64(10), ">="))
		assert.True(t, evaluate(int64(5), int64(10), "<"))
		assert.True(t, evaluate(int64(10), int64(10), "<="))
	})

	t.Run("float comparison", func(t *testing.T) {
		assert.True(t, evaluate(float64(10.5), float64(10.5), "="))
		assert.True(t, evaluate(float64(10.5), float64(5.5), ">"))
		assert.True(t, evaluate(float64(5.5), float64(10.5), "<"))
	})

	t.Run("string comparison", func(t *testing.T) {
		assert.True(t, evaluate("abc", "abc", "="))
		assert.True(t, evaluate("xyz", "abc", ">"))
		assert.True(t, evaluate("abc", "xyz", "<"))
	})

	t.Run("boolean comparison", func(t *testing.T) {
		assert.True(t, evaluate(true, true, "="))
		assert.True(t, evaluate(false, false, "="))
		assert.True(t, evaluate(true, false, "!="))
		assert.True(t, evaluate(true, false, ">")) // true > false
		assert.True(t, evaluate(false, true, "<")) // false < true
	})

	t.Run("unsupported Operator", func(t *testing.T) {
		assert.False(t, evaluate("abc", "abc", "LIKE"))
		assert.False(t, evaluate(10, 10, "BETWEEN"))
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: matches function
// ─────────────────────────────────────────────────────────────────────────────

func TestMatches(t *testing.T) {
	t.Run("AND with all conditions matching", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []types.FilterCondition{
			{Column: "a", Operator: "=", Value: int64(10)},
			{Column: "b", Operator: "=", Value: "test"},
		}
		assert.True(t, matches(record, conditions, "AND"))
	})

	t.Run("AND with one condition failing", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []types.FilterCondition{
			{Column: "a", Operator: "=", Value: int64(10)},
			{Column: "b", Operator: "=", Value: "other"},
		}
		assert.False(t, matches(record, conditions, "AND"))
	})

	t.Run("OR with one condition matching", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []types.FilterCondition{
			{Column: "a", Operator: "=", Value: int64(999)},
			{Column: "b", Operator: "=", Value: "test"},
		}
		assert.True(t, matches(record, conditions, "OR"))
	})

	t.Run("OR with no conditions matching", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []types.FilterCondition{
			{Column: "a", Operator: "=", Value: int64(999)},
			{Column: "b", Operator: "=", Value: "other"},
		}
		assert.False(t, matches(record, conditions, "OR"))
	})

	t.Run("missing Column in AND returns false", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10)}) // no "b"
		conditions := []types.FilterCondition{
			{Column: "a", Operator: "=", Value: int64(10)},
			{Column: "b", Operator: "=", Value: "test"}, // missing
		}
		assert.False(t, matches(record, conditions, "AND"))
	})

	t.Run("empty conditions with AND returns true", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10)})
		conditions := []types.FilterCondition{}
		assert.True(t, matches(record, conditions, "AND"))
	})

	t.Run("empty conditions with OR returns false", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10)})
		conditions := []types.FilterCondition{}
		assert.False(t, matches(record, conditions, "OR"))
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Database-Specific Column Name Patterns
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_DatabaseColumnPatterns(t *testing.T) {
	ctx := context.Background()

	// Test patterns commonly seen in different databases

	t.Run("PostgreSQL style (snake_case)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"user_account_id": int64(1)}),
			makeRecord(map[string]any{"user_account_id": int64(2)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "user_account_id", Operator: "=", Value: 1},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("MySQL style (mixed)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"userid": int64(1)}),
			makeRecord(map[string]any{"userid": int64(2)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "UserID", Operator: "=", Value: 1}, // uppercase input
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("Oracle style (UPPERCASE)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"customer_name": "John"}),
			makeRecord(map[string]any{"customer_name": "Jane"}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "CUSTOMER_NAME", Operator: "=", Value: "John"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("SQL Server style (brackets removed, special chars)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"_order_date_": "2025-01-15"}),
			makeRecord(map[string]any{"_order_date_": "2025-01-16"}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "[Order Date]", Operator: "=", Value: "2025-01-15"},
			},
		}
		// After Reformat: [Order Date] → _order_date_

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("MongoDB style (nested path dots)", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"address_city": "NYC"}),
			makeRecord(map[string]any{"address_city": "LA"}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "address.city", Operator: "=", Value: "NYC"}, // dot notation
			},
		}
		// After Reformat: address.city → address_city

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Concurrent Filtering Performance Sanity Check
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_LargeDataset(t *testing.T) {
	ctx := context.Background()

	// Generate a larger dataset
	records := make([]types.RawRecord, 1000)
	for i := 0; i < 1000; i++ {
		records[i] = makeRecord(map[string]any{
			"id":       int64(i),
			"category": []string{"A", "B", "C", "D", "E"}[i%5],
			"Value":    float64(i * 10),
		})
	}

	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions: []types.FilterCondition{
			{Column: "category", Operator: "=", Value: "A"},
			{Column: "Value", Operator: ">=", Value: 500},
		},
	}

	result, err := FilterRecords(ctx, records, filter, false)
	require.NoError(t, err)

	// Category A appears at indices 0, 5, 10, 15, ... (every 5th)
	// Value >= 500 means id >= 50
	// So we want category A with id >= 50: 50, 55, 60, 65, ..., 995
	// That's (1000 - 50) / 5 = 190 records
	expectedCount := 0
	for i := 50; i < 1000; i += 5 {
		expectedCount++
	}
	assert.Len(t, result, expectedCount)
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Delete Operations Always Synced (CDC-Safe Behavior)
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_DeleteOperationsAlwaysSynced(t *testing.T) {
	ctx := context.Background()

	t.Run("delete operations bypass filter conditions", func(t *testing.T) {
		// Create records with different operation types
		// Filter condition: status = "active" (would normally exclude "inactive" records)
		records := []types.RawRecord{
			types.CreateRawRecord(map[string]any{"status": "active", "id": int64(1)}, map[string]any{constants.OlakeID: "id1", constants.OpType: "c"}),   // create - should match filter
			types.CreateRawRecord(map[string]any{"status": "inactive", "id": int64(2)}, map[string]any{constants.OlakeID: "id2", constants.OpType: "c"}), // create - should NOT match filter
			types.CreateRawRecord(map[string]any{"status": "active", "id": int64(3)}, map[string]any{constants.OlakeID: "id3", constants.OpType: "u"}),   // update - should match filter
			types.CreateRawRecord(map[string]any{"status": "inactive", "id": int64(4)}, map[string]any{constants.OlakeID: "id4", constants.OpType: "u"}), // update - should NOT match filter
			types.CreateRawRecord(map[string]any{"status": "active", "id": int64(5)}, map[string]any{constants.OlakeID: "id5", constants.OpType: "d"}),   // delete - should ALWAYS match (bypass filter)
			types.CreateRawRecord(map[string]any{"status": "inactive", "id": int64(6)}, map[string]any{constants.OlakeID: "id6", constants.OpType: "d"}), // delete - should ALWAYS match (bypass filter)
			types.CreateRawRecord(map[string]any{"id": int64(7)}, map[string]any{constants.OlakeID: "id7", constants.OpType: "d"}),                       // delete with missing status - should ALWAYS match
		}

		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "active"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)

		// Expected results:
		// - id1: create with status="active" → matches filter ✓
		// - id2: create with status="inactive" → filtered out ✗
		// - id3: update with status="active" → matches filter ✓
		// - id4: update with status="inactive" → filtered out ✗
		// - id5: delete with status="active" → always synced ✓
		// - id6: delete with status="inactive" → always synced ✓
		// - id7: delete with missing status → always synced ✓
		// Total: 5 records (2 creates/updates + 3 deletes)
		assert.Len(t, result, 5, "delete operations should always be synced regardless of filter")

		// Verify all delete operations are present
		deleteOps := 0
		for _, r := range result {
			if r.OlakeColumns[constants.OpType] == "d" {
				deleteOps++
			}
		}
		assert.Equal(t, 3, deleteOps, "all 3 delete operations should be included")

		// Verify non-delete operations are filtered correctly
		nonDeleteOps := 0
		for _, r := range result {
			if r.OlakeColumns[constants.OpType] != "d" {
				nonDeleteOps++
				assert.Equal(t, "active", r.Data["status"], "non-delete operations should match filter")
			}
		}
		assert.Equal(t, 2, nonDeleteOps, "only 2 non-delete operations should match filter")
	})

	t.Run("delete operations with complex AND filter", func(t *testing.T) {
		// Complex filter that would exclude most records
		records := []types.RawRecord{
			types.CreateRawRecord(map[string]any{"age": int64(25), "city": "NYC"}, map[string]any{constants.OlakeID: "id1", constants.OpType: "c"}),
			types.CreateRawRecord(map[string]any{"age": int64(20), "city": "LA"}, map[string]any{constants.OlakeID: "id2", constants.OpType: "d"}),  // delete - should bypass
			types.CreateRawRecord(map[string]any{"age": int64(30), "city": "NYC"}, map[string]any{constants.OlakeID: "id3", constants.OpType: "d"}), // delete - should bypass
		}

		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "age", Operator: ">=", Value: 30},
				{Column: "city", Operator: "=", Value: "NYC"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)

		// Expected: id1 filtered out (age=25 < 30), id2 and id3 are deletes so always included
		assert.Len(t, result, 2, "only delete operations should be included")
		for _, r := range result {
			assert.Equal(t, "d", r.OlakeColumns[constants.OpType], "all results should be delete operations")
		}
	})

	t.Run("delete operations with OR filter", func(t *testing.T) {
		records := []types.RawRecord{
			types.CreateRawRecord(map[string]any{"status": "pending"}, map[string]any{constants.OlakeID: "id1", constants.OpType: "c"}),
			types.CreateRawRecord(map[string]any{"status": "canceled"}, map[string]any{constants.OlakeID: "id2", constants.OpType: "d"}), // delete - should bypass
			types.CreateRawRecord(map[string]any{"status": "completed"}, map[string]any{constants.OlakeID: "id3", constants.OpType: "c"}),
		}

		filter := types.FilterInput{
			LogicalOperator: "OR",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "pending"},
				{Column: "status", Operator: "=", Value: "completed"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)

		// Expected: id1 matches (pending), id2 is delete (always included), id3 matches (completed)
		assert.Len(t, result, 3, "all records should be included (2 match OR, 1 is delete)")
	})

	t.Run("delete operations with missing filter columns", func(t *testing.T) {
		// Delete operations in MongoDB CDC only contain document key, not full fields
		// This simulates that scenario
		records := []types.RawRecord{
			types.CreateRawRecord(map[string]any{"_id": "doc1"}, map[string]any{constants.OlakeID: "id1", constants.OpType: "d"}), // delete with only key
			types.CreateRawRecord(map[string]any{"_id": "doc2"}, map[string]any{constants.OlakeID: "id2", constants.OpType: "d"}), // delete with only key
			types.CreateRawRecord(map[string]any{"status": "active", "_id": "doc3"}, map[string]any{constants.OlakeID: "id3", constants.OpType: "c"}),
		}

		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "active"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false)
		require.NoError(t, err)

		// Expected: id1 and id2 are deletes (always included), id3 matches filter
		assert.Len(t, result, 3, "delete operations should be included even without filter columns")
		for _, r := range result {
			if r.OlakeColumns[constants.OpType] == "d" {
				// Verify delete operations don't have status field (CDC scenario)
				_, hasStatus := r.Data["status"]
				assert.False(t, hasStatus, "delete operations may not have filter columns")
			}
		}
	})
}
