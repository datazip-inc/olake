package destination

import (
	"context"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helper functions
// ─────────────────────────────────────────────────────────────────────────────

func makeRecord(data map[string]any) types.RawRecord {
	return types.RawRecord{Data: data}
}

func makeIcebergSchema(cols map[string]string) map[string]string {
	return cols
}

func makeParquetSchema(cols map[string]types.DataType) typeutils.Fields {
	fields := typeutils.Fields{}
	for name, dt := range cols {
		fields[name] = typeutils.NewField(dt)
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
	schema := makeIcebergSchema(map[string]string{"id": "long", "name": "string"})

	result, err := FilterRecords(ctx, records, filter, true, schema)
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
	schema := makeIcebergSchema(map[string]string{"id": "long"})

	result, err := FilterRecords(ctx, records, filter, false, schema)
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
	schema := makeIcebergSchema(map[string]string{"id": "long"})

	result, err := FilterRecords(ctx, records, filter, false, schema)
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
			name:          "lowercase filter column",
			filterColumn:  "user_id",
			schemaColumn:  "user_id",
			recordColumn:  "user_id",
			expectedCount: 1,
		},
		{
			name:          "UPPERCASE filter column gets lowercased",
			filterColumn:  "USER_ID",
			schemaColumn:  "user_id",
			recordColumn:  "user_id",
			expectedCount: 1,
		},
		{
			name:          "MixedCase filter column gets lowercased",
			filterColumn:  "UserId",
			schemaColumn:  "userid",
			recordColumn:  "userid",
			expectedCount: 1,
		},
		{
			name:          "CamelCase filter column gets lowercased",
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
			schema := makeIcebergSchema(map[string]string{tt.schemaColumn: "long"})

			result, err := FilterRecords(ctx, records, filter, false, schema)
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
		makeRecord(map[string]any{"value": int64(10)}),
		makeRecord(map[string]any{"value": int64(20)}),
		makeRecord(map[string]any{"value": int64(30)}),
		makeRecord(map[string]any{"value": int64(40)}),
		makeRecord(map[string]any{"value": int64(50)}),
	}

	tests := []struct {
		name          string
		operator      string
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

	schema := makeIcebergSchema(map[string]string{"value": "long"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := types.FilterInput{
				LogicalOperator: "AND",
				Conditions: []types.FilterCondition{
					{Column: "value", Operator: tt.operator, Value: tt.filterValue},
				},
			}

			result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"active": "boolean"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"count": "int"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"bignum": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"price": "float"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"amount": "double"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"name": "string"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"created_at": "timestamptz"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"is_active": types.Bool})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"age": types.Int32})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"id": types.Int64})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"temp": types.Float32})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"balance": types.Float64})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"status": types.String})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeParquetSchema(map[string]types.DataType{"updated_at": types.Timestamp})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{
			"age":    "long",
			"city":   "string",
			"active": "boolean",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{
			"age":    "long",
			"city":   "string",
			"active": "boolean",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 1) // Only record 3 matches all three
	})

	t.Run("empty logical operator defaults to AND", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "", // empty should default to AND
			Conditions: []types.FilterCondition{
				{Column: "city", Operator: "=", Value: "NYC"},
				{Column: "active", Operator: "=", Value: true},
			},
		}
		schema := makeIcebergSchema(map[string]string{
			"age":    "long",
			"city":   "string",
			"active": "boolean",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{
			"department": "string",
			"level":      "long",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{
			"department": "string",
			"level":      "long",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{
			"department": "string",
			"level":      "long",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"name": "string", "age": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"name": "string", "age": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"name": "string", "age": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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

	t.Run("AND with missing column returns false", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "active"},
			},
		}
		schema := makeIcebergSchema(map[string]string{
			"id":     "long",
			"name":   "string",
			"status": "string",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		// Record 0: has status=active → matches
		// Record 1: missing status → no match (missing column = false)
		// Record 2: has status=inactive → doesn't match
		// Record 3: missing status → no match (missing column = false)
		assert.Len(t, result, 1)
	})

	t.Run("OR with missing column returns false for that condition", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "OR",
			Conditions: []types.FilterCondition{
				{Column: "name", Operator: "=", Value: "Alice"},
				{Column: "status", Operator: "=", Value: "inactive"},
			},
		}
		schema := makeIcebergSchema(map[string]string{
			"id":     "long",
			"name":   "string",
			"status": "string",
		})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
	schema := makeIcebergSchema(map[string]string{"id": "long"})

	_, err := FilterRecords(ctx, records, filter, false, schema)
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

	schema := makeIcebergSchema(map[string]string{"name": "string"})

	tests := []struct {
		name          string
		operator      string
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
					{Column: "name", Operator: tt.operator, Value: tt.filterValue},
				},
			}

			result, err := FilterRecords(ctx, records, filter, false, schema)
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

	schema := makeIcebergSchema(map[string]string{"enabled": "boolean"})

	t.Run("boolean equals true", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "enabled", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false, schema)
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

		result, err := FilterRecords(ctx, records, filter, false, schema)
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

		result, err := FilterRecords(ctx, records, filter, false, schema)
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

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 2) // true > false
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: Type Coercion from Filter Values
// ─────────────────────────────────────────────────────────────────────────────

func TestFilterRecords_TypeCoercion(t *testing.T) {
	ctx := context.Background()

	t.Run("string filter value to int64", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"id": int64(100)}),
			makeRecord(map[string]any{"id": int64(200)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "id", Operator: "=", Value: "100"}, // string value
			},
		}
		schema := makeIcebergSchema(map[string]string{"id": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("float filter value to int32", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"count": int32(10)}),
			makeRecord(map[string]any{"count": int32(20)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "count", Operator: "=", Value: 10.0}, // float value
			},
		}
		schema := makeIcebergSchema(map[string]string{"count": "int"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("int filter value to float64", func(t *testing.T) {
		records := []types.RawRecord{
			makeRecord(map[string]any{"price": float64(99.99)}),
			makeRecord(map[string]any{"price": float64(100.0)}),
		}
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "price", Operator: "=", Value: 100}, // int value
			},
		}
		schema := makeIcebergSchema(map[string]string{"price": "double"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 1)
	})

	t.Run("string boolean value", func(t *testing.T) {
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
		schema := makeIcebergSchema(map[string]string{"active": "boolean"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
	schema := makeIcebergSchema(map[string]string{"name": "string"})

	result, err := FilterRecords(ctx, records, filter, false, schema)
	require.NoError(t, err)
	assert.Len(t, result, 0) // unsupported operator returns false for all
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
			"status":      "cancelled",
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

	schema := makeIcebergSchema(map[string]string{
		"order_id":    "long",
		"customer_id": "long",
		"total":       "double",
		"status":      "string",
		"priority":    "int",
		"is_express":  "boolean",
	})

	t.Run("high value express orders", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "AND",
			Conditions: []types.FilterCondition{
				{Column: "total", Operator: ">=", Value: 100},
				{Column: "is_express", Operator: "=", Value: true},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 3) // orders 1001, 1003, 1005
	})

	t.Run("completed or cancelled orders", func(t *testing.T) {
		filter := types.FilterInput{
			LogicalOperator: "OR",
			Conditions: []types.FilterCondition{
				{Column: "status", Operator: "=", Value: "completed"},
				{Column: "status", Operator: "=", Value: "cancelled"},
			},
		}

		result, err := FilterRecords(ctx, records, filter, false, schema)
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

		result, err := FilterRecords(ctx, records, filter, false, schema)
		require.NoError(t, err)
		assert.Len(t, result, 3) // orders 1001, 1003, 1005
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: resolveColumnType function
// ─────────────────────────────────────────────────────────────────────────────

func TestResolveColumnType(t *testing.T) {
	t.Run("iceberg schema types", func(t *testing.T) {
		schema := makeIcebergSchema(map[string]string{
			"bool_col":      "boolean",
			"int_col":       "int",
			"long_col":      "long",
			"float_col":     "float",
			"double_col":    "double",
			"string_col":    "string",
			"timestamp_col": "timestamptz",
		})

		assert.Equal(t, types.Bool, resolveColumnType("bool_col", schema))
		assert.Equal(t, types.Int32, resolveColumnType("int_col", schema))
		assert.Equal(t, types.Int64, resolveColumnType("long_col", schema))
		assert.Equal(t, types.Float32, resolveColumnType("float_col", schema))
		assert.Equal(t, types.Float64, resolveColumnType("double_col", schema))
		assert.Equal(t, types.String, resolveColumnType("string_col", schema))
		assert.Equal(t, types.Timestamp, resolveColumnType("timestamp_col", schema))
		assert.Equal(t, types.Unknown, resolveColumnType("nonexistent", schema))
	})

	t.Run("parquet schema types", func(t *testing.T) {
		schema := makeParquetSchema(map[string]types.DataType{
			"bool_col":   types.Bool,
			"int32_col":  types.Int32,
			"int64_col":  types.Int64,
			"float_col":  types.Float32,
			"double_col": types.Float64,
			"string_col": types.String,
		})

		assert.Equal(t, types.Bool, resolveColumnType("bool_col", schema))
		assert.Equal(t, types.Int32, resolveColumnType("int32_col", schema))
		assert.Equal(t, types.Int64, resolveColumnType("int64_col", schema))
		assert.Equal(t, types.Float32, resolveColumnType("float_col", schema))
		assert.Equal(t, types.Float64, resolveColumnType("double_col", schema))
		assert.Equal(t, types.String, resolveColumnType("string_col", schema))
		assert.Equal(t, types.Unknown, resolveColumnType("nonexistent", schema))
	})

	t.Run("unsupported schema type", func(t *testing.T) {
		schema := "unsupported_type"
		assert.Equal(t, types.Unknown, resolveColumnType("any_col", schema))
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Test: evaluate function
// ─────────────────────────────────────────────────────────────────────────────

func TestEvaluate(t *testing.T) {
	t.Run("nil handling", func(t *testing.T) {
		assert.True(t, evaluate(nil, nil, "="))
		assert.False(t, evaluate(nil, nil, "!="))
		assert.True(t, evaluate("value", nil, "!="))
		assert.True(t, evaluate(nil, "value", "!="))
		assert.False(t, evaluate(nil, "value", "="))
		assert.False(t, evaluate(nil, "value", ">"))
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

	t.Run("unsupported operator", func(t *testing.T) {
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
		conditions := []parsedCondition{
			{column: "a", operator: "=", value: int64(10)},
			{column: "b", operator: "=", value: "test"},
		}
		assert.True(t, matches(record, conditions, "AND"))
	})

	t.Run("AND with one condition failing", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []parsedCondition{
			{column: "a", operator: "=", value: int64(10)},
			{column: "b", operator: "=", value: "other"},
		}
		assert.False(t, matches(record, conditions, "AND"))
	})

	t.Run("OR with one condition matching", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []parsedCondition{
			{column: "a", operator: "=", value: int64(999)},
			{column: "b", operator: "=", value: "test"},
		}
		assert.True(t, matches(record, conditions, "OR"))
	})

	t.Run("OR with no conditions matching", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10), "b": "test"})
		conditions := []parsedCondition{
			{column: "a", operator: "=", value: int64(999)},
			{column: "b", operator: "=", value: "other"},
		}
		assert.False(t, matches(record, conditions, "OR"))
	})

	t.Run("missing column in AND returns false", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10)}) // no "b"
		conditions := []parsedCondition{
			{column: "a", operator: "=", value: int64(10)},
			{column: "b", operator: "=", value: "test"}, // missing
		}
		assert.False(t, matches(record, conditions, "AND"))
	})

	t.Run("empty conditions with AND returns true", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10)})
		conditions := []parsedCondition{}
		assert.True(t, matches(record, conditions, "AND"))
	})

	t.Run("empty conditions with OR returns false", func(t *testing.T) {
		record := makeRecord(map[string]any{"a": int64(10)})
		conditions := []parsedCondition{}
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
		schema := makeIcebergSchema(map[string]string{"user_account_id": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"userid": "long"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"customer_name": "string"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"_order_date_": "string"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
		schema := makeIcebergSchema(map[string]string{"address_city": "string"})

		result, err := FilterRecords(ctx, records, filter, false, schema)
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
			"value":    float64(i * 10),
		})
	}

	filter := types.FilterInput{
		LogicalOperator: "AND",
		Conditions: []types.FilterCondition{
			{Column: "category", Operator: "=", Value: "A"},
			{Column: "value", Operator: ">=", Value: 500},
		},
	}
	schema := makeIcebergSchema(map[string]string{
		"id":       "long",
		"category": "string",
		"value":    "double",
	})

	result, err := FilterRecords(ctx, records, filter, false, schema)
	require.NoError(t, err)

	// Category A appears at indices 0, 5, 10, 15, ... (every 5th)
	// value >= 500 means id >= 50
	// So we want category A with id >= 50: 50, 55, 60, 65, ..., 995
	// That's (1000 - 50) / 5 = 190 records
	expectedCount := 0
	for i := 50; i < 1000; i += 5 {
		expectedCount++
	}
	assert.Len(t, result, expectedCount)
}
