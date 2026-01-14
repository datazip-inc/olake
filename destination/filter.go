package destination

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
func FilterRecords(
	ctx context.Context,
	records []types.RawRecord,
	filter types.FilterInput,
	legacy bool,
	schema any,
) ([]types.RawRecord, error) {

	if legacy || len(filter.Conditions) == 0 || len(records) == 0 {
		return records, nil
	}

	conditions := make([]parsedCondition, len(filter.Conditions))
	for i, cond := range filter.Conditions {
		dataType := resolveColumnType(cond.Column, schema)
		if dataType == types.Unknown {
			return nil, fmt.Errorf("unknown datatype for column [%s]", cond.Column)
		}

		parsedVal, err := typeutils.ReformatValue(dataType, cond.Value)
		if err != nil && err != typeutils.ErrNullValue {
			return nil, fmt.Errorf(
				"failed to parse filter value for column [%s]: %w",
				cond.Column,
				err,
			)
		}

		conditions[i] = parsedCondition{
			column:   cond.Column,
			operator: cond.Operator,
			value:    parsedVal,
		}
	}

	return filterConcurrently(ctx, records, conditions, filter.LogicalOperator)
}

// parsedCondition holds a fully-typed condition
type parsedCondition struct {
	column   string
	operator string
	value    any
}

// resolveColumnType resolves datatype from schema (iceberg or parquet)
func resolveColumnType(column string, schema any) types.DataType {
	switch s := schema.(type) {
	case map[string]string: // iceberg schema
		switch s[column] {
		case "boolean":
			return types.Bool
		case "int":
			return types.Int32
		case "long":
			return types.Int64
		case "float":
			return types.Float32
		case "double":
			return types.Float64
		case "string":
			return types.String
		case "timestamptz":
			return types.Timestamp
		}
	case typeutils.Fields: // parquet schema
		if field, ok := s[column]; ok {
			for _, t := range field.Types() {
				if t != types.Null {
					return t
				}
			}
		}
	}
	return types.Unknown
}

// filterConcurrently evaluates records in parallel
func filterConcurrently(
	ctx context.Context,
	records []types.RawRecord,
	conditions []parsedCondition,
	logicalOp string,
) ([]types.RawRecord, error) {

	concurrency := runtime.GOMAXPROCS(0)
	if concurrency < 1 {
		concurrency = 1
	}

	var mu sync.Mutex
	filtered := make([]types.RawRecord, 0, len(records))

	err := utils.Concurrent(ctx, records, concurrency, func(
		_ context.Context,
		record types.RawRecord,
		_ int,
	) error {
		if matches(record, conditions, logicalOp) {
			mu.Lock()
			filtered = append(filtered, record)
			mu.Unlock()
		}
		return nil
	})

	return filtered, err
}

// matches evaluates AND / OR logic
func matches(
	record types.RawRecord,
	conditions []parsedCondition,
	logicalOp string,
) bool {

	isAnd := !strings.EqualFold(strings.TrimSpace(logicalOp), "OR")

	for _, cond := range conditions {
		ok := evaluate(
			record.Data[cond.column],
			cond.value,
			cond.operator,
		)
		// for AND, if any condition is not satisfied, return false
		if isAnd && !ok {
			return false
		}
		// for OR, if any condition is satisfied, return true
		if !isAnd && ok {
			return true
		}
	}

	return isAnd
}

// evaluate compares values using typeutils.Compare
func evaluate(recordVal, filterVal any, operator string) bool {
	if recordVal == nil || filterVal == nil {
		switch operator {
		case "=":
			return recordVal == nil && filterVal == nil
		case "!=":
			return recordVal != nil || filterVal != nil
		default:
			return false
		}
	}

	cmp := typeutils.Compare(recordVal, filterVal)

	switch operator {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	default:
		return false
	}
}
