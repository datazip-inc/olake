package destination

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

// parsedCondition holds a fully-typed condition
type parsedCondition struct {
	column   string
	operator string
	value    any
}

// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
func FilterRecords(
	ctx context.Context,
	records []types.RawRecord,
	filter types.FilterInput,
	legacy bool,
	schema any,
) ([]types.RawRecord, error) {
	logger.Infof("filtering records with filter: %+v", filter)

	if legacy {
		logger.Warnf("legacy filter detected, skipping destination filtering")
		return records, nil
	}

	if len(filter.Conditions) == 0 {
		logger.Warnf("no filter conditions, returning records as-is")
		return records, nil
	}

	if len(records) == 0 {
		logger.Warnf("no records to filter")
		return records, nil
	}

	conditions := make([]parsedCondition, len(filter.Conditions))
	for i, cond := range filter.Conditions {
		cond.Column = utils.Reformat(cond.Column)
		dataType := resolveColumnType(cond.Column, schema)
		if dataType == types.Unknown {
			return nil, fmt.Errorf("unknown datatype for column [%s]", cond.Column)
		}

		parsedVal, err := typeutils.ReformatValue(dataType, cond.Value)
		if err != nil && err != typeutils.ErrNullValue {
			return nil, fmt.Errorf(
				"failed to parse filter value for column [%s]: %s",
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
	concurrency := runtime.GOMAXPROCS(0) * 16
	var mu sync.Mutex
	filtered := make([]types.RawRecord, 0, len(records))

	err := utils.Concurrent(ctx, records, concurrency, func(
		_ context.Context,
		record types.RawRecord,
		_ int,
	) error {
		// Delete operations should always be synced, regardless of filter conditions.
		// This is because:
		// 1. If a record was previously synced, we need to delete it
		// 2. If a record was never synced (filtered out), deleting it is a no-op anyway
		// 3. Delete operations in MongoDB CDC only contain the document key, not full document fields,
		//    so filter conditions that require those fields cannot be evaluated
		if record.OperationType == "d" {
			mu.Lock()
			filtered = append(filtered, record)
			mu.Unlock()
			return nil
		}

		match := matches(record, conditions, logicalOp)
		if match {
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
		recordVal := record.Data[cond.column]

		ok := evaluate(recordVal, cond.value, cond.operator)
		if isAnd && !ok {
			return false
		}

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
