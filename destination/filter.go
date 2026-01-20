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

// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
func FilterRecords(
	ctx context.Context,
	records []types.RawRecord,
	filter types.FilterInput,
	legacy bool,
	schema any,
) ([]types.RawRecord, error) {
	logger.Debugf(
		"[FilterRecords] called: legacy=%v, conditions=%d, records=%d",
		legacy,
		len(filter.Conditions),
		len(records),
	)

	if legacy {
		logger.Debug("[FilterRecords] legacy filter detected → skipping destination filtering")
		return records, nil
	}

	if len(filter.Conditions) == 0 {
		logger.Debug("[FilterRecords] no filter conditions → returning records as-is")
		return records, nil
	}

	if len(records) == 0 {
		logger.Debug("[FilterRecords] no records to filter")
		return records, nil
	}

	conditions := make([]parsedCondition, len(filter.Conditions))
	for i, cond := range filter.Conditions {
		cond.Column = utils.Reformat(cond.Column)
		logger.Debugf(
			"[FilterRecords] parsing condition[%d]: column=%s operator=%s value=%v (%T)",
			i,
			cond.Column,
			cond.Operator,
			cond.Value,
			cond.Value,
		)

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

		logger.Debugf(
			"[FilterRecords] parsed condition[%d] value → %v (%T)",
			i,
			parsedVal,
			parsedVal,
		)

		conditions[i] = parsedCondition{
			column:   cond.Column,
			operator: cond.Operator,
			value:    parsedVal,
		}
	}

	logger.Debug("[FilterRecords] starting concurrent filtering")
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
	logger.Debugf("[resolveColumnType] resolving datatype for column=%s", column)

	switch s := schema.(type) {
	case map[string]string: // iceberg schema
		logger.Debug("[resolveColumnType] iceberg schema detected")
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
		logger.Debug("[resolveColumnType] parquet schema detected")
		if field, ok := s[column]; ok {
			for _, t := range field.Types() {
				if t != types.Null {
					return t
				}
			}
		}
	}

	logger.Debugf("[resolveColumnType] unknown datatype for column=%s", column)
	return types.Unknown
}

// filterConcurrently evaluates records in parallel
func filterConcurrently(
	ctx context.Context,
	records []types.RawRecord,
	conditions []parsedCondition,
	logicalOp string,
) ([]types.RawRecord, error) {
	logger.Debugf(
		"[filterConcurrently] records=%d conditions=%d logicalOp=%s",
		len(records),
		len(conditions),
		logicalOp,
	)

	concurrency := runtime.GOMAXPROCS(0)
	if concurrency < 1 {
		concurrency = 1
	}

	var mu sync.Mutex
	filtered := make([]types.RawRecord, 0, len(records))

	err := utils.Concurrent(ctx, records, concurrency, func(
		_ context.Context,
		record types.RawRecord,
		idx int,
	) error {
		// Delete operations should always be synced, regardless of filter conditions.
		// This is because:
		// 1. If a record was previously synced, we need to delete it
		// 2. If a record was never synced (filtered out), deleting it is a no-op anyway
		// 3. Delete operations in MongoDB CDC only contain the document key, not full document fields,
		//    so filter conditions that require those fields cannot be evaluated
		if record.OperationType == "d" {
			logger.Debugf(
				"[filterConcurrently] record[%d] is delete operation → skipping filter",
				idx,
			)
			mu.Lock()
			filtered = append(filtered, record)
			mu.Unlock()
			return nil
		}

		match := matches(record, conditions, logicalOp)

		logger.Debugf(
			"[filterConcurrently] record[%d] match=%v",
			idx,
			match,
		)

		if match {
			mu.Lock()
			filtered = append(filtered, record)
			mu.Unlock()
		}
		return nil
	})

	logger.Debugf(
		"[filterConcurrently] finished: input=%d output=%d",
		len(records),
		len(filtered),
	)

	return filtered, err
}

// matches evaluates AND / OR logic
func matches(
	record types.RawRecord,
	conditions []parsedCondition,
	logicalOp string,
) bool {
	isAnd := !strings.EqualFold(strings.TrimSpace(logicalOp), "OR")
	logger.Debugf("[matches] logicalOp=%s isAnd=%v", logicalOp, isAnd)

	for _, cond := range conditions {
		recordVal := record.Data[cond.column]

		ok := evaluate(recordVal, cond.value, cond.operator)

		logger.Debugf(
			"[matches] column=%s operator=%s recordVal=%v filterVal=%v result=%v",
			cond.column,
			cond.operator,
			recordVal,
			cond.value,
			ok,
		)

		if isAnd && !ok {
			logger.Debug("[matches] AND condition failed → record rejected")
			return false
		}

		if !isAnd && ok {
			logger.Debug("[matches] OR condition satisfied → record accepted")
			return true
		}
	}

	logger.Debugf("[matches] final result=%v", isAnd)
	return isAnd
}

// evaluate compares values using typeutils.Compare
func evaluate(recordVal, filterVal any, operator string) bool {
	logger.Debugf(
		"[evaluate] recordVal=%v (%T) filterVal=%v (%T) operator=%s",
		recordVal,
		recordVal,
		filterVal,
		filterVal,
		operator,
	)

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

	logger.Debugf("[evaluate] compare result=%d", cmp)

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
		logger.Debugf("[evaluate] unsupported operator=%s", operator)
		return false
	}
}
