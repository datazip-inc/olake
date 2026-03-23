package typeutils

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// parsedCondition holds a fully-typed condition
type parsedCondition struct {
	column   string
	operator string
	value    any
}

// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
func FilterRecords(ctx context.Context, records []types.RawRecord, filter types.FilterConfig, isLegacy bool, schema any) ([]types.RawRecord, error) {
	if len(filter.Conditions) == 0 {
		return records, nil
	}
	if isLegacy {
		// Legacy filters are pushed down to SQL (see pkg/jdbc SQLFilter); skip re-filtering here.
		logger.Warnf("legacy filter detected, skipping filtering records")
		return records, nil
	}
	logger.Infof("filtering records with filter: %+v", filter)
	conditions := make([]parsedCondition, len(filter.Conditions))
	for i, cond := range filter.Conditions {
		cond.Column = utils.Reformat(cond.Column)
		dataType, err := getFilterColumnDataType(cond.Column, schema)
		if err != nil {
			return nil, err
		}
		parsedVal, err := ParseFilterValue(dataType, cond.Value)
		if err != nil && err != ErrNullValue {
			return nil, fmt.Errorf("failed to parse filter value for column [%s]: %s", cond.Column, err)
		}
		conditions[i] = parsedCondition{
			column:   cond.Column,
			operator: cond.Operator,
			value:    parsedVal,
		}
	}
	return func() ([]types.RawRecord, error) {
		concurrency := runtime.GOMAXPROCS(0) * 16
		var mu sync.Mutex
		filtered := make([]types.RawRecord, 0, len(records))

		err := utils.Concurrent(ctx, records, concurrency, func(_ context.Context, record types.RawRecord, _ int) error {
			match := matches(record, conditions, filter.LogicalOperator)
			if match {
				mu.Lock()
				filtered = append(filtered, record)
				mu.Unlock()
			}
			return nil
		})
		return filtered, err
	}()
}

func getFilterColumnDataType(column string, schema any) (types.DataType, error) {
	switch s := schema.(type) {
	case Fields: // parquet
		field, found := s[column]
		if !found {
			return types.Unknown, fmt.Errorf("filter column [%s] missing from schema", column)
		}
		return field.getType(), nil

	case map[string]string: // iceberg
		icebergType, found := s[column]
		if !found {
			return types.Unknown, fmt.Errorf("filter column [%s] missing from schema", column)
		}
		return types.IcebergTypeToDatatype(icebergType), nil

	default:
		return types.Unknown, fmt.Errorf("unsupported schema type for filtering: %T", schema)
	}
}

// matches evaluates AND / OR logic from filter conditions
func matches(record types.RawRecord, conditions []parsedCondition, logicalOp string) bool {
	isOr := strings.EqualFold(strings.TrimSpace(logicalOp), "OR")
	for _, c := range conditions {
		if evaluate(record.Data[c.column], c.value, c.operator) == isOr {
			return isOr
		}
	}
	return !isOr
}

// evaluate compares values using typeutils.Compare from filter conditions
func evaluate(recordVal, filterVal any, operator string) bool {
	if recordVal == nil || filterVal == nil {
		return (operator == "=" && recordVal == filterVal) || (operator == "!=" && recordVal != filterVal)
	}

	cmp := Compare(recordVal, filterVal)

	return (operator == "=" && cmp == 0) || (operator == "!=" && cmp != 0) || (operator == ">" && cmp > 0) || (operator == ">=" && cmp >= 0) || (operator == "<" && cmp < 0) || (operator == "<=" && cmp <= 0)
}
