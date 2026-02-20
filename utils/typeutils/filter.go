package typeutils

import (
	"context"
	"runtime"
	"strings"
	"sync"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
// FilterRecords applies filtering ONLY for new filters.
// For legacy filters, records are returned unchanged.
func FilterRecords(
	ctx context.Context,
	records []types.RawRecord,
	filter types.FilterInput,
	legacy bool,
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
	return func() ([]types.RawRecord, error) {
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
			if record.OlakeColumns[constants.OpType].(string) == "d" {
				mu.Lock()
				filtered = append(filtered, record)
				mu.Unlock()
				return nil
			}

			match := matches(record, filter.Conditions, filter.LogicalOperator)
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

// matches evaluates AND / OR logic from filter conditions
func matches(record types.RawRecord, conditions []types.FilterCondition, logicalOp string) bool {
	isOr := strings.EqualFold(strings.TrimSpace(logicalOp), "OR")
	for _, c := range conditions {
		if evaluate(record.Data[c.Column], c.Value, c.Operator) == isOr {
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
