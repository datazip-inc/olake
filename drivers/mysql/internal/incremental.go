package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	lastSecondaryCursorValue := m.state.GetCursor(stream.Self(), secondaryCursor)

	//TODO:
	// 1. we need to ensure that state never has nil values for any cursor in all cases
	if lastPrimaryCursorValue == nil {
		logger.Warnf("last primary cursor value is nil for stream[%s]", stream.ID())
	} else if lastSecondaryCursorValue == nil {
		logger.Warnf("last secondary cursor value is nil for stream[%s]", stream.ID())
	}

	filter, err := jdbc.SQLFilter(stream, m.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	incrementalCondition, queryArgs, err := m.buildIncrementalCondition(primaryCursor, secondaryCursor, lastPrimaryCursorValue, lastSecondaryCursorValue)
	if err != nil {
		return fmt.Errorf("failed to format cursor condition: %s", err)
	}
	filter = utils.Ternary(filter != "", fmt.Sprintf("(%s) AND (%s)", filter, incrementalCondition), incrementalCondition).(string)
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), filter)

	var rows *sql.Rows
	if secondaryCursor != "" && lastSecondaryCursorValue != nil {
		rows, err = m.client.QueryContext(ctx, query, lastPrimaryCursorValue, lastSecondaryCursorValue)
	} else {
		rows, err = m.client.QueryContext(ctx, query, lastPrimaryCursorValue)
	}

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s", stream.ID(), logger.InterpolateQueryPlaceholders(query, queryArgs))
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	// Scan rows and process
	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, m.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}

		if err := processFn(record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}

	return rows.Err()
}

// buildIncrementalCondition generates the incremental condition SQL for MySQL
func (m *MySQL) buildIncrementalCondition(primaryCursor string, secondaryCursor string, lastPrimaryCursorValue any, lastSecondaryCursorValue any) (string, []any, error) {
	primaryCondition := fmt.Sprintf("`%s` >= ?", primaryCursor)
	queryArgs := []any{lastPrimaryCursorValue}
	if secondaryCursor != "" && lastSecondaryCursorValue != nil {
		queryArgs = []any{lastPrimaryCursorValue, lastSecondaryCursorValue}
		primaryCondition = fmt.Sprintf(" %s OR (`%s` IS NULL AND `%s` >= ?)",
			primaryCondition, primaryCursor, secondaryCursor)
	}
	return primaryCondition, queryArgs, nil
}
