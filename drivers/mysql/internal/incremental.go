package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	filter, err := jdbc.SQLFilter(stream, m.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	incrementalCondition, queryArgs, err := m.buildIncrementalCondition(stream)
	if err != nil {
		return fmt.Errorf("failed to format cursor condition: %s", err)
	}

	query := jdbc.MySQLIncrementalQuery(stream, filter, incrementalCondition)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s and args: %v", stream.ID(), query, queryArgs)

	var rows *sql.Rows
	rows, err = m.client.QueryContext(ctx, query, queryArgs...)
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
func (m *MySQL) buildIncrementalCondition(stream types.StreamInterface) (string, []any, error) {
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	lastSecondaryCursorValue := m.state.GetCursor(stream.Self(), secondaryCursor)

	// TODO:
	// should we fail here or just warning is ok, (to aware user in start that incremental won't make sense here)
	if lastPrimaryCursorValue == nil {
		logger.Warnf("last primary cursor value is nil for stream[%s]", stream.ID())
	}
	if secondaryCursor != "" && lastSecondaryCursorValue == nil {
		logger.Warnf("last secondary cursor value is nil for stream[%s]", stream.ID())
	}
	// TODO: common out incremental condition for all jdbc supported drivers
	incrementalCondition := fmt.Sprintf("`%s` >= ?", primaryCursor)
	queryArgs := []any{lastPrimaryCursorValue}
	if secondaryCursor != "" && lastSecondaryCursorValue != nil {
		queryArgs = []any{lastPrimaryCursorValue, lastSecondaryCursorValue}
		incrementalCondition = fmt.Sprintf(" %s OR (`%s` IS NULL AND `%s` >= ?)",
		incrementalCondition, primaryCursor, secondaryCursor)
	}
	return incrementalCondition, queryArgs, nil
}

func (m *MySQL) PostIncremental(ctx context.Context, stream types.StreamInterface, noErr bool) error {
	return fmt.Errorf("post incremental not supported for MySQL")
}
