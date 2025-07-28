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
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	lastSecondaryCursorValue := m.state.GetCursor(stream.Self(), secondaryCursor)

	// Get filter string (fully evaluated with values already inserted)
	staticFilter, err := jdbc.SQLFilter(stream, m.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	incrementalCondition, err := m.buildIncrementalCondition(primaryCursor, secondaryCursor, lastPrimaryCursorValue)
	if err != nil {
		return fmt.Errorf("failed to format cursor condition: %s", err)
	}

	combinedFilter := incrementalCondition
	if staticFilter != "" {
		combinedFilter = fmt.Sprintf("(%s) AND (%s)", staticFilter, incrementalCondition)
	}
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", stream.Namespace(), stream.Name(), combinedFilter)

	var rows *sql.Rows
	var args []any
	if secondaryCursor != "" && lastSecondaryCursorValue != nil {
		args = []any{lastPrimaryCursorValue, lastPrimaryCursorValue, lastSecondaryCursorValue}
		rows, err = m.client.QueryContext(ctx, query, lastPrimaryCursorValue, lastSecondaryCursorValue)
	} else {
		args = []any{lastPrimaryCursorValue}
		rows, err = m.client.QueryContext(ctx, query, lastPrimaryCursorValue)
	}

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s", stream.ID(), logger.InterpolateQueryPlaceholders(query, args))
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
func (m *MySQL) buildIncrementalCondition(primaryCursorField string, secondaryCursorField string, lastSecondaryCursorValue any) (string, error) {
	primaryCondition := fmt.Sprintf("`%s` >= ?", primaryCursorField)

	if secondaryCursorField != "" && lastSecondaryCursorValue != nil {
		primaryCondition = fmt.Sprintf(" %s OR (`%s` IS NULL AND `%s` >= ?)",
			primaryCondition, primaryCursorField, secondaryCursorField)
	}
	return primaryCondition, nil
}
