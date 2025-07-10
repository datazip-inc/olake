package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// StreamIncrementalChanges implements incremental sync for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	cursorField := stream.Cursor()
	lastCursorValue := o.state.GetCursor(stream.Self(), cursorField)

	filter, err := jdbc.SQLFilter(stream, o.Type())
	if err != nil {
		return fmt.Errorf("failed to create sql filter during incremental sync: %s", err)
	}
	incrementalCondition := fmt.Sprintf("%q > '%v'", cursorField, lastCursorValue)
	if filter != "" {
		filter = fmt.Sprintf("%s AND %s", filter, incrementalCondition)
	} else {
		filter = incrementalCondition
	}

	query := fmt.Sprintf("SELECT * FROM %q.%q WHERE %s ORDER BY %q",
		stream.Namespace(), stream.Name(), filter, cursorField)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %s", stream.ID(), filter)

	rows, err := o.client.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, o.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %w", err)
		}

		if err := processFn(record); err != nil {
			return fmt.Errorf("process error: %w", err)
		}
	}

	return rows.Err()
}
