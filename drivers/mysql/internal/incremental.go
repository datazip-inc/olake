package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

func (m *MySQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	cursorField := stream.Cursor()
	lastCursorValue := m.state.GetCursor(stream.Self(), cursorField)
	query := fmt.Sprintf(`SELECT * FROM %s.%s WHERE %s >= ? ORDER BY %s`, stream.Namespace(), stream.Name(), cursorField, cursorField)

	logger.Infof("Starting incremental sync for stream[%s]", stream.ID())

	rows, err := m.client.QueryContext(ctx, query, []any{lastCursorValue}...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

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
