package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

func (m *MySQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	cursorField := stream.Cursor()
	lastCursorValue := m.state.GetCursor(stream.Self(), cursorField)
	batchSize := 10000
	query := fmt.Sprintf(`SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT %d`, stream.Name(), cursorField, cursorField, batchSize)

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
