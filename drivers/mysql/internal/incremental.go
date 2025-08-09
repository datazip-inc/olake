package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

func (m *MySQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	filter, err := jdbc.SQLFilter(stream, m.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	opts := jdbc.IncrementalConditionOptions{
		Driver: "mysql",
		Stream: stream,
		State:  m.state,
		Filter: filter,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}

	var rows *sql.Rows
	rows, err = m.client.QueryContext(ctx, incrementalQuery, queryArgs...)
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
