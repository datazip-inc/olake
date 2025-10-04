package driver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

func (m *MySQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	opts := jdbc.IncrementalConditionOptions{
		Driver: constants.MySQL,
		Stream: stream,
		State:  m.state,
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

		if err := processFn(ctx, record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}

	return rows.Err()
}

func (m *MySQL) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, m.client, constants.MySQL, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}
