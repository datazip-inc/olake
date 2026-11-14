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
	opts := jdbc.DriverOptions{
		Driver: constants.MySQL,
		Stream: stream,
		State:  m.state,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %w", err)
	}
	unsignedDecimals, err := m.unsignedDecimals(ctx, stream)
	if err != nil {
		return err
	}

	var rows *sql.Rows
	rows, err = m.client.QueryContext(ctx, incrementalQuery, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %w", err)
	}
	defer rows.Close()

	// Scan rows and process
	columnTypeName := mysqlColumnTypeName(unsignedDecimals)
	for rows.Next() {
		record := make(types.Record)
		rowBytes, err := jdbc.MapScan(rows, record, m.dataTypeConverter, mysqlColumnSizer, columnTypeName)
		if err != nil {
			return fmt.Errorf("failed to scan record: %w", err)
		}

		if err := processFn(ctx, record, rowBytes); err != nil {
			return fmt.Errorf("process error: %w", err)
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
