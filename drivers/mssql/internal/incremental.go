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

func (m *MSSQL) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.MSSQL,
		Stream: stream,
		State:  m.state,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(ctx, opts)
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

func (m *MSSQL) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, m.client, constants.MSSQL, stream)
	if err != nil {
		return nil, nil, err
	}

	primaryCursor, secondaryCursor := stream.Cursor()
	maxPrimaryCursorValue, err = m.normalizeCursorValue(ctx, stream, primaryCursor, maxPrimaryCursorValue)
	if err != nil {
		return nil, nil, err
	}
	if secondaryCursor != "" {
		maxSecondaryCursorValue, err = m.normalizeCursorValue(ctx, stream, secondaryCursor, maxSecondaryCursorValue)
		if err != nil {
			return nil, nil, err
		}
	}

	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}

func (m *MSSQL) normalizeCursorValue(ctx context.Context, stream types.StreamInterface, cursorField string, value any) (any, error) {
	if cursorField == "" || value == nil {
		return value, nil
	}

	var dataType string
	if err := m.client.QueryRowContext(ctx, jdbc.MSSQLColumnTypeQuery(), stream.Namespace(), stream.Name(), cursorField).Scan(&dataType); err != nil {
		return nil, fmt.Errorf("failed to get MSSQL cursor type: %s", err)
	}

	return normalizeMSSQLValueForState(value, dataType), nil
}
