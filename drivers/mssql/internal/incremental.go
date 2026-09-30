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
		return fmt.Errorf("failed to build incremental condition: %w", err)
	}

	setter := jdbc.NewReader(ctx, incrementalQuery, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return m.client.QueryContext(ctx, query, args...)
	}, queryArgs...)

	if err := jdbc.MapScanConcurrent(setter, m.dataTypeConverter, processFn, mssqlColumnSizer); err != nil {
		return fmt.Errorf("incremental process error: %w", err)
	}

	return nil
}

func (m *MSSQL) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, m.client, constants.MSSQL, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}
