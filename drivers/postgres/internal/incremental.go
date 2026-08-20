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

func (p *Postgres) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.Postgres,
		Stream: stream,
		State:  p.state,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}

	setter := jdbc.NewReader(ctx, incrementalQuery, func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
		return p.client.QueryContext(ctx, query, args...)
	}, queryArgs...)

	if err := jdbc.MapScanConcurrent(setter, p.dataTypeConverter, processFn, pgColumnSizer); err != nil {
		return fmt.Errorf("incremental process error: %s", err)
	}
	
	return nil
}

func (p *Postgres) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, p.client, constants.Postgres, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}
