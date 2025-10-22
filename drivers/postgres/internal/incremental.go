package driver

import (
	"context"
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

	rows, err := p.client.QueryContext(ctx, incrementalQuery, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, p.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}

		if err := processFn(ctx, record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}
	return rows.Err()
}

func (p *Postgres) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, p.client, constants.Postgres, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}
