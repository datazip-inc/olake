package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

// IncrementalChanges is not supported for PostgreSQL
func (p *Postgres) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	filter, err := jdbc.SQLFilter(stream, p.Type())
	if err != nil {
		return fmt.Errorf("failed to parse filter during chunk iteration: %s", err)
	}

	opts := jdbc.IncrementalConditionOptions{
		Driver: "postgres",
		Stream: stream,
		State:  p.state,
		Filter: filter,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(opts)
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

		if err := processFn(record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}
	return rows.Err()
}
