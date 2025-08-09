package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

// StreamIncrementalChanges implements incremental sync for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	filter, err := jdbc.SQLFilter(stream, o.Type())
	if err != nil {
		return fmt.Errorf("failed to create sql filter during incremental sync: %s", err)
	}

	opts := jdbc.IncrementalConditionOptions{
		Driver: "oracle",
		Stream: stream,
		State:  o.state,
		Client: o.client,
		Filter: filter,
	}
	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}

	rows, err := o.client.QueryContext(ctx, incrementalQuery, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, o.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}

		if err := processFn(record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}
	return rows.Err()
}
