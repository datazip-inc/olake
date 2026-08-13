package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
)

const stateTimestampFormat = "2006-01-02 15:04:05.000000"

func (d *DB2) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, d.client, constants.DB2, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}

// FormatCursorValue formats time cursors without UTC conversion to be saved in state,
// db2 timestamps do NOT store timezone information and applying UTC changes the actual time value.
func (d *DB2) FormatCursorValue(cursorValue any) any {
	if v, ok := cursorValue.(time.Time); ok {
		return v.Format(stateTimestampFormat)
	}
	return cursorValue
}

func (d *DB2) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	opts := jdbc.DriverOptions{
		Driver: constants.DB2,
		Stream: stream,
		State:  d.state,
		Client: d.client,
	}

	incrementalQuery, queryArgs, err := jdbc.BuildIncrementalQuery(ctx, opts)
	if err != nil {
		return fmt.Errorf("failed to build incremental query: %s", err)
	}
	return d.readBatchConcurrent(ctx, incrementalQuery, queryArgs, processFn)
}
