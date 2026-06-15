package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

func (d *DB2) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, d.client, constants.DB2, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
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

	// TODO: check differnet way to handle or use Viper
	if os.Getenv("DB2_READ_MODE") != "mapscan" {
		logger.Infof("[DB2] incremental read path: readbatch (fetch_size=%d)", db2DefaultFetchSize)
		return d.readBatchConcurrent(ctx, incrementalQuery, queryArgs, processFn)
	}

	logger.Infof("[DB2] incremental read path: mapscan (DB2_READ_MODE=mapscan)")
	rows, err := d.client.QueryContext(ctx, incrementalQuery, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer rows.Close()

	for rows.Next() {
		record := make(types.Record)
		if err := jdbc.MapScan(rows, record, d.dataTypeConverter); err != nil {
			return fmt.Errorf("failed to scan record: %s", err)
		}
		if err := processFn(ctx, record); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}
	return rows.Err()
}
