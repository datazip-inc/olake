package driver

import (
	"context"
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
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}

	setter := jdbc.NewReader(ctx, incrementalQuery, m.client.QueryContext, queryArgs...)
	return jdbc.MapScanConcurrent(setter, m.dataTypeConverter, processFn)
}

func (m *MySQL) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	maxPrimaryCursorValue, maxSecondaryCursorValue, err := jdbc.GetMaxCursorValues(ctx, m.client, constants.MySQL, stream)
	if err != nil {
		return nil, nil, err
	}
	return maxPrimaryCursorValue, maxSecondaryCursorValue, nil
}
