package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

func (k *Kafka) StreamIncrementalChanges(_ context.Context, _ types.StreamInterface, _ abstract.BackfillMsgFn) error {
	return fmt.Errorf("StreamIncrementalChanges not supported for Kafka driver")
}

func (k *Kafka) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	return nil, nil, fmt.Errorf("FetchMaxCursorValues not supported for Kafka driver")
}
