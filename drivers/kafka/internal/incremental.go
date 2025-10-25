package driver

import (
	"context"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

func (k *Kafka) StreamIncrementalChanges(_ context.Context, _ types.StreamInterface, _ abstract.BackfillMsgFn) error {
	return nil
}

func (k *Kafka) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	return nil, nil, nil
}
