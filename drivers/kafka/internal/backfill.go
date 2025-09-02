package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

func (k *Kafka) GetOrSplitChunks(_ context.Context, _ *destination.WriterPool, _ types.StreamInterface) (*types.Set[types.Chunk], error) {
	return nil, fmt.Errorf("GetOrSplitChunks not supported for Kafka driver")
}

func (k *Kafka) ChunkIterator(_ context.Context, _ types.StreamInterface, _ types.Chunk, _ abstract.BackfillMsgFn) error {
	return fmt.Errorf("ChunkIterator not supported for Kafka driver")
}
