package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

func (m *MySQL) StreamIncremental(ctx context.Context, stream types.StreamInterface, processFn abstract.IncrementalMsgFn) error {
	return fmt.Errorf("incremental streaming not supported for Mongo as of now")
}
