package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// IncrementalChanges is not supported for Oracle
func (o *Oracle) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	return fmt.Errorf("incremental sync is not supported for Oracle driver")
}
