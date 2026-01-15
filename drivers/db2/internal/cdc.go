package driver

import (
	"context"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// CDC is not supported yet

func (d *DB2) PreCDC(ctx context.Context, streams []types.StreamInterface) error { return nil }
func (d *DB2) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return nil
}
func (d *DB2) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
	return nil
}
