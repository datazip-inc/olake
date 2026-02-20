package driver

import (
	"context"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// CDC is not supported yet
func (d *DB2) ChangeStreamConfig() (bool, bool, bool) { return false, false, false }

func (d *DB2) PreCDC(ctx context.Context, streams []types.StreamInterface) error { return nil }
func (d *DB2) StreamChanges(ctx context.Context, streamIndex int, metadataStates map[types.StreamInterface]any, processFn abstract.CDCMsgFn) (any, error) {
	return nil, nil
}
func (d *DB2) PostCDC(ctx context.Context, streamIndex int) error {
	return nil
}

func (d *DB2) GetCDCStartPosition(stream types.StreamInterface, streamIndex int) (string, error) {
	return "", nil
}
