package driver

import (
	"context"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// CDC is not supported yet

// PostCDC is called after CDC operation completes
func (o *Oracle) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, _ string) error {
	return nil
}

// PreCDC is called before CDC operation starts
func (o *Oracle) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return nil
}

// StreamChanges streams CDC changes for a given stream
func (o *Oracle) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return nil
}

// CDCSupported returns whether CDC is supported
func (o *Oracle) CDCSupported() bool {
	return o.CDCSupport // CDC is not supported yet
}

// SetupState sets the state for the driver
func (o *Oracle) SetupState(state *types.State) {
	o.state = state
}

func (o *Oracle) PartitionStreamChanges(_ context.Context, _ types.PartitionMetaData, _ abstract.CDCMsgFn) error {
	return nil
}

func (o *Oracle) GetPartitions() (map[string][]types.PartitionMetaData, int) {
	return nil, 0
}
