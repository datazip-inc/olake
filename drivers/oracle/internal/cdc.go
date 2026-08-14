package driver

import (
	"context"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// CDC is not supported yet

// PreCDC is called before CDC operation starts
func (o *Oracle) PreCDC(_ context.Context, _ []types.StreamInterface) error {
	return nil
}

// StreamChanges streams CDC changes for a given stream
func (o *Oracle) StreamChanges(_ context.Context, _ int, _ map[string]any, _ abstract.CDCMsgFn) (any, error) {
	return nil, nil //nolint:nilnil // CDC unsupported for Oracle: nil state with nil error is the stub contract
}

// PostCDC is called after CDC operation completes
func (o *Oracle) PostCDC(_ context.Context, _ int) error {
	return nil
}

// CDCSupported returns whether CDC is supported
func (o *Oracle) CDCSupported() bool {
	return o.CDCSupport // CDC is not supported yet
}

func (o *Oracle) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}

// SetupState sets the state for the driver
func (o *Oracle) SetupState(state *types.State) {
	o.state = state
}
