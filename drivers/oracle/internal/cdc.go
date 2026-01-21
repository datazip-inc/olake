package driver

import (
	"context"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
)

// CDC is not supported yet

// PreCDC is called before CDC operation starts
func (o *Oracle) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return nil
}

// StreamChanges streams CDC changes for a given stream
func (o *Oracle) StreamChanges(_ context.Context, _ int, _ abstract.CDCMsgFn) error {
	return nil
}

// PostCDC is called after CDC operation completes
func (o *Oracle) PostCDC(ctx context.Context, _ int) error {
	return nil
}

// CDCSupported returns whether CDC is supported
func (o *Oracle) CDCSupported() bool {
	return o.CDCSupport // CDC is not supported yet
}

func (o *Oracle) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}

// Oracle dosen't support CDC yet so 2pc functions havent been implemented for it
func (o *Oracle) GetCDCPosition() string      { return "" }
func (o *Oracle) GetCDCStartPosition() string { return "" }
func (o *Oracle) SetNextCDCPosition(position string)      {}
func (o *Oracle) GetNextCDCPosition() string              { return "" }
func (o *Oracle) SetCurrentCDCPosition(position string)   {}
func (o *Oracle) SetProcessingStreams(streamIDs []string) {}
func (o *Oracle) RemoveProcessingStream(streamID string)  {}
func (o *Oracle) GetProcessingStreams() []string          { return nil }
func (o *Oracle) SetTargetCDCPosition(position string)    {}
func (o *Oracle) GetTargetCDCPosition() string            { return "" }
func (o *Oracle) SaveNextCDCPositionForStream(string)     {}
func (o *Oracle) CommitCDCPositionForStream(string)       {}
func (o *Oracle) CheckPerStreamRecovery(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error { return nil }
func (o *Oracle) AcknowledgeCDCPosition(ctx context.Context, position string) error { return nil }


// SetupState sets the state for the driver
func (o *Oracle) SetupState(state *types.State) {
	o.state = state
}
