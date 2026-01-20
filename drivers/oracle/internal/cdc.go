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

func (o *Oracle) GetCDCPosition() string {
	return "" // Oracle CDC not yet supported
}

func (o *Oracle) GetCDCStartPosition() string {
	return "" // Oracle CDC not yet supported
}

func (o *Oracle) SetNextCDCPosition(position string) {
	// Oracle CDC not yet supported
}

func (o *Oracle) GetNextCDCPosition() string {
	// Oracle CDC not yet supported
	return ""
}

func (o *Oracle) SetCurrentCDCPosition(position string) {
	// Oracle CDC not yet supported
}

func (o *Oracle) SetProcessingStreams(streamIDs []string) {
	// Oracle CDC not yet supported
}

func (o *Oracle) RemoveProcessingStream(streamID string) {
	// Oracle CDC not yet supported
}

func (o *Oracle) GetProcessingStreams() []string {
	// Oracle CDC not yet supported
	return nil
}

func (o *Oracle) SetTargetCDCPosition(position string) {
	// Oracle CDC not yet supported
}

func (o *Oracle) GetTargetCDCPosition() string {
	// Oracle CDC not yet supported
	return ""
}

// SaveNextCDCPositionForStream - no-op for Oracle
func (o *Oracle) SaveNextCDCPositionForStream(streamID string) {}

// CommitCDCPositionForStream - no-op for Oracle
func (o *Oracle) CommitCDCPositionForStream(streamID string) {}

// CheckPerStreamRecovery - no-op for Oracle
func (o *Oracle) CheckPerStreamRecovery(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error {
	return nil
}

// AcknowledgeCDCPosition - no-op for Oracle
func (o *Oracle) AcknowledgeCDCPosition(ctx context.Context, position string) error {
	return nil
}

// SetupState sets the state for the driver
func (o *Oracle) SetupState(state *types.State) {
	o.state = state
}
