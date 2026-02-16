package abstract

import (
	"context"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
)

type BackfillMsgFn func(ctx context.Context, message map[string]any) error
type CDCMsgFn func(ctx context.Context, message CDCChange) error

type Config interface {
	Validate() error
}

// GlobalPosition2PC is for drivers that use a global CDC position shared across all streams (MySQL, Postgres).
// These drivers need bounded sync because all streams share the same LSN/binlog position.
type GlobalPosition2PC interface {
	SetTargetCDCPosition(position string) // sets target position for bounded recovery sync (empty = use latest)
	GetTargetCDCPosition() string         // returns target position (empty = use latest)
}

type PerStreamRecovery2PC interface {
	GetCDCPositionForStream(streamID string) string
}

// PositionAcknowledgment is for drivers that need to acknowledge CDC positions to the source (Postgres).
// This is used for LSN acknowledgment to advance the replication slot and avoid lsn mismatch.
type PositionAcknowledgment interface {
	AcknowledgeCDCPosition(ctx context.Context, position string) error // acknowledges CDC position to source
}

type DriverInterface interface {
	GetConfigRef() Config
	Spec() any
	Type() string
	// specific to test & setup
	Setup(ctx context.Context) error
	SetupState(state *types.State)
	// sync artifacts
	MaxConnections() int
	MaxRetries() int
	// specific to discover
	GetStreamNames(ctx context.Context) ([]string, error)
	ProduceSchema(ctx context.Context, stream string) (*types.Stream, error)
	// specific to backfill
	GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error)
	ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error
	//incremental specific
	FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error)
	StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error
	// specific to cdc
	CDCSupported() bool
	ChangeStreamConfig() (sequential bool, parallel bool, concurrent bool)
	PreCDC(ctx context.Context, streams []types.StreamInterface) error // to init state
	StreamChanges(ctx context.Context, identifier int, processFn CDCMsgFn) error
	PostCDC(ctx context.Context, identifier int) error // to save state

	GetCDCStartPosition(stream types.StreamInterface, streamIndex int) (string, error) // returns starting CDC position from state (for predictable thread IDs)
	SetCurrentCDCPosition(stream types.StreamInterface, position string)               // updates the current CDC position in state (for recovery)
	GetCDCPosition(streamID string) string                                             // returns current CDC position (binlog pos for MySQL, LSN for Postgres)
}
