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
	StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error
	// specific to cdc
	CDCSupported() bool
	PreCDC(ctx context.Context, streams []types.StreamInterface) error // to init state
	StreamChanges(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error
	PostCDC(ctx context.Context, stream types.StreamInterface, success bool) error // to save state
}
