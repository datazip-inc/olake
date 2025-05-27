package abstract

import (
	"context"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
)

type BackfillMsgFn func(message map[string]any) error
type CDCMsgFn func(message CDCChange) error

type Config interface {
	Validate() error
}

type DriverInterface interface {
	GetConfigRef() Config
	Spec() any
	Type() string
	// Sets up client, doesn't performs any Checks
	Setup(ctx context.Context) error
	// max connnection to be used
	MaxConnections() int
	// Discover discovers the streams; Returns cached if already discovered
	Discover(ctx context.Context) ([]*types.Stream, error)
	// specific to backfill
	GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) ([]types.Chunk, error)
	ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error
	// specific to cdc
	CDCSupported() bool
	PreCDC(ctx context.Context, streams []types.StreamInterface) error
	StreamChanges(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error
	PostCDC(ctx context.Context, stream types.StreamInterface, success bool) error
}
