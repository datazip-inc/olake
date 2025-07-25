package abstract

import (
	"context"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
)

type BackfillMsgFn func(message map[string]any) error
type CDCMsgFn func(message CDCChange) error
type TypeConverterFn func(value interface{}, columnType string) (interface{}, error)

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
	ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, datatypeConverter TypeConverterFn, processFn BackfillMsgFn) error
	//incremental specific
	StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, datatypeConverter TypeConverterFn, cb BackfillMsgFn) error
	// specific to cdc
	CDCSupported() bool
	PreCDC(ctx context.Context, streams []types.StreamInterface, datatypeConverter TypeConverterFn) error // to init state
	StreamChanges(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error
	PostCDC(ctx context.Context, stream types.StreamInterface, success bool) error // to save state
}
