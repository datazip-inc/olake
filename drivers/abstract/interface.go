package abstract

import (
	"context"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
)

// DiscoverSampleTiers are the record counts a SampledSchemaProducer builds a stream's schema from,
// smallest first. Discover runs one tier for every stream before starting the next, so when the
// discover timeout hits after the first tier, every stream keeps the largest tier it completed.
var DiscoverSampleTiers = []int{1, 100, 10000}

// SampledSchemaProducer is implemented by drivers that infer a stream's schema by reading records.
type SampledSchemaProducer interface {
	// ProduceSampledSchema builds the full stream from at most limit records.
	ProduceSampledSchema(ctx context.Context, streamID types.StreamID, limit int) (*types.Stream, error)
}

type BackfillMsgFn func(ctx context.Context, message map[string]any, sourceBytes int64) error
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
	GetStreamNames(ctx context.Context) ([]types.StreamID, error)
	ProduceSchema(ctx context.Context, stream types.StreamID) (*types.Stream, error)
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
	StreamChanges(ctx context.Context, identifier int, metadataState map[string]any, processFn CDCMsgFn) (any, error)
	PostCDC(ctx context.Context, identifier int) error // to save state
}
