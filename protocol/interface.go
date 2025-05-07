package protocol

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/types"
)

type Config interface {
	Validate() error
}

type Connector interface {
	// Setting up config reference in driver i.e. must be pointer
	GetConfigRef() Config
	Spec() any
	// Sets up connections and perform checks; doesn't load Streams
	//
	// Note: Check shouldn't be called before Setup as they're composed at Connector level
	Check() error
	Type() string
}

type Driver interface {
	Connector
	// Sets up client, doesn't performs any Checks
	Setup() error
	// Discover discovers the streams; Returns cached if already discovered
	Discover(discoverSchema bool) ([]*types.Stream, error)
	// Read is dedicatedly designed for FULL_REFRESH and INCREMENTAL mode
	Read(pool *WriterPool, stream Stream) error
	ChangeStreamSupported() bool
	SetupState(state *types.State)
}

// Bulk Read Driver
type ChangeStreamDriver interface {
	RunChangeStream(pool *WriterPool, streams ...Stream) error
	StateType() types.StateType
}

type Write = func(ctx context.Context, channel <-chan types.Record) error
type FlattenFunction = func(record types.Record) (types.Record, error)

type Writer interface {
	Connector
	// Setup sets up an Adapter for dedicated use for a stream
	// avoiding the headover for different streams
	Setup(stream Stream, opts *Options) error
	// Write function being used by drivers
	Write(ctx context.Context, record types.RawRecord) error

	// ReInitiationRequiredOnSchemaEvolution is implemented by Writers incase the writer needs to be re-initialized
	// such as when writing parquet files, but in destinations like Kafka/Clickhouse/BigQuery they can handle
	// schema update with an Alter Query
	Flattener() FlattenFunction
	// EvolveSchema updates the schema based on changes.
	// Need to pass olakeTimestamp as end argument to get the correct partition path based on record ingestion time.
	EvolveSchema(bool, bool, map[string]*types.Property, types.Record, time.Time) error
	Close() error
}

type Stream interface {
	ID() string
	Self() *types.ConfiguredStream
	Name() string
	Namespace() string
	Schema() *types.TypeSchema
	GetStream() *types.Stream
	GetSyncMode() types.SyncMode
	SupportedSyncModes() *types.Set[types.SyncMode]
	Cursor() string
	Validate(source *types.Stream) error
	NormalizationEnabled() bool
}

type State interface {
	ResetStreams()
	SetType(typ types.StateType)
	GetCursor(stream *types.ConfiguredStream, key string) any
	SetCursor(stream *types.ConfiguredStream, key, value any)
	GetChunks(stream *types.ConfiguredStream) *types.Set[types.Chunk]
	SetChunks(stream *types.ConfiguredStream, chunks *types.Set[types.Chunk])
	RemoveChunk(stream *types.ConfiguredStream, chunk types.Chunk)
	SetGlobalState(globalState any)
}
