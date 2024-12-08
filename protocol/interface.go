package protocol

import (
	"context"

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
}

// Bulk Read Driver
type ChangeStreamDriver interface {
	RunChangeStream(pool *WriterPool, streams ...Stream) error
	SetupGlobalState(state *types.State) error
	StateType() types.StateType
}

// JDBC Driver
type JDBCDriver interface {
	FullLoad(stream Stream, channel chan<- types.Record) error
	RunChangeStream(channel chan<- types.Record, streams ...Stream) error
	SetupGlobalState(state *types.State) error
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
	Write(ctx context.Context, channel <-chan types.Record) error

	// ReInitiationRequiredOnSchemaEvolution is implemented by Writers incase the writer needs to be re-initialized
	// such as when writing parquet files, but in destinations like Kafka/Clickhouse/BigQuery they can handle
	// schema update with an Alter Query
	ReInitiationOnTypeChange() bool
	ReInitiationOnNewColumns() bool
	Flattener() FlattenFunction
	EvolveSchema(map[string]*types.Property) error
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
	InitialState() any
	GetStateCursor() any
	GetStateKey(key string) any
	SetStateCursor(value any)
	SetStateKey(key string, value any)
	Validate(source *types.Stream) error
}

type State interface {
	SetType(typ types.StateType)
	IsZero() bool
}
