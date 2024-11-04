package protocol

import (
	"github.com/datazip-inc/olake/types"
)

type Connector interface {
	// Setting up config reference in driver i.e. must be pointer
	GetConfigRef() any
	Spec() any
	// Sets up connections and perform checks; doesn't load Streams
	//
	// Note: Check shouldn't be called before Setup as they're composed at Connector level
	Check() error
	// Composition with Check
	//
	// Sets up connections, and perform checks; loads/setup stream as well
	//
	// Note: Check shouldn't be called before Setup as they're composed at Connector level
	Setup() error

	Type() string
}

type Driver interface {
	Connector
	// Discover returns cached streams
	//
	// TODO: Remove error return in future if not required
	Discover() ([]*types.Stream, error)
	Read(stream Stream, channel chan<- types.Record) error
	BulkRead() bool
}

// Bulk Read Driver
type BulkDriver interface {
	GroupRead(channel chan<- types.Record, streams ...Stream) error
	SetupGlobalState(state *types.State) error
	StateType() types.StateType
}

// JDBC Driver
type JDBCDriver interface {
	FullLoad(stream Stream, channel chan<- types.Record) error
	GroupRead(channel chan<- types.Record, streams ...Stream) error
	SetupGlobalState(state *types.State) error
	StateType() types.StateType
}

type Adapter interface {
	Connector
	Write(channel <-chan types.Record) error
	Create(streamName string) error
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
	GetState() any
	SetState(value any)
	BatchSize() int
	SetBatchSize(size int)
	Validate(source *types.Stream) error
}
