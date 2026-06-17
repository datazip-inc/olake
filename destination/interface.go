package destination

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

type Config interface {
	Validate() error
}

type Write = func(ctx context.Context, channel <-chan types.Record) error
type FlattenFunction = func(record types.Record) (types.Record, error)

type Writer interface {
	// GetConfigRef returns a fresh config pointer to unmarshal destination config into.
	GetConfigRef() Config
	// SetConfig assigns a shared, pre-parsed read-only config instance provided by the writer pool.
	SetConfig(Config)
	Spec() any
	Type() string
	// Sets up connections and perform checks; doesn't load Streams
	//
	// Note: Check shouldn't be called before Setup as they're composed at Connector level
	Check(ctx context.Context) error
	// Setup sets up an Adapter for dedicated use for a stream
	// avoiding the headover for different streams
	Setup(ctx context.Context, stream types.StreamInterface, schema any, opts *Options) (any, *types.MetadataState, error)
	// Write function being used by drivers
	Write(ctx context.Context, record []types.RawRecord) error
	// flatten data and validates thread schema (return true if thread schema is different w.r.t records)
	FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error)
	// EvolveSchema updates the schema based on changes.
	// Need to pass olakeTimestamp as end argument to get the correct partition path based on record ingestion time.
	EvolveSchema(ctx context.Context, globalSchema, recordsSchema any) (any, error)
	// DropStreams is used to clear the destination before re-writing the stream
	DropStreams(ctx context.Context, dropStreams []types.StreamInterface) error
	Close(ctx context.Context, finalMetadataState any) error
}

// Initializable is implemented by destinations that own long-lived process
// resources (currently: the Iceberg shared JVM). The protocol layer calls
// Initialize once per CLI invocation, via the Initialize dispatcher below, before
// any Check/Setup/DropStreams work — so the resource is started exactly once at
// a known point and every later path can simply read it without locking.
type Initializable interface {
	Initialize(ctx context.Context) error
}

// Shutdownable is implemented by destinations that own long-lived process
// resources (currently: the Iceberg shared JVM). The protocol layer calls
// Shutdown once per CLI invocation, via the Shutdown dispatcher below, so the
// JVM is torn down on normal sync/check/clear completion. Signal-based
// teardown is handled inside the destination itself.
type Shutdownable interface {
	Shutdown(ctx context.Context) error
}
