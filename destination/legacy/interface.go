// Package legacydst declares the legacy (row-based) destination contract.
//
// A legacy destination implements the Writer interface, registers itself in
// RegisteredWriters via init(), and is driven by LegacyPool / LegacyThread
// (see pool.go) which fan records out batch-by-batch.
//
// This package is symmetric with destination/arrow:
//
//	destination/legacy/interface.go  ←→  destination/arrow/interface.go
//	destination/legacy/pool.go       ←→  destination/arrow/pool.go
package legacydst

import (
	"context"

	dstcore "github.com/datazip-inc/olake/destination/core"
	"github.com/datazip-inc/olake/types"
)

// Config is the validatable destination config implemented by every legacy
// destination (iceberg.Config, parquet.Config, …).
type Config interface {
	Validate() error
}

// Write and FlattenFunction are alias types kept for backwards-compatibility
// with the old destination package signatures.
type (
	Write           = func(ctx context.Context, channel <-chan types.Record) error
	FlattenFunction = func(record types.Record) (types.Record, error)
)

// Options is a type alias to dstcore.Options so that legacy Writer
// implementations only need to import this package. The struct itself lives
// in dstcore because it is shared by both pool flavours.
type Options = dstcore.Options

// Writer is the legacy, row-based destination contract. Every legacy
// destination (iceberg, parquet) implements all eight methods.
type Writer interface {
	GetConfigRef() Config
	Spec() any
	Type() string

	// Check sets up connections and performs a health check; it does not
	// load Streams. Check is composed at the connector level and must not
	// be invoked before Setup.
	Check(ctx context.Context) error

	// Setup prepares the adapter for a dedicated stream so that subsequent
	// threads of the same stream can reuse the same writer instance.
	Setup(ctx context.Context, stream types.StreamInterface, schema any, opts *Options) (any, *types.MetadataState, error)

	// Write is invoked per batch by LegacyThread.
	Write(ctx context.Context, record []types.RawRecord) error

	// FlattenAndCleanData flattens incoming records and reports whether
	// the per-thread schema has drifted from the batch schema.
	FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error)

	// EvolveSchema updates the destination schema based on detected
	// drift. The returned value becomes the new global schema for this
	// stream.
	EvolveSchema(ctx context.Context, globalSchema, recordsSchema any) (any, error)

	// DropStreams is invoked by `olake clear` and by ClearDestination.
	DropStreams(ctx context.Context, dropStreams []types.StreamInterface) error

	Close(ctx context.Context, finalMetadataState any) error
}

// NewFunc constructs a fresh Writer. Each thread of a Pool gets its own
// Writer via NewFunc so per-thread state stays isolated.
type NewFunc func() Writer

// RegisteredWriters is populated by each legacy destination's init():
//
//	func init() {
//	    legacydst.RegisteredWriters[types.Iceberg] = func() legacydst.Writer { return &Iceberg{} }
//	}
//
// LegacyPool reads from this map at construction time.
var RegisteredWriters = map[types.DestinationType]NewFunc{}
