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
	GetConfigRef() Config
	Spec() any
	Type() string
	// Sets up connections and perform checks; doesn't load Streams
	//
	// Note: Check shouldn't be called before Setup as they're composed at Connector level
	Check(ctx context.Context) error
	// Setup sets up an Adapter for dedicated use for a stream
	// avoiding the headover for different streams
	Setup(ctx context.Context, stream types.StreamInterface, createOrLoadSchema bool, opts *Options) (any, error)
	// Write function being used by drivers
	Write(ctx context.Context, schema any, record []types.RawRecord) error
	// flatten data and validate past schema and current schema through records
	// returns true if change detected with new schema
	FlattenAndCleanData(pastSchema any, records []types.RawRecord) (bool, any, error)
	// EvolveSchema updates the schema based on changes.
	// Need to pass olakeTimestamp as end argument to get the correct partition path based on record ingestion time.
	EvolveSchema(ctx context.Context, newSchema any) error
	// DropStreams is used to clear the destination before re-writing the stream
	DropStreams(ctx context.Context, selectedStream []string) error
	Close(ctx context.Context) error
}
