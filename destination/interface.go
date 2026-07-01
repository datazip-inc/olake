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
	Spec() any

	Type() string
	// checks if the destination config is valid
	Check(ctx context.Context) error
	// setup the writer for the thread
	Setup(ctx context.Context, stream types.StreamInterface, schema any, opts *Options) (any, *types.MetadataState, error)
	// write the records to the destination
	Write(ctx context.Context, record []types.RawRecord) error
	// flatten data and validates thread schema (return true if thread schema is different w.r.t records)
	FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error)
	// EvolveSchema updates the schema based on changes.
	EvolveSchema(ctx context.Context, globalSchema, recordsSchema any) (any, error)
	// drop the streams from the destination
	DropStreams(ctx context.Context, dropStreams []types.StreamInterface) error
	// cleans and commits(if no error) files and data to destination
	Close(ctx context.Context, finalMetadataState any) error
}
