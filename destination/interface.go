package destination

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

// Destination represents the process-level destination manager.
type Destination interface {
	Type() string
	// connection test and validation of config
	Check(ctx context.Context) error
	// drop streams used to drop tables from destination
	DropTables(ctx context.Context, dropStreams []types.StreamInterface) error
	// NewWriterThread creates a thread-specific writer for a stream.
	NewWriterThread(ctx context.Context, stream types.StreamInterface, schema any, opts *Options) (Writer, any, *types.MetadataState, error)
	// Cleanup tears down destination-owned process resources (e.g. the shared JVM).
	Close(ctx context.Context) error
}

// Writer represents a thread-specific writer for a stream.
type Writer interface {
	// Write function being used by drivers
	Write(ctx context.Context, record []types.RawRecord) error
	// flatten data and validates thread schema (return true if thread schema is different w.r.t records)
	FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error)
	// EvolveSchema updates the schema based on changes.
	EvolveSchema(ctx context.Context, globalSchema, recordsSchema any) (any, error)
	// cleans and commits(if no error) files and data to destination
	CommitAndCleanup(ctx context.Context, finalMetadataState any) error
}
