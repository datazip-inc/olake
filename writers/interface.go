package writers

import (
	"context"

	"github.com/datazip-inc/olake/types"
)

// PartitionedRollingWriter is the entry point of this package: a partitioned,
// rolling, transactional file writer. Give it a Partitioner (which partition does a
// record belong to?) and a NewRollingWriter (how to write one partition's records);
// it routes each record to the right partition and writes partitions concurrently,
// then commits them together via two-phase commit.
//
//	w := writers.New(basePath, partitionBy, newWriter, writers.Config{})
//	w.Write(ctx, records) // call per batch; partitions are written in parallel
//	w.Close(ctx)          // flush every partition's last file
//	w.Commit(ctx)         // make everything durable — or w.Abort(ctx) on failure
//
// A PartitionedRollingWriter is driven by a single goroutine (in OLake, one per
// WriterThread): it is not safe for concurrent external calls, but parallelizes
// internally across partitions. See SingleRoller for the ready-made
// RollingWriter that append-only backends (parquet/S3) plug in.
type PartitionedRollingWriter interface {
	Write(ctx context.Context, records []types.RawRecord) error
	Close(ctx context.Context) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

// RollingWriter writes every record routed to a single partition. The
// PartitionedRollingWriter owns one per active partition and guarantees that calls
// to a given instance are serialized — so per-partition state (an open Roller, the
// Iceberg upsert tracker) needs no synchronization — while different instances run
// concurrently.
//
// A backend supplies the implementation. The common case, one data-file stream per
// partition, is covered by SingleRoller; Iceberg plugs in a custom type that drives
// data + delete rollers and tracks dedup state.
type RollingWriter interface {
	// Write consumes a batch of records already known to belong to this
	// partition, in arrival order.
	Write(ctx context.Context, rows []*types.RawRecord) error
	// Close flushes any open/trailing file. Called once, after the final Write.
	// It stages output but does not make it durable — that is the job of committer
	// (see PartitionedRollingWriter.Commit).
	Close(ctx context.Context) error
}
