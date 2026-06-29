package writers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type PartitionedRollingWriterImpl struct {
	basePath  string
	partition Partitioner
	newWriter NewRollingWriter
	cfg       Config

	// partitions is mutated only on the routing path (inside Write,
	// single-threaded) and is read-only during fan-out, so it needs no lock.
	partitions map[string]RollingWriter
}

// New builds a PartitionedRollingWriter. partition routes each record to a
// Partition; newWriter builds the RollingWriter for each partition the first time
// it appears.
func New(basePath string, partition Partitioner, newWriter NewRollingWriter, cfg Config) PartitionedRollingWriter {
	return &PartitionedRollingWriterImpl{
		basePath:   basePath,
		partition:  partition,
		newWriter:  newWriter,
		cfg:        cfg.withDefaults(),
		partitions: make(map[string]RollingWriter),
	}
}

// Write routes each record to its partition and writes the partitions
// concurrently. Routing — partition + lazy writer creation — happens first on a
// single goroutine; then each partition's batch is handed to its RollingWriter in
// parallel, bounded by Config.Concurrency. The first partition error cancels the
// rest and is returned.
func (w *PartitionedRollingWriterImpl) Write(ctx context.Context, records []types.RawRecord) error {
	if len(records) == 0 {
		return nil
	}

	// Route: bucket records by partition Key (order preserved within a bucket),
	// creating a RollingWriter the first time a Key is seen.
	groups := make(map[string][]*types.RawRecord)
	order := make([]string, 0)
	for i := range records {
		p, err := w.partition(w.basePath, &records[i])
		if err != nil {
			return fmt.Errorf("failed to partition record %d: %w", i, err)
		}
		if _, seen := groups[p.Key]; !seen {
			order = append(order, p.Key)
			if err := w.ensurePartition(p); err != nil {
				return err
			}
		}
		groups[p.Key] = append(groups[p.Key], &records[i])
	}

	// Fan out: one goroutine per partition, so each RollingWriter keeps its
	// single-writer contract while distinct partitions run in parallel.
	return utils.Concurrent(ctx, order, w.cfg.Concurrency, func(ctx context.Context, key string, _ int) error {
		if err := w.partitions[key].Write(ctx, groups[key]); err != nil {
			return fmt.Errorf("failed to write partition %q: %w", key, err)
		}
		return nil
	})
}

// ensurePartition lazily creates the writer for a partition the first time it is seen.
func (w *PartitionedRollingWriterImpl) ensurePartition(p Partition) error {
	if _, ok := w.partitions[p.Key]; ok {
		return nil
	}
	pw, err := w.newWriter(p)
	if err != nil {
		return fmt.Errorf("failed to create writer for partition %q: %w", p.Key, err)
	}
	w.partitions[p.Key] = pw
	return nil
}

// committer is the per-partition half of two-phase commit: a RollingWriter that
// can make its staged output durable (Commit) or discard it (Abort). SingleRoller
// implements it by forwarding to its sink; backends that commit globally (e.g.
// Iceberg) don't, and are skipped by Commit/Abort.
type committer interface {
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

// Close flushes every partition's trailing file, concurrently. After Close the
// output is staged but not yet durable; call Commit to make it durable (or Abort
// to discard). Close is a no-op when no records were ever written.
func (w *PartitionedRollingWriterImpl) Close(ctx context.Context) error {
	return w.fanOut(ctx, func(ctx context.Context, pw RollingWriter) error {
		return pw.Close(ctx)
	})
}

// Commit makes staged output durable across all partitions, concurrently. It is
// the commit half of two-phase commit and runs over partitions whose writers
// implement committer (e.g. SingleRoller, forwarding to its Sink). Writers without
// a per-partition commit — Iceberg, which commits globally — are skipped, leaving
// the backend to run its own commit after Close.
func (w *PartitionedRollingWriterImpl) Commit(ctx context.Context) error {
	return w.fanOut(ctx, func(ctx context.Context, pw RollingWriter) error {
		if c, ok := pw.(committer); ok {
			return c.Commit(ctx)
		}
		return nil
	})
}

// Abort discards staged output across all partitions. Unlike the other phases it
// is best-effort: every partition is attempted even if some fail (so nothing is
// left staged), and all errors are joined into the return value.
func (w *PartitionedRollingWriterImpl) Abort(ctx context.Context) error {
	var (
		mu   sync.Mutex
		errs []error
	)
	_ = utils.Concurrent(ctx, w.keys(), w.cfg.Concurrency, func(ctx context.Context, key string, _ int) error {
		c, ok := w.partitions[key].(committer)
		if !ok {
			return nil
		}
		if err := c.Abort(ctx); err != nil {
			mu.Lock()
			errs = append(errs, fmt.Errorf("partition %q: %w", key, err))
			mu.Unlock()
		}
		return nil // never fail-fast: abort must be attempted for every partition
	})
	return errors.Join(errs...)
}

// fanOut runs fn against every partition writer concurrently, bounded by
// Config.Concurrency, returning the first error (which cancels the rest).
func (w *PartitionedRollingWriterImpl) fanOut(ctx context.Context, fn func(context.Context, RollingWriter) error) error {
	return utils.Concurrent(ctx, w.keys(), w.cfg.Concurrency, func(ctx context.Context, key string, _ int) error {
		return fn(ctx, w.partitions[key])
	})
}

func (w *PartitionedRollingWriterImpl) keys() []string {
	keys := make([]string, 0, len(w.partitions))
	for k := range w.partitions {
		keys = append(keys, k)
	}
	return keys
}

// Partition identifies the destination bucket for a record. Key groups records
// and names their files — a path-like string such as "country=IN/dt=2024-01-05",
// or "" for an unpartitioned stream. Meta is an opaque backend payload threaded
// from the Partitioner to NewRollingWriter: Iceberg carries the typed partition
// values it needs in commit metadata, parquet leaves it nil.
type Partition struct {
	Key  string
	Meta any
}

// Partitioner answers "which partition does this record belong to?". It runs once
// per record on the routing path (single-threaded, before any fan-out), so it may
// read shared state without locking.
type Partitioner func(basePath string, rec *types.RawRecord) (Partition, error)

// NewRollingWriter builds the RollingWriter for a partition the
// PartitionedRollingWriter is seeing for the first time. Wire the backend's
// Roller(s) + Sink here.
type NewRollingWriter func(p Partition) (RollingWriter, error)

// Config tunes the PartitionedRollingWriter. Zero values fall back to package defaults.
type Config struct {
	// Concurrency bounds how many partitions are written or flushed at once.
	// Parquet encoding is CPU- and memory-bound (each in-flight partition buffers
	// roughly one row group), so this also caps peak memory. Defaults to
	// DefaultPartitionConcurrency.
	Concurrency int
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultPartitionConcurrency
	}
	return c
}
