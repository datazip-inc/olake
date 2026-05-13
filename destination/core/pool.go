// Package dstcore holds the destination-wide orchestration primitives shared
// by every pool implementation (legacydst, arrowdst, …).
//
// Why a separate package?
//
//	drivers, protocol           ──►  destination          (re-exports Pool/Thread)
//	destination                 ──►  dstcore, legacydst, arrowdst
//	destination/legacy   ──►  dstcore
//	destination/arrow    ──►  dstcore
//
// Keeping the contract types (Pool, Thread, Stats) here breaks any potential
// import cycle: both the legacy and the arrow pool packages depend on dstcore,
// not on each other, and not on the top-level "destination" facade.
package dstcore

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/datazip-inc/olake/types"
)

// Pool is the minimum contract drivers consume. Both LegacyPool and arrowPool
// implement it.
type Pool interface {
	NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (Thread, *types.MetadataState, error)
	AddRecordsToSyncStats(count int64)
	GetStats() *Stats
}

// Thread is the minimum contract for a per-thread writer. Both LegacyThread
// and arrowThread implement it.
type Thread interface {
	Push(ctx context.Context, record types.RawRecord) error
	Close(ctx context.Context, finalMetadataState any) error
}

// Stats tracks the records that flow through a Pool across all threads.
type Stats struct {
	TotalRecordsToSync atomic.Int64
	ReadCount          atomic.Int64
	RecordsFiltered    atomic.Int64
	ThreadCount        atomic.Int64
}

// NewStats returns a zeroed Stats. Exported so pool packages can construct
// their own Stats without reflection.
func NewStats() *Stats { return &Stats{} }

// Options carries per-thread settings interpreted by Pool.NewWriter and by
// Writer.Setup (legacy path). The set of fields is intentionally small and
// shared by every pool implementation so the orchestration layer stays
// destination-agnostic.
type Options struct {
	Identifier  string
	Number      int64
	Backfill    bool
	ThreadID    string
	ApplyFilter bool
}

// ThreadOptions is a functional option mutator for Options.
type ThreadOptions func(*Options)

// ApplyThreadOpts folds a slice of ThreadOptions into a fresh Options value.
func ApplyThreadOpts(opts []ThreadOptions) *Options {
	out := &Options{}
	for _, o := range opts {
		o(out)
	}
	return out
}

// WithIdentifier sets Options.Identifier.
func WithIdentifier(id string) ThreadOptions { return func(o *Options) { o.Identifier = id } }

// WithNumber sets Options.Number.
func WithNumber(n int64) ThreadOptions { return func(o *Options) { o.Number = n } }

// WithBackfill sets Options.Backfill.
func WithBackfill(b bool) ThreadOptions { return func(o *Options) { o.Backfill = b } }

// WithThreadID sets Options.ThreadID.
func WithThreadID(id string) ThreadOptions { return func(o *Options) { o.ThreadID = id } }

// WithApplyFilter sets Options.ApplyFilter.
func WithApplyFilter(b bool) ThreadOptions { return func(o *Options) { o.ApplyFilter = b } }

// RunRecoveredFlush is the standard panic-recovered wrapper used by every
// Thread implementation to guard a flush goroutine.
func RunRecoveredFlush(fn func() error) (err error) {
	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic recovered in flush: %v", rec)
			}
		}
	}()
	return fn()
}
