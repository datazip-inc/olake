package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// ---------------------------------------------------------------------------
// Interfaces drivers consume.
// ---------------------------------------------------------------------------

// Pool is the minimum contract drivers use. Both LegacyWriterPool and
// ArrowWriterPool implement it.
type Pool interface {
	NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (Thread, *types.MetadataState, error)
	AddRecordsToSyncStats(count int64)
	GetStats() *Stats
}

// Thread is the minimum contract for a per-thread writer. Both
// LegacyWriterThread and ArrowWriterThread implement it.
type Thread interface {
	Push(ctx context.Context, record types.RawRecord) error
	Close(ctx context.Context, finalMetadataState any) error
}

// ---------------------------------------------------------------------------
// Shared option / stats / artifact types.
// ---------------------------------------------------------------------------

type (
	NewFunc        func() Writer
	InsertFunction func(record types.RawRecord) (err error)
	CloseFunction  func()
	WriterOption   func(Writer) error

	Options struct {
		Identifier  string
		Number      int64
		Backfill    bool
		ThreadID    string
		ApplyFilter bool
	}

	ThreadOptions func(opt *Options)

	// writerSchema is the per-stream artifact shared across all threads of a
	// given pool. Both pools mutate it under writerSchema.mu.
	writerSchema struct {
		mu            sync.RWMutex
		schema        any
		metadataState *types.MetadataState
	}

	Stats struct {
		TotalRecordsToSync atomic.Int64 // total record that are required to sync
		ReadCount          atomic.Int64 // records that got read
		RecordsFiltered    atomic.Int64 // records that got filtered
		ThreadCount        atomic.Int64 // total number of writer threads
	}
)

// RegisteredWriters keeps the legacy Writer implementations.
var RegisteredWriters = map[types.DestinationType]NewFunc{}

// ---------------------------------------------------------------------------
// ThreadOption builders.
// ---------------------------------------------------------------------------

func WithIdentifier(identifier string) ThreadOptions {
	return func(opt *Options) { opt.Identifier = identifier }
}

func WithNumber(number int64) ThreadOptions {
	return func(opt *Options) { opt.Number = number }
}

func WithBackfill(backfill bool) ThreadOptions {
	return func(opt *Options) { opt.Backfill = backfill }
}

func WithThreadID(threadID string) ThreadOptions {
	return func(opt *Options) { opt.ThreadID = threadID }
}

func WithApplyFilter(applyFilter bool) ThreadOptions {
	return func(opt *Options) { opt.ApplyFilter = applyFilter }
}

// ---------------------------------------------------------------------------
// Entry point used by protocol/sync.go.
// ---------------------------------------------------------------------------

// NewPool picks the right implementation:
//   - arrowOverride == nil  -> read arrow_writes from destination config
//   - arrowOverride != nil  -> CLI value wins
func NewPool(ctx context.Context, cfg *types.WriterConfig, syncStreams []string, batchSize int64, arrowOverride *bool) (Pool, error) {
	if resolveArrowMode(cfg, arrowOverride) {
		return newArrowWriterPool(ctx, cfg, syncStreams, batchSize)
	}
	return newLegacyWriterPool(ctx, cfg, syncStreams, batchSize)
}

func resolveArrowMode(cfg *types.WriterConfig, arrowOverride *bool) bool {
	if arrowOverride != nil {
		return *arrowOverride
	}
	// Best-effort peek at the raw config map for arrow_writes.
	if raw, ok := cfg.WriterConfig.(map[string]any); ok {
		if v, ok := raw["arrow_writes"].(bool); ok {
			return v
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Internal helpers shared by both pool implementations.
// ---------------------------------------------------------------------------

func newStats() *Stats {
	return &Stats{
		TotalRecordsToSync: atomic.Int64{},
		ThreadCount:        atomic.Int64{},
		ReadCount:          atomic.Int64{},
		RecordsFiltered:    atomic.Int64{},
	}
}

func registerStreamArtifacts(m *sync.Map, streams []string) {
	for _, s := range streams {
		m.Store(s, &writerSchema{
			mu:     sync.RWMutex{},
			schema: nil,
		})
	}
}

func loadStreamArtifact(m *sync.Map, streamID string) (*writerSchema, error) {
	raw, ok := m.Load(streamID)
	if !ok {
		return nil, fmt.Errorf("failed to get stream artifacts for stream[%s]", streamID)
	}
	a, ok := raw.(*writerSchema)
	if !ok {
		return nil, fmt.Errorf("failed to convert raw stream artifact[%T] to *writerSchema", raw)
	}
	return a, nil
}

func applyThreadOpts(options []ThreadOptions) *Options {
	return ApplyThreadOpts(options)
}

// ApplyThreadOpts is the exported form used by sub-packages (e.g. arrowpipe).
func ApplyThreadOpts(options []ThreadOptions) *Options {
	opts := &Options{}
	for _, one := range options {
		one(opts)
	}
	return opts
}

// runRecoveredFlush is the standard panic-recovered wrapper used by both
// thread implementations.
func runRecoveredFlush(fn func() error) (err error) {
	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic recovered in flush: %v", rec)
			}
		}
	}()
	return fn()
}

// ---------------------------------------------------------------------------
// ClearDestination — behaviour unchanged; lives here now.
// ---------------------------------------------------------------------------

func ClearDestination(ctx context.Context, config *types.WriterConfig, dropStreams []types.StreamInterface) error {
	newfunc, found := RegisteredWriters[config.Type]
	if !found {
		return fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}
	adapter := newfunc()
	if err := utils.Unmarshal(config.WriterConfig, adapter.GetConfigRef()); err != nil {
		return err
	}
	if dropStreams != nil {
		if err := adapter.DropStreams(ctx, dropStreams); err != nil {
			return fmt.Errorf("failed to drop the streams: %s", err)
		}
	}
	return nil
}
