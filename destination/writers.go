package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

type (
	initWriter func(config any) (Writer, func(ctx context.Context), error)

	Options struct {
		Backfill    bool
		ThreadID    string
		ApplyFilter bool
	}

	ThreadOptions func(opt *Options)
	writerSchema  struct {
		mu     sync.RWMutex
		schema any
	}

	Stats struct {
		TotalRecordsToSync atomic.Int64 // total record that are required to sync
		ReadCount          atomic.Int64 // records that got read
		RecordsFiltered    atomic.Int64 // records that got filtered
		ThreadCount        atomic.Int64 // total number of writer threads
	}

	WriterPool struct {
		stats        *Stats
		initWriter   initWriter
		shutdown     func(ctx context.Context)
		writerSchema sync.Map
		batchSize    int64
	}

	// writer thread used by reader
	WriterThread struct {
		stats          *Stats
		buffer         []types.RawRecord
		threadID       string
		writer         Writer
		batchSize      int64
		streamArtifact *writerSchema
		group          *utils.CxGroup
	}
)

var RegisteredWriters = map[types.DestinationType]initWriter{}

func WithBackfill(backfill bool) ThreadOptions {
	return func(opt *Options) {
		opt.Backfill = backfill
	}
}

func WithThreadID(threadID string) ThreadOptions {
	return func(opt *Options) {
		opt.ThreadID = threadID
	}
}
func WithApplyFilter(applyFilter bool) ThreadOptions {
	return func(opt *Options) {
		opt.ApplyFilter = applyFilter
	}
}

// NewWriterPool builds a writer pool for a destination. It owns the destination of
// destination-level process resources: it starts them up front (e.g. the Iceberg
// shared JVM, via Initializable), validates the connection (Check), and exposes
// Close to tear them down. Call pool.Close() when done (defer it right after a
// successful NewWriterPool).
func NewWriterPool(ctx context.Context, config *types.WriterConfig, syncStreams []string, batchSize int64) (*WriterPool, error) {
	initWriter, found := RegisteredWriters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	adapter, shutdown, err := initWriter(config.WriterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize destination: %s", err)
	}
	pool := &WriterPool{
		stats: &Stats{
			TotalRecordsToSync: atomic.Int64{},
			ThreadCount:        atomic.Int64{},
			ReadCount:          atomic.Int64{},
			RecordsFiltered:    atomic.Int64{},
		},
		initWriter: initWriter,
		shutdown:   shutdown,
		batchSize:  batchSize,
	}

	if err := adapter.Check(ctx); err != nil {
		return nil, fmt.Errorf("failed to test destination: %s", err)
	}

	for _, stream := range syncStreams {
		pool.writerSchema.Store(stream, &writerSchema{
			mu:     sync.RWMutex{},
			schema: nil,
		})
	}

	return pool, nil
}

// Shutdown tears down destination-level process resources (like the Iceberg Java server)
func (w *WriterPool) Shutdown(ctx context.Context) {
	if w.shutdown != nil {
		w.shutdown(ctx)
	}
}

func (w *WriterPool) AddRecordsToSyncStats(count int64) {
	w.stats.TotalRecordsToSync.Add(count)
}

func (w *WriterPool) GetStats() *Stats {
	return w.stats
}

func (w *WriterPool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (*WriterThread, *types.MetadataState, error) {
	w.stats.ThreadCount.Add(1)

	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	rawStreamArtifact, ok := w.writerSchema.Load(stream.ID())
	if !ok {
		return nil, nil, fmt.Errorf("failed to get stream artifacts for stream[%s]", stream.ID())
	}

	streamArtifact, ok := rawStreamArtifact.(*writerSchema)
	if !ok {
		return nil, nil, fmt.Errorf("failed to convert raw stream artifact[%T] to *StreamArtifact struct", rawStreamArtifact)
	}

	writerThread, prevStreamState, err := func() (Writer, *types.MetadataState, error) {
		// init writer and point it at the config parsed once at pool creation,
		// shared read-only across all writer threads.
		writerThread, _, err := w.initWriter(nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize writer: %s", err)
		}

		// setup table and schema
		streamArtifact.mu.Lock()
		defer streamArtifact.mu.Unlock()
		threadSchema, prevStreamState, err := writerThread.Setup(ctx, stream, streamArtifact.schema, opts)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create writer thread: %s", err)
		}
		if streamArtifact.schema == nil {
			// First thread for this stream: cache the schema so subsequent threads
			// skip parsing the schema out of the GET_OR_CREATE_TABLE response.
			// metadataState is intentionally NOT cached, every NewWriter call must
			// receive a fresh olake_2pc snapshot from Java so that retries see the
			// up-to-date committed chunk IDs / cursor positions.
			streamArtifact.schema = threadSchema
		}

		return writerThread, prevStreamState, nil
	}()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup writer thread: %s", err)
	}
	return &WriterThread{
		buffer:         []types.RawRecord{},
		batchSize:      w.batchSize,
		threadID:       opts.ThreadID,
		writer:         writerThread,
		stats:          w.stats,
		streamArtifact: streamArtifact,
		group:          utils.NewCGroupWithLimit(ctx, 1), // currently only one thread (To make sure flush can run parallel when buffer filling)
	}, prevStreamState, nil
}

func (wt *WriterThread) Push(ctx context.Context, record types.RawRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wt.group.Ctx().Done():
		// if group context is done, return the group err (retry error as there can be rate limits on s3)
		return wt.group.Block()
	default:
		wt.stats.ReadCount.Add(1)
		wt.buffer = append(wt.buffer, record)
		if len(wt.buffer) >= int(wt.batchSize) {
			buf := make([]types.RawRecord, len(wt.buffer))
			copy(buf, wt.buffer)
			wt.buffer = wt.buffer[:0]
			wt.group.Add(func(ctx context.Context) error {
				return wt.flush(ctx, buf)
			})
		}
		return nil
	}
}

func (wt *WriterThread) flush(ctx context.Context, buf []types.RawRecord) (err error) {
	// skip empty buffers
	if len(buf) == 0 {
		return nil
	}

	defer func() {
		if err == nil {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic recovered in flush: %v", rec)
			}
		}
	}()

	// create flush context
	flushCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	recordsCountBeforeFiltering := len(buf)
	evolution, buf, threadSchema, err := wt.writer.FlattenAndCleanData(flushCtx, buf)
	if err != nil {
		return fmt.Errorf("failed to flatten and clean data: %s", err)
	}
	wt.stats.RecordsFiltered.Add(int64(recordsCountBeforeFiltering - len(buf)))
	// TODO: after flattening record type raw_record not make sense
	if evolution {
		wt.streamArtifact.mu.Lock()
		newSchema, err := wt.writer.EvolveSchema(flushCtx, wt.streamArtifact.schema, threadSchema)
		if err == nil && newSchema != nil {
			wt.streamArtifact.schema = newSchema
		}
		wt.streamArtifact.mu.Unlock()
		if err != nil {
			return fmt.Errorf("failed to evolve schema: %s", err)
		}
	}

	if err := wt.writer.Write(flushCtx, buf); err != nil {
		return fmt.Errorf("failed to write records: %s", err)
	}

	logger.Infof("Thread[%s]: successfully wrote %d records", wt.threadID, len(buf))
	return nil
}

func (wt *WriterThread) Close(ctx context.Context, finalMetadataState any) (err error) {
	select {
	case <-ctx.Done():
		err := wt.writer.Close(ctx, finalMetadataState)
		if err != nil {
			return fmt.Errorf("failed to close writer: %s", err)
		}
		return nil
	default:
		defer wt.stats.ThreadCount.Add(-1)
		defer func() {
			wt.streamArtifact.mu.Lock()
			defer wt.streamArtifact.mu.Unlock()

			closeErr := wt.writer.Close(ctx, finalMetadataState)
			if closeErr != nil {
				err = utils.Ternary(err == nil, closeErr, fmt.Errorf("%s: flush error: %w", closeErr, err)).(error)
			}
		}()

		wt.group.Add(func(ctx context.Context) error {
			return wt.flush(ctx, wt.buffer)
		})

		if err := wt.group.Block(); err != nil {
			return fmt.Errorf("failed to flush data while closing: %s", err)
		}
		return nil
	}
}

func DropStreams(ctx context.Context, config *types.WriterConfig, dropStreams []types.StreamInterface) error {
	if len(dropStreams) == 0 {
		return nil
	}

	initWriter, found := RegisteredWriters[config.Type]
	if !found {
		return fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	adapter, shutdown, err := initWriter(config.WriterConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize destination: %s", err)
	}
	defer shutdown(context.Background())

	if err := adapter.DropStreams(ctx, dropStreams); err != nil {
		return fmt.Errorf("failed to drop streams: %s", err)
	}

	return nil
}
