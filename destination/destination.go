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
	initDest func(config any) (Destination, error)

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

	Pool struct {
		stats        *Stats
		destination  Destination // adapter owning destination-level resources; reused for Clear/Close and spawning writer threads
		writerSchema *sync.Map
		batchSize    int64
	}

	// writer thread used by reader
	Thread struct {
		stats          *Stats
		buffer         []types.RawRecord
		threadID       string
		writer         Writer
		batchSize      int64
		streamArtifact *writerSchema
		group          *utils.CxGroup
	}
)

var RegisteredWriters = map[types.DestinationType]initDest{}

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

func Init(config *types.WriterConfig) (Destination, error) {
	initDest, found := RegisteredWriters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	return initDest(config.WriterConfig)
}

// Pool creates destination pool which have data relevant to all threads that are spawned in destination side
func NewPool(ctx context.Context, config *types.WriterConfig, syncStreams []string, batchSize int64) (*Pool, error) {
	adapter, err := Init(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize destination: %s", err)
	}

	writerSchemaMap := sync.Map{}
	for _, stream := range syncStreams {
		writerSchemaMap.Store(stream, &writerSchema{
			mu:     sync.RWMutex{},
			schema: nil,
		})
	}

	pool := &Pool{
		stats: &Stats{
			TotalRecordsToSync: atomic.Int64{},
			ThreadCount:        atomic.Int64{},
			ReadCount:          atomic.Int64{},
			RecordsFiltered:    atomic.Int64{},
		},
		destination:  adapter,
		batchSize:    batchSize,
		writerSchema: &writerSchemaMap,
	}

	return pool, nil
}

// Close tears down destination-owned process resources (e.g. the Iceberg shared
// JVM) via the pool's destination adapter. Idempotent and safe to defer right
// after a successful NewPool.
func (p *Pool) Close(ctx context.Context) {
	p.destination.Close(ctx)
}

// drop tables from destination
func (p *Pool) DropTables(ctx context.Context, dropStreams []types.StreamInterface) error {
	return p.destination.DropTables(ctx, dropStreams)
}

func (p *Pool) AddRecordsToSyncStats(count int64) {
	p.stats.TotalRecordsToSync.Add(count)
}

func (p *Pool) GetStats() *Stats {
	return p.stats
}

func (p *Pool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (*Thread, *types.MetadataState, error) {
	p.stats.ThreadCount.Add(1)

	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	rawStreamArtifact, ok := p.writerSchema.Load(stream.ID())
	if !ok {
		return nil, nil, fmt.Errorf("failed to get stream artifacts for stream[%s]", stream.ID())
	}

	streamArtifact, ok := rawStreamArtifact.(*writerSchema)
	if !ok {
		return nil, nil, fmt.Errorf("failed to convert raw stream artifact[%T] to *StreamArtifact struct", rawStreamArtifact)
	}

	writerThread, prevStreamState, err := func() (Writer, *types.MetadataState, error) {
		// create thread writer from the destination adapter
		// setup table and schema
		streamArtifact.mu.Lock()
		defer streamArtifact.mu.Unlock()
		writerThread, threadSchema, prevStreamState, err := p.destination.NewWriterThread(ctx, stream, streamArtifact.schema, opts)
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
	return &Thread{
		buffer:         []types.RawRecord{},
		batchSize:      p.batchSize,
		threadID:       opts.ThreadID,
		writer:         writerThread,
		stats:          p.stats,
		streamArtifact: streamArtifact,
		group:          utils.NewCGroupWithLimit(ctx, 1), // currently only one thread (To make sure flush can run parallel when buffer filling)
	}, prevStreamState, nil
}

func (t *Thread) Push(ctx context.Context, record types.RawRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.group.Ctx().Done():
		// if group context is done, return the group err (retry error as there can be rate limits on s3)
		return t.group.Block()
	default:
		t.stats.ReadCount.Add(1)
		t.buffer = append(t.buffer, record)
		if len(t.buffer) >= int(t.batchSize) {
			buf := make([]types.RawRecord, len(t.buffer))
			copy(buf, t.buffer)
			t.buffer = t.buffer[:0]
			t.group.Add(func(ctx context.Context) error {
				return t.flush(ctx, buf)
			})
		}
		return nil
	}
}

func (t *Thread) flush(ctx context.Context, buf []types.RawRecord) (err error) {
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
	evolution, buf, threadSchema, err := t.writer.FlattenAndCleanData(flushCtx, buf)
	if err != nil {
		return fmt.Errorf("failed to flatten and clean data: %s", err)
	}
	t.stats.RecordsFiltered.Add(int64(recordsCountBeforeFiltering - len(buf)))
	// TODO: after flattening record type raw_record not make sense
	if evolution {
		t.streamArtifact.mu.Lock()
		newSchema, err := t.writer.EvolveSchema(flushCtx, t.streamArtifact.schema, threadSchema)
		if err == nil && newSchema != nil {
			t.streamArtifact.schema = newSchema
		}
		t.streamArtifact.mu.Unlock()
		if err != nil {
			return fmt.Errorf("failed to evolve schema: %s", err)
		}
	}

	if err := t.writer.Write(flushCtx, buf); err != nil {
		return fmt.Errorf("failed to write records: %s", err)
	}

	logger.Infof("Thread[%s]: successfully wrote %d records", t.threadID, len(buf))
	return nil
}

func (t *Thread) Close(ctx context.Context, finalMetadataState any) (err error) {
	select {
	case <-ctx.Done():
		err := t.writer.CommitAndCleanup(ctx, finalMetadataState)
		if err != nil {
			return fmt.Errorf("failed to close writer: %s", err)
		}
		return nil
	default:
		defer t.stats.ThreadCount.Add(-1)
		defer func() {
			t.streamArtifact.mu.Lock()
			defer t.streamArtifact.mu.Unlock()

			closeErr := t.writer.CommitAndCleanup(ctx, finalMetadataState)
			if closeErr != nil {
				err = utils.Ternary(err == nil, closeErr, fmt.Errorf("%s: flush error: %w", closeErr, err)).(error)
			}
		}()

		t.group.Add(func(ctx context.Context) error {
			return t.flush(ctx, t.buffer)
		})

		if err := t.group.Block(); err != nil {
			return fmt.Errorf("failed to flush data while closing: %s", err)
		}
		return nil
	}
}
