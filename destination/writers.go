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

const DestError = "destination error"

type (
	NewFunc        func() Writer
	InsertFunction func(record types.RawRecord) (err error)
	CloseFunction  func()
	WriterOption   func(Writer) error

	Options struct {
		Identifier string
		Number     int64
		Backfill   bool
		ThreadID   string
	}

	ThreadOptions func(opt *Options)
	writerSchema  struct {
		mu     sync.RWMutex
		schema any
	}

	Stats struct {
		TotalRecordsToSync atomic.Int64 // total record that are required to sync
		ReadCount          atomic.Int64 // records that got read
		ThreadCount        atomic.Int64 // total number of writer threads
	}

	WriterPool struct {
		configMutex  sync.Mutex
		stats        *Stats
		config       any
		init         NewFunc
		writerSchema sync.Map
	}

	// writer thread used by reader
	WriterThread struct {
		stats          *Stats
		buffer         []types.RawRecord
		threadID       string
		writer         Writer
		batchSize      int
		streamArtifact *writerSchema
	}
)

var RegisteredWriters = map[types.DestinationType]NewFunc{}

func WithIdentifier(identifier string) ThreadOptions {
	return func(opt *Options) {
		opt.Identifier = identifier
	}
}

func WithNumber(number int64) ThreadOptions {
	return func(opt *Options) {
		opt.Number = number
	}
}

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

func NewWriterPool(ctx context.Context, config *types.WriterConfig, syncStreams, dropStreams []string) (*WriterPool, error) {
	newfunc, found := RegisteredWriters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	adapter := newfunc()
	if err := utils.Unmarshal(config.WriterConfig, adapter.GetConfigRef()); err != nil {
		return nil, err
	}

	err := adapter.Check(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to test destination: %s", err)
	}

	if dropStreams != nil {
		if err := adapter.DropStreams(ctx, dropStreams); err != nil {
			return nil, fmt.Errorf("failed to clear destination: %s", err)
		}
	}

	pool := &WriterPool{
		stats: &Stats{
			TotalRecordsToSync: atomic.Int64{},
			ThreadCount:        atomic.Int64{},
			ReadCount:          atomic.Int64{},
		},
		config: config.WriterConfig,
		init:   newfunc,
	}

	for _, stream := range syncStreams {
		pool.writerSchema.Store(stream, &writerSchema{
			mu:     sync.RWMutex{},
			schema: nil,
		})
	}

	return pool, nil
}

func (w *WriterPool) AddRecordsToSyncStats(count int64) {
	w.stats.TotalRecordsToSync.Add(count)
}

func (w *WriterPool) GetStats() *Stats {
	return w.stats
}

func (w *WriterPool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (*WriterThread, error) {
	w.stats.ThreadCount.Add(1)

	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	rawStreamArtifact, ok := w.writerSchema.Load(stream.ID())
	if !ok {
		return nil, fmt.Errorf("failed to get stream artifacts for stream[%s]", stream.ID())
	}

	streamArtifact, ok := rawStreamArtifact.(*writerSchema)
	if !ok {
		return nil, fmt.Errorf("failed to convert raw stream artifact[%T] to *StreamArtifact struct", rawStreamArtifact)
	}

	var writerThread Writer
	err := func() error {
		// init writer with configurations
		writerThread = w.init()
		w.configMutex.Lock()
		err := utils.Unmarshal(w.config, writerThread.GetConfigRef())
		w.configMutex.Unlock()
		if err != nil {
			return err
		}

		// setup table and schema
		streamArtifact.mu.Lock()
		defer streamArtifact.mu.Unlock()

		output, err := writerThread.Setup(ctx, stream, streamArtifact.schema, opts)
		if err != nil {
			return fmt.Errorf("failed to setup the writer thread: %s", err)
		}

		if streamArtifact.schema == nil {
			streamArtifact.schema = output
		}

		return nil
	}()
	if err != nil {
		return nil, fmt.Errorf("failed to setup writer thread: %s", err)
	}

	return &WriterThread{
		buffer:         []types.RawRecord{},
		batchSize:      10000,
		threadID:       opts.ThreadID,
		writer:         writerThread,
		stats:          w.stats,
		streamArtifact: streamArtifact,
	}, nil
}

func (wt *WriterThread) Push(ctx context.Context, record types.RawRecord) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context closed")
	default:
		wt.stats.ReadCount.Add(1)
		wt.buffer = append(wt.buffer, record)
		if len(wt.buffer) >= wt.batchSize {
			err := wt.flush(ctx, wt.buffer)
			if err != nil {
				return fmt.Errorf("failed to flush data: %s", err)
			}
			// empty buffer
			wt.buffer = wt.buffer[:0]
		}
		return nil
	}
}

func (wt *WriterThread) flush(ctx context.Context, buf []types.RawRecord) (err error) {
	// skip empty buffers
	if len(buf) == 0 {
		return nil
	}

	// create flush context
	flushCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	evolution, buf, threadSchema, err := wt.writer.FlattenAndCleanData(buf)
	if err != nil {
		return fmt.Errorf("failed to flatten and clean data: %s", err)
	}

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

func (wt *WriterThread) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context closed")
	default:
		defer wt.stats.ThreadCount.Add(-1)
		err := wt.flush(ctx, wt.buffer)
		if err != nil {
			return fmt.Errorf("failed to flush data while closing: %s", err)
		}

		wt.streamArtifact.mu.Lock()
		defer wt.streamArtifact.mu.Unlock()

		return wt.writer.Close(ctx)
	}
}
