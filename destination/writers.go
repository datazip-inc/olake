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
	}

	ThreadOptions   func(opt *Options)
	StreamArtifacts struct {
		mutex  sync.RWMutex
		schema any
	}

	Stats struct {
		readCount     atomic.Int64
		writeCount    atomic.Int64
		writerThreads atomic.Int64
	}
	WriterPool struct {
		stats           *Stats
		totalRecords    atomic.Int64
		config          any
		init            NewFunc
		streamArtifacts sync.Map
	}

	ThreadEvent struct {
		buffer         []types.RawRecord
		writer         Writer
		stats          *Stats
		batchSize      int
		streamArtifact *StreamArtifacts
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

func (t *ThreadEvent) Push(ctx context.Context, record types.RawRecord) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context closed")
	default:
		t.stats.readCount.Add(1)
		t.buffer = append(t.buffer, record)
		if len(t.buffer) > t.batchSize {
			err := t.flush(ctx, t.buffer)
			if err != nil {
				return fmt.Errorf("failed to flush data: %s", err)
			}
			// empty buffer
			t.buffer = t.buffer[:0]
		}
		return nil
	}
}

func (t *ThreadEvent) Close(ctx context.Context) error {
	defer t.stats.writerThreads.Add(-1)
	err := t.flush(ctx, t.buffer)
	if err != nil {
		return fmt.Errorf("failed to flush data while closing: %s", err)
	}

	t.streamArtifact.mutex.Lock()
	defer t.streamArtifact.mutex.Unlock()

	return t.writer.Close(ctx)
}

func (t *ThreadEvent) flush(ctx context.Context, buf []types.RawRecord) (err error) {
	// skip empty buffers
	if len(buf) == 0 {
		return nil
	}

	// create flush context
	flushCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	t.streamArtifact.mutex.RLock()
	cachedSchema := t.streamArtifact.schema
	t.streamArtifact.mutex.RUnlock()

	schemaEvolution, newSchema, err := t.writer.FlattenAndCleanData(cachedSchema, buf)
	if err != nil {
		return fmt.Errorf("failed to flatten and clean data: %s", err)
	}

	if schemaEvolution {
		t.streamArtifact.mutex.Lock()
		t.streamArtifact.schema = newSchema
		t.streamArtifact.mutex.Unlock()
		if err := t.writer.EvolveSchema(flushCtx, newSchema); err != nil {
			return fmt.Errorf("failed to evolve schema: %s", err)
		}
		cachedSchema = newSchema
	}

	if err := t.writer.Write(flushCtx, cachedSchema, buf); err != nil {
		return fmt.Errorf("failed to write records: %s", err)
	}

	t.stats.writeCount.Add(int64(len(buf)))
	logger.Infof("Successfully wrote %d records", len(buf))
	return nil
}

func (w *WriterPool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (*ThreadEvent, error) {
	w.stats.writerThreads.Add(1)

	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	rawStreamArtifact, ok := w.streamArtifacts.Load(stream.ID())
	if !ok {
		return nil, fmt.Errorf("failed to get stream artifacts for stream[%s]", stream.ID())
	}

	streamArtifact, ok := rawStreamArtifact.(*StreamArtifacts)
	if !ok {
		return nil, fmt.Errorf("failed to convert raw stream artifact[%T] to *StreamArtifact struct", rawStreamArtifact)
	}

	var writerThread Writer
	err := func() error {
		streamArtifact.mutex.Lock()
		defer streamArtifact.mutex.Unlock()

		writerThread = w.init()
		if err := utils.Unmarshal(w.config, writerThread.GetConfigRef()); err != nil {
			return err
		}

		output, err := writerThread.Setup(ctx, stream, streamArtifact.schema == nil, opts)
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

	return &ThreadEvent{
		buffer:         []types.RawRecord{},
		batchSize:      10000,
		writer:         writerThread,
		stats:          w.stats,
		streamArtifact: streamArtifact,
	}, nil
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
		totalRecords: atomic.Int64{},
		stats: &Stats{
			writerThreads: atomic.Int64{},
			readCount:     atomic.Int64{},
			writeCount:    atomic.Int64{},
		},
		config: config.WriterConfig,
		init:   newfunc,
	}

	for _, stream := range syncStreams {
		pool.streamArtifacts.Store(stream, &StreamArtifacts{
			mutex:  sync.RWMutex{},
			schema: nil,
		})
	}

	return pool, nil
}

func (w *WriterPool) SyncedRecords() int64 {
	return w.stats.writeCount.Load()
}

func (w *WriterPool) AddRecordsToSync(recordCount int64) {
	w.totalRecords.Add(recordCount)
}

func (w *WriterPool) GetRecordsToSync() int64 {
	return w.totalRecords.Load()
}

func (w *WriterPool) GetReadRecords() int64 {
	return w.stats.readCount.Load()
}

func (w *WriterPool) GetWriterThreads() int64 {
	return w.stats.writerThreads.Load()
}
