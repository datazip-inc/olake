package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"golang.org/x/sync/errgroup"
)

const DestError = "destination error"

type (
	NewFunc        func() Writer
	InsertFunction func(record types.RawRecord) (err error)
	CloseFunction  func()
	WriterOption   func(Writer) error

	Options struct {
		Identifier  string
		Number      int64
		Backfill    bool
		CreateTable bool
	}

	ThreadOptions   func(opt *Options)
	StreamArtifacts struct {
		mutex  sync.RWMutex
		schema any
	}
	WriterPool struct {
		maxThreads      int
		batchSize       int64
		totalRecords    atomic.Int64
		readCount       atomic.Int64
		writeCount      atomic.Int64
		flushThreads    atomic.Int64
		writerThreads   atomic.Int64
		config          any
		init            NewFunc
		streamArtifacts sync.Map
	}

	ThreadEvent struct {
		recordSize     int64
		stream         types.StreamInterface
		buffer         []types.RawRecord
		errGroup       *errgroup.Group
		groupCtx       context.Context
		writer         Writer
		writerThreads  *atomic.Int64
		flushThreads   *atomic.Int64
		readCounter    *atomic.Int64
		writeCount     *atomic.Int64
		batchSize      int64
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

func (t *ThreadEvent) Push(record types.RawRecord) error {
	t.readCounter.Add(1)
	t.buffer = append(t.buffer, record)
	t.recordSize += int64(len(fmt.Sprintf("%v", record)))
	if t.recordSize > t.batchSize {
		t.recordSize = 0
		newBuffer := make([]types.RawRecord, len(t.buffer))
		copy(newBuffer, t.buffer)
		t.buffer = t.buffer[:0]
		t.errGroup.Go(func() error {
			return t.flush(newBuffer)
		})
	}
	return nil
}

func (t *ThreadEvent) Close() error {
	defer t.writerThreads.Add(-1)
	buffer := make([]types.RawRecord, len(t.buffer))
	copy(buffer, t.buffer)
	t.buffer = t.buffer[:0]
	if len(buffer) > 0 {
		t.errGroup.Go(func() error {
			return t.flush(buffer)
		})
	}
	err := t.errGroup.Wait()
	if err != nil {
		return fmt.Errorf("failed to flush batches: %s", err)
	}

	t.streamArtifact.mutex.Lock()
	defer t.streamArtifact.mutex.Unlock()
	return t.writer.Close(context.Background())
}

func (t *ThreadEvent) flush(buf []types.RawRecord) error {
	// TODO: add recovery function
	t.flushThreads.Add(1)
	defer t.flushThreads.Add(-1)

	t.streamArtifact.mutex.RLock()
	cachedSchema := t.streamArtifact.schema
	t.streamArtifact.mutex.RUnlock()

	schemaEvolution, newSchema, err := t.writer.FlattenAndCleanData(cachedSchema, buf)
	if err != nil {
		return fmt.Errorf("failed to flush data: %s", err)
	}

	if schemaEvolution {
		t.streamArtifact.mutex.Lock()
		defer t.streamArtifact.mutex.Unlock()
		t.streamArtifact.schema = newSchema
		if err := t.writer.EvolveSchema(t.groupCtx, newSchema, buf, time.Now().UTC()); err != nil {
			return fmt.Errorf("failed to evolve schema: %s", err)
		}
	}

	if err := t.writer.Write(t.groupCtx, newSchema, buf); err != nil {
		return fmt.Errorf("failed to write records: %s", err)
	}

	t.writeCount.Add(int64(len(buf)))
	return nil
}

func (w *WriterPool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (*ThreadEvent, error) {
	w.writerThreads.Add(1)

	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	rawStreamArtifact, ok := w.streamArtifacts.Load(stream.ID())
	if !ok {
		return nil, fmt.Errorf("failed to get stream lock for stream[%s]", stream.ID())
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

	// setup flush group
	flushGroup, flushCtx := errgroup.WithContext(ctx)
	flushGroup.SetLimit(w.maxThreads)

	return &ThreadEvent{
		buffer:         []types.RawRecord{},
		stream:         stream,
		errGroup:       flushGroup,
		groupCtx:       flushCtx,
		writer:         writerThread,
		batchSize:      w.batchSize,
		readCounter:    &w.readCount,
		writeCount:     &w.writeCount,
		writerThreads:  &w.writerThreads,
		flushThreads:   &w.flushThreads,
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

	maxBatchSize := determineMaxBatchSize()
	logger.Infof("writer max batch size set to: %d bytes and max threads to: %d", maxBatchSize, 2)

	pool := &WriterPool{
		maxThreads:    2, // TODO: hardcoded to 2 can have discussion what could be proper value for it
		batchSize:     maxBatchSize,
		totalRecords:  atomic.Int64{},
		writerThreads: atomic.Int64{},
		flushThreads:  atomic.Int64{},
		writeCount:    atomic.Int64{},
		config:        config.WriterConfig,
		init:          newfunc,
	}

	if syncStreams != nil {
		for _, stream := range syncStreams {
			pool.streamArtifacts.Store(stream, &StreamArtifacts{
				mutex:  sync.RWMutex{},
				schema: nil,
			})
		}
	}

	return pool, nil
}

func (w *WriterPool) SyncedRecords() int64 {
	return w.writeCount.Load()
}

func (w *WriterPool) AddRecordsToSync(recordCount int64) {
	w.totalRecords.Add(recordCount)
}

func (w *WriterPool) GetRecordsToSync() int64 {
	return w.totalRecords.Load()
}

func (w *WriterPool) GetReadRecords() int64 {
	return w.readCount.Load()
}

func (w *WriterPool) GetFlushThreads() int64 {
	return w.flushThreads.Load()
}

func (w *WriterPool) GetWriterThreads() int64 {
	return w.writerThreads.Load()
}

func determineMaxBatchSize() int64 {
	ramGB := utils.DetermineSystemMemoryGB()
	switch {
	case ramGB > 32:
		return 600 * 1024 * 1024
	case ramGB > 16:
		return 100 * 1024 * 1024
	case ramGB > 8:
		return 100 * 1024 * 1024
	default:
		return 100 * 1024 * 1024
	}
}
