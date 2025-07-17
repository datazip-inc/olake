package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"golang.org/x/sync/errgroup"
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

	ThreadOptions func(opt *Options)

	// ThreadEvent struct {
	// 	Close  CloseFunction
	// 	Insert InsertFunction
	// }

	WriterPool struct {
		maxThreads    int
		batchSize     int
		totalRecords  atomic.Int64
		recordCount   atomic.Int64
		readCount     atomic.Int64
		ThreadCounter atomic.Int64 // Used in naming files in S3 and global count for threads
		config        any          // respective writer config
		init          NewFunc      // To initialize exclusive destination threads
		group         *errgroup.Group
		groupCtx      context.Context
		tmu           sync.Mutex // Mutex between threads
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

// NewWriter creates a new WriterPool with optional configuration
func NewWriter(ctx context.Context, config *types.WriterConfig, dropStreams []string) (*WriterPool, error) {
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

	// Clear destination if flag is set
	if dropStreams != nil {
		if err := adapter.DropStreams(ctx, dropStreams); err != nil {
			return nil, fmt.Errorf("failed to clear destination: %s", err)
		}
	}

	group, ctx := errgroup.WithContext(ctx)
	return &WriterPool{
		maxThreads:    config.MaxThreads,
		batchSize:     config.BatchSize,
		totalRecords:  atomic.Int64{},
		recordCount:   atomic.Int64{},
		ThreadCounter: atomic.Int64{},
		config:        config.WriterConfig,
		init:          newfunc,
		group:         group,
		groupCtx:      ctx,
		tmu:           sync.Mutex{},
	}, nil
}

type ThreadEvent struct {
	*WriterPool
	stream     types.StreamInterface
	recordChan chan types.RawRecord
	parentCtx  context.Context // make it child context
	options    *Options
	errGroup   *errgroup.Group
	groupCtx   context.Context
}

// Initialize new adapter thread for writing into destination
func (w *WriterPool) NewThread(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) *ThreadEvent {
	opts := &Options{}
	for _, one := range options {
		one(opts)
	}
	group, ctx := errgroup.WithContext(ctx)
	return &ThreadEvent{
		WriterPool: w,
		recordChan: make(chan types.RawRecord, w.batchSize),
		options:    opts,
		stream:     stream,
		parentCtx:  ctx,
		errGroup:   group,
		groupCtx:   ctx,
	}
}

func (t *ThreadEvent) Push(record types.RawRecord) error {
	select {
	case t.recordChan <- record:
		if len(t.recordChan) > t.batchSize {
			t.errGroup.Go(t.flush)
		}
		t.readCount.Add(1)
		return nil
	case <-t.parentCtx.Done():
		return nil
	}
}

func (t *ThreadEvent) Close() error {
	t.errGroup.Go(t.flush)
	close(t.recordChan)
	return t.errGroup.Wait()
}

func (t *ThreadEvent) flush() error {
	// just get records equal to batch size
	fetchSize := len(t.recordChan)
	var recordArr []types.RawRecord
	for len(recordArr) < fetchSize {
		val, ok := <-t.recordChan
		if ok {
			recordArr = append(recordArr, val)
		}
	}
	// init the writer and flush records
	var thread Writer
	err := func() error {
		t.tmu.Lock() // lock for concurrent access of w.config
		defer t.tmu.Unlock()
		thread = t.init() // set the thread variable
		if err := utils.Unmarshal(t.config, thread.GetConfigRef()); err != nil {
			return err
		}
		return thread.Setup(t.stream, t.options)
	}()
	if err != nil {
		return fmt.Errorf("failed to init thread[%d]: %s", t.options.Number, err)
	}

	// insert record
	if err := thread.Write(t.parentCtx, recordArr); err != nil {
		return fmt.Errorf("failed to write record: %s", err)
	}
	t.recordCount.Add(int64(len(recordArr)))
	return nil
}

// Returns total records fetched at runtime
func (w *WriterPool) SyncedRecords() int64 {
	return w.recordCount.Load()
}

func (w *WriterPool) AddRecordsToSync(recordCount int64) {
	w.totalRecords.Add(recordCount)
}

func (w *WriterPool) GetRecordsToSync() int64 {
	return w.totalRecords.Load()
}

func (w *WriterPool) Wait() error {
	return w.group.Wait()
}
