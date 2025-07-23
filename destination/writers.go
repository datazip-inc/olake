package destination

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/typeutils"
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

	ThreadEvent struct {
		Close  CloseFunction
		Insert InsertFunction
	}

	WriterPool struct {
		totalRecords  atomic.Int64
		recordCount   atomic.Int64
		ThreadCounter atomic.Int64 // Used in naming files in S3 and global count for threads
		config        any          // respective writer config
		init          NewFunc      // To initialize exclusive destination threads
		group         *errgroup.Group
		groupCtx      context.Context
		tmu           sync.Mutex // Mutex between threads
	}
)

var RegisteredWriters = map[types.AdapterType]NewFunc{}

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

// Initialize new adapter thread for writing into destination
func (w *WriterPool) NewThread(parent context.Context, stream types.StreamInterface, errChan chan error, options ...ThreadOptions) *ThreadEvent {
	// setup options
	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	var thread Writer
	recordChan := make(chan types.RawRecord)
	child, childCancel := context.WithCancel(parent)

	// fields to make sure schema evolution remain specifc to one thread
	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema())

	initNewWriter := func() error {
		w.tmu.Lock() // lock for concurrent access of w.config
		defer w.tmu.Unlock()
		thread = w.init() // set the thread variable
		if err := utils.Unmarshal(w.config, thread.GetConfigRef()); err != nil {
			return err
		}
		return thread.Setup(stream, opts)
	}

	normalizeFunc := func(rawRecord types.RawRecord) (types.Record, error) {
		flattenedData, err := thread.Flattener()(rawRecord.Data) // flatten the record first
		if err != nil {
			return nil, err
		}

		// schema evolution
		change, typeChange, mutations := fields.Process(flattenedData)
		if change || typeChange {
			w.tmu.Lock()
			stream.Schema().Override(fields.ToProperties()) // update the schema in Stream
			w.tmu.Unlock()
			err := thread.EvolveSchema(change, typeChange, mutations.ToProperties(), flattenedData, rawRecord.OlakeTimestamp)
			if err != nil {
				return nil, fmt.Errorf("failed to evolve schema: %s", err)
			}
		}
		err = typeutils.ReformatRecord(fields, flattenedData)
		if err != nil {
			return nil, err
		}
		return flattenedData, nil
	}

	w.group.Go(func() (err error) {
		w.ThreadCounter.Add(1)
		defer func() {
			w.ThreadCounter.Add(-1)
			childCancel() // no more inserts
			// must be called before return
			if err != nil {
				errChan <- fmt.Errorf("%s, %s", DestError, err)
			}
			close(errChan)
		}()
		// init writer first
		if err := initNewWriter(); err != nil {
			return fmt.Errorf("failed to initialize writer: %s", err)
		}

		defer func() {
			// Need to lock as iceberg writer closes the rpc server if number of threads calling it goes to zero per stream.
			w.tmu.Lock()
			defer w.tmu.Unlock()

			if recErr := recover(); err == nil && recErr != nil {
				err = fmt.Errorf("panic recovered in writer thread: %s", recErr)
			}

			// capture error on thread close
			if err == nil {
				if threadCloseErr := thread.Close(child); threadCloseErr != nil {
					err = fmt.Errorf("failed to close thread: %s", threadCloseErr)
				}
			}
		}()

		for {
			select {
			case <-parent.Done():
				return nil
			default:
				record, ok := <-recordChan
				if !ok {
					return nil
				}
				// add insert time
				record.OlakeTimestamp = time.Now().UTC()

				// check for normalization
				if stream.NormalizationEnabled() {
					normalizedData, err := normalizeFunc(record)
					if err != nil {
						return fmt.Errorf("failed to normalize record: %s", err)
					}
					record.Data = normalizedData
				}
				// insert record
				if err := thread.Write(child, record); err != nil {
					return fmt.Errorf("failed to write record: %s", err)
				}
				w.recordCount.Add(1) // increase the record count
			}
		}
	})

	return &ThreadEvent{
		Insert: func(record types.RawRecord) error {
			select {
			case <-child.Done():
				return fmt.Errorf("%s, main writer closed", DestError)
			case recordChan <- record:
				return nil
			}
		},
		Close: func() {
			close(recordChan)
		},
	}
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
