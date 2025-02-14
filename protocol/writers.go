package protocol

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"golang.org/x/sync/errgroup"
)

type NewFunc func() Writer
type InsertFunction func(record types.RawRecord) (exit bool, err error)
type CloseFunction func()

var RegisteredWriters = map[types.AdapterType]NewFunc{}

type Options struct {
	Identifier  string
	Number      int64
	WaitChannel chan struct{}
}

type ThreadOptions func(opt *Options)

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

func WithWaitChannel(waitChannel chan struct{}) ThreadOptions {
	return func(opt *Options) {
		opt.WaitChannel = waitChannel
	}
}

type WriterPool struct {
	recordCount   atomic.Int64
	threadCounter atomic.Int64 // Used in naming files in S3 and global count for threads
	config        any          // respective writer config
	init          NewFunc      // To initialize exclusive destination threads
	group         *errgroup.Group
	groupCtx      context.Context
	tmu           sync.Mutex // Mutex between threads
}

// Shouldn't the name be NewWriterPool?
func NewWriter(ctx context.Context, config *types.WriterConfig) (*WriterPool, error) {
	newfunc, found := RegisteredWriters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	adapter := newfunc()
	if err := utils.Unmarshal(config.WriterConfig, adapter.GetConfigRef()); err != nil {
		return nil, err
	}

	err := adapter.Check()
	if err != nil {
		return nil, fmt.Errorf("failed to test destination: %s", err)
	}

	group, ctx := errgroup.WithContext(ctx)
	return &WriterPool{
		recordCount:   atomic.Int64{},
		threadCounter: atomic.Int64{},
		config:        config.WriterConfig,
		init:          newfunc,
		group:         group,
		groupCtx:      ctx,
		tmu:           sync.Mutex{},
	}, nil
}

type ThreadEvent struct {
	Close  CloseFunction
	Insert InsertFunction
}

// Initialize new adapter thread for writing into destination
func (w *WriterPool) NewThread(parent context.Context, stream Stream, options ...ThreadOptions) (*ThreadEvent, error) {
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
		thread = w.init() // set the thread variable
		err := func() error {
			w.tmu.Lock() // lock for concurrent access of w.config
			defer w.tmu.Unlock()
			if err := utils.Unmarshal(w.config, thread.GetConfigRef()); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
		return thread.Setup(stream, opts)
	}

	normalizeFunc := func(rawRecord types.RawRecord) (types.Record, error) {
		flattenedData, err := thread.Flattener()(rawRecord.Data) // flatten the record first
		if err != nil {
			return nil, err
		}

		// add constants key fields
		flattenedData[constants.OlakeID] = rawRecord.OlakeID
		flattenedData[constants.OlakeTimestamp] = rawRecord.OlakeTimestamp
		if rawRecord.DeleteTime != 0 {
			flattenedData[constants.CDCDeletedAt] = rawRecord.DeleteTime
		}

		// schema evolution
		change, typeChange, mutations := fields.Process(flattenedData)
		if change || typeChange {
			w.tmu.Lock()
			stream.Schema().Override(fields.ToProperties()) // update the schema in Stream
			w.tmu.Unlock()
			err := thread.EvolveSchema(change, typeChange, mutations.ToProperties(), flattenedData)
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

	w.group.Go(func() error {
		err := func() error {
			w.threadCounter.Add(1)
			defer func() {
				childCancel()  // no more inserts
				thread.Close() // close it after closing inserts
				// if wait channel is provided, close it
				if opts.WaitChannel != nil {
					close(opts.WaitChannel)
				}
				w.threadCounter.Add(-1)
			}()
			// init writer first
			if err := initNewWriter(); err != nil {
				return err
			}

			return func() error {
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
						record.OlakeTimestamp = time.Now().UTC().UnixMilli()
						// check for normalization
						if thread.Normalization() {
							normalizedData, err := normalizeFunc(record)
							if err != nil {
								return err
							}
							record.Data = normalizedData
						}
						// insert record
						if err := thread.Write(child, record); err != nil {
							return err
						}
						w.recordCount.Add(1) // increase the record count

						if w.TotalRecords()%batchSize == 0 {
							if !state.IsZero() {
								logger.LogState(state)
							}
						}
					}
				}
			}()
		}()
		if err != nil {
			logger.Errorf("main writer closed, with error: %s", err)
		}
		return err
	})

	return &ThreadEvent{
		Insert: func(record types.RawRecord) (bool, error) {
			select {
			case <-child.Done():
				return false, fmt.Errorf("main writer closed")
			case recordChan <- record:
				return false, nil
			}
		},
		Close: func() {
			close(recordChan)
		},
	}, nil
}

// Returns total records fetched at runtime
func (w *WriterPool) TotalRecords() int64 {
	return w.recordCount.Load()
}

func (w *WriterPool) Wait() error {
	return w.group.Wait()
}
