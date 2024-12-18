package protocol

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/piyushsingariya/relec/memory"
	"github.com/piyushsingariya/relec/safego"
	"golang.org/x/sync/errgroup"
)

type NewFunc func() Writer
type InsertFunction func(record types.Record) (exit bool, err error)

var RegisteredWriters = map[types.AdapterType]NewFunc{}

type Options struct {
	Identifier string
	Number     int64
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

// Initialize new adapter thread for writing into destination
func (w *WriterPool) NewThread(parent context.Context, stream Stream, options ...ThreadOptions) (InsertFunction, error) {
	// setup options
	opts := &Options{}
	for _, one := range options {
		one(opts)
	}

	var thread Writer
	threadInitialized := make(chan struct{}) // to handle the first initialization

	w.threadCounter.Add(1)
	frontend := make(chan types.Record) // To be given to Reader
	backend := make(chan types.Record)  // To be given to Writer
	errChan := make(chan error)
	child, childCancel := context.WithCancel(parent)

	// spawnWriter spawns a writer process with child context
	spawnWriter := func() {
		spawned := make(chan struct{})

		w.group.Go(func() error {
			defer childCancel() // spawnWriter uses childCancel to exit the middleware

			thread = w.init() // set the thread variable
			w.tmu.Lock()      // lock for concurrent access of w.config
			if err := utils.Unmarshal(w.config, thread.GetConfigRef()); err != nil {
				w.tmu.Unlock() // unlock
				return err
			}
			w.tmu.Unlock() // unlock

			if err := thread.Setup(stream, opts); err != nil {
				return err
			}

			safego.Close(spawned)           // signal spawnWriter to exit
			safego.Close(threadInitialized) // close after initialization

			err := func() error {
				defer w.threadCounter.Add(-1)

				return utils.ErrExecSequential(func() error {
					// Close backend here since with writer exit; processes pushing into backend will be stuck
					// since no reader on backend
					defer safego.Close(backend)

					return thread.Write(child, backend)
				}, thread.Close)
			}()
			// if err != nil && !strings.Contains(err.Error(), "short write") {
			if err != nil {
				errChan <- err
			}

			return err
		})

		<-spawned
	}

	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema())

	// middleware that has abstracted the repetition code from Writers
	w.group.Go(func() error {
		err := func() error {
			// not defering canceling the child context so that writing process
			// can finish writing all the records pushed into the channel
			defer safego.Close(backend)
			defer func() {
				safego.Close(frontend)
			}()

			<-threadInitialized // wait till thread is initialized for the first time
			flatten := thread.Flattener()
		main:
			for {
				select {
				case <-child.Done():
					break main
				case <-parent.Done():
					break main
				default:
					// Note: Why printing state logic is at start
					// i.e. because if Writer has exited before pushing into the channel;
					// first the code will be blocked, second we might endup printing wrong state
					if w.TotalRecords()%int64(batchSize_) == 0 {
						if !state.IsZero() {
							logger.LogState(state)
						}
					}

					record, ok := <-frontend
					if !ok {
						break main
					}

					memory.Lock(child)             // lock until memory free
					record, err := flatten(record) // flatten the record first
					if err != nil {
						return err
					}

					change, typeChange, mutations := fields.Process(record)
					if change || typeChange {
						w.tmu.Lock()
						stream.Schema().Override(fields.ToProperties()) // update the schema in Stream
						w.tmu.Unlock()
					}

					// handle schema evolution here
					if (typeChange && thread.ReInitiationOnTypeChange()) || (change && thread.ReInitiationOnNewColumns()) {
						childCancel()                                   // Close the current writer and spawn new
						child, childCancel = context.WithCancel(parent) // replace the original child context and cancel function
						spawnWriter()                                   // spawn a writer with newer context
					} else if typeChange || change {
						err := thread.EvolveSchema(mutations.ToProperties())
						if err != nil {
							return fmt.Errorf("failed to evolve schema: %s", err)
						}
					}

					err = typeutils.ReformatRecord(fields, record)
					if err != nil {
						return err
					}

					if !safego.Insert(backend, record) {
						return nil // Exit here since backend closed by backend reader
					}

					w.recordCount.Add(1) // increase the record count
				}
			}

			return nil
		}()
		if err != nil {
			errChan <- fmt.Errorf("error in writer middleware: %s", err)
		}

		return err
	})

	spawnWriter()
	return func(record types.Record) (bool, error) {
		select {
		case err := <-errChan:
			childCancel() // cancel the writers
			return false, err
		default:
			if !safego.Insert(frontend, record) {
				return true, nil
			}

			return false, nil
		}
	}, nil
}

// Returns total records fetched at runtime
func (w *WriterPool) TotalRecords() int64 {
	return w.recordCount.Load()
}

func (w *WriterPool) Wait() error {
	return w.group.Wait()
}
