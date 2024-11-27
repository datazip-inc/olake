package protocol

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/safego"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"golang.org/x/sync/errgroup"
)

type NewFunc func() Writer
type InsertFunction func(record types.Record) (exit bool, err error)

var RegisteredWriters = map[types.AdapterType]NewFunc{}

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
func (w *WriterPool) NewThread(parent context.Context, stream Stream) (InsertFunction, error) {
	thread := w.init()

	w.tmu.Lock() // lock for concurrent access of w.config
	if err := utils.Unmarshal(w.config, thread.GetConfigRef()); err != nil {
		w.tmu.Unlock() // unlock
		return nil, err
	}
	w.tmu.Unlock() // unlock

	if err := thread.Setup(stream); err != nil {
		return nil, err
	}

	w.threadCounter.Add(1)
	frontend := make(chan types.Record)  // To be given to Reader
	backend := make(chan []types.Record) // To be given to Writer
	errChan := make(chan error)
	child, childCancel := context.WithCancel(parent)

	// spawnWriter spawns a writer process with child context
	spawnWriter := func() {
		w.group.Go(func() error {
			defer childCancel() // spawnWriter uses childCancel to exit the middleware

			err := func() error {
				defer w.threadCounter.Add(-1)

				return utils.ErrExecSequential(func() error {
					return thread.Write(child, backend)
				}, thread.Close)
			}()
			// if err != nil && !strings.Contains(err.Error(), "short write") {
			if err != nil {
				errChan <- err
			}

			return err
		})
	}

	fields := make(typeutils.Fields)
	fields.FromSchema(stream.Schema())

	// middleware that has abstracted the repetition code from Writers
	w.group.Go(func() error {
		err := func() error {
			defer safego.Close(backend)
			defer func() {
				safego.Close(frontend)
			}()
			// not defering canceling the child context so that writing process
			// can finish writing all the records pushed into the channel
			flatten := thread.Flattener()
			records := []types.Record{}
		main:
			for {
				select {
				case <-child.Done():
					break main
				case <-parent.Done():
					break main
				default:
					// Note: Why push logic is not at the end of the code block
					// i.e. because if Writer has exited before pushing into the channel;
					// first the code will be blocked, second we might endup printing wrong state
					if w.TotalRecords()%int64(batchSize_) == 0 {
						backend <- records
						if !state.IsZero() {
							logger.LogState(state)
						}

						records = []types.Record{}
					}

					record, ok := <-frontend
					if !ok {
						break main
					}

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

					w.recordCount.Add(1) // increase the record count
					records = append(records, record)
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
