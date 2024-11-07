package protocol

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/safego"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"golang.org/x/sync/errgroup"
)

type NewFunc func() Writer

var RegisteredWriters = map[types.AdapterType]NewFunc{}

type WriterPool struct {
	recordCount   atomic.Int64
	threadCounter atomic.Int64 // Used in naming files in S3 and global count for threads
	config        any          // respective adapter config
	init          NewFunc      // To initialize exclusive destination threads
	group         *errgroup.Group
	groupCtx      context.Context
}

func NewWriter(ctx context.Context, config *types.WriterConfig) (*WriterPool, error) {
	newfunc, found := RegisteredWriters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	adapter := newfunc()
	if err := utils.Unmarshal(config.AdapterConfig, adapter.GetConfigRef()); err != nil {
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
		config:        config.AdapterConfig,
		init:          newfunc,
		group:         group,
		groupCtx:      ctx,
	}, nil
}

// Initialize new adapter thread for writing into destination
func (w *WriterPool) NewThread(ctx context.Context, stream Stream) (chan types.Record, error) {
	adapter := w.init()
	if err := utils.Unmarshal(w.config, adapter.GetConfigRef()); err != nil {
		return nil, err
	}

	if err := adapter.Setup(stream); err != nil {
		return nil, err
	}

	w.threadCounter.Add(1)
	frontend := make(chan types.Record)
	backend := make(chan types.Record)

	w.group.Go(func() error {
		defer safego.Close(backend)

	main:
		for {
			select {
			case <-ctx.Done():
				break main
			default:
				record, ok := <-frontend
				if !ok {
					break main
				}

				backend <- record
				w.logState()
			}
		}

		return nil
	})
	w.group.Go(func() error {
		defer adapter.Close()
		defer w.threadCounter.Add(-1)

		return adapter.Write(ctx, backend)
	})

	return frontend, nil
}

// Returns total records fetched at runtime
func (w *WriterPool) TotalRecords() int64 {
	return w.recordCount.Load()
}

func (w *WriterPool) Wait() error {
	return w.group.Wait()
}

func (w *WriterPool) logState() {
	if w.TotalRecords()%int64(batchSize_) == 0 {
		logger.LogState(state)
	}
}
