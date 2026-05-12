package destination

import (
	"context"
	"fmt"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// LegacyWriterPool is the pool implementation that matches today's behaviour:
// flatten in Go, call Writer.Write per batch. It implements destination.Pool.
type LegacyWriterPool struct {
	configMutex  sync.Mutex
	stats        *Stats
	config       any
	init         NewFunc
	writerSchema sync.Map
	batchSize    int64
}

// LegacyWriterThread is the per-thread writer for the legacy path. It
// implements destination.Thread.
type LegacyWriterThread struct {
	stats          *Stats
	buffer         []types.RawRecord
	threadID       string
	writer         Writer
	batchSize      int64
	streamArtifact *writerSchema
	group          *utils.CxGroup
}

func newLegacyWriterPool(ctx context.Context, config *types.WriterConfig, syncStreams []string, batchSize int64) (*LegacyWriterPool, error) {
	newfunc, found := RegisteredWriters[config.Type]
	if !found {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", config.Type)
	}

	adapter := newfunc()
	if err := utils.Unmarshal(config.WriterConfig, adapter.GetConfigRef()); err != nil {
		return nil, err
	}
	if err := adapter.Check(ctx); err != nil {
		return nil, fmt.Errorf("failed to test destination: %s", err)
	}

	pool := &LegacyWriterPool{
		stats:     newStats(),
		config:    config.WriterConfig,
		init:      newfunc,
		batchSize: batchSize,
	}
	registerStreamArtifacts(&pool.writerSchema, syncStreams)
	return pool, nil
}

func (w *LegacyWriterPool) AddRecordsToSyncStats(count int64) { w.stats.TotalRecordsToSync.Add(count) }
func (w *LegacyWriterPool) GetStats() *Stats                  { return w.stats }

func (w *LegacyWriterPool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...ThreadOptions) (Thread, *types.MetadataState, error) {
	w.stats.ThreadCount.Add(1)
	opts := applyThreadOpts(options)

	artifact, err := loadStreamArtifact(&w.writerSchema, stream.ID())
	if err != nil {
		return nil, nil, err
	}

	var writerImpl Writer
	prevStreamState, err := func() (*types.MetadataState, error) {
		writerImpl = w.init()
		w.configMutex.Lock()
		err := utils.Unmarshal(w.config, writerImpl.GetConfigRef())
		w.configMutex.Unlock()
		if err != nil {
			return nil, err
		}

		artifact.mu.Lock()
		defer artifact.mu.Unlock()

		output, prev, err := writerImpl.Setup(ctx, stream, artifact.schema, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to setup the writer thread: %s", err)
		}

		if artifact.schema == nil {
			// First thread for this stream: persist schema and the olake_2pc state
			// so all subsequent threads (e.g. concurrent backfill chunks) can also
			// check which chunks were already committed.
			artifact.schema = output
			artifact.metadataState = prev
		}
		return artifact.metadataState, nil
	}()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup writer thread: %s", err)
	}

	return &LegacyWriterThread{
		buffer:         []types.RawRecord{},
		batchSize:      w.batchSize,
		threadID:       opts.ThreadID,
		writer:         writerImpl,
		stats:          w.stats,
		streamArtifact: artifact,
		group:          utils.NewCGroupWithLimit(ctx, 1), // currently only one thread (To make sure flush can run parallel when buffer filling)
	}, prevStreamState, nil
}

func (wt *LegacyWriterThread) Push(ctx context.Context, record types.RawRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wt.group.Ctx().Done():
		// if group context is done, return the group err (retry error as there can be rate limits on s3)
		return wt.group.Block()
	default:
		wt.stats.ReadCount.Add(1)
		wt.buffer = append(wt.buffer, record)
		if len(wt.buffer) >= int(wt.batchSize) {
			buf := make([]types.RawRecord, len(wt.buffer))
			copy(buf, wt.buffer)
			wt.buffer = wt.buffer[:0]
			wt.group.Add(func(ctx context.Context) error {
				return wt.flush(ctx, buf)
			})
		}
		return nil
	}
}

func (wt *LegacyWriterThread) flush(ctx context.Context, buf []types.RawRecord) error {
	if len(buf) == 0 {
		return nil
	}
	return runRecoveredFlush(func() error {
		flushCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		recordsCountBeforeFiltering := len(buf)
		evolution, buf, threadSchema, err := wt.writer.FlattenAndCleanData(flushCtx, buf)
		if err != nil {
			return fmt.Errorf("failed to flatten and clean data: %s", err)
		}
		wt.stats.RecordsFiltered.Add(int64(recordsCountBeforeFiltering - len(buf)))

		// TODO: after flattening record type raw_record not make sense
		if evolution {
			wt.streamArtifact.mu.Lock()
			newSchema, evErr := wt.writer.EvolveSchema(flushCtx, wt.streamArtifact.schema, threadSchema)
			if evErr == nil && newSchema != nil {
				wt.streamArtifact.schema = newSchema
			}
			wt.streamArtifact.mu.Unlock()
			if evErr != nil {
				return fmt.Errorf("failed to evolve schema: %s", evErr)
			}
		}

		if err := wt.writer.Write(flushCtx, buf); err != nil {
			return fmt.Errorf("failed to write records: %s", err)
		}
		logger.Infof("Thread[%s]: successfully wrote %d records", wt.threadID, len(buf))
		return nil
	})
}

func (wt *LegacyWriterThread) Close(ctx context.Context, finalMetadataState any) (err error) {
	select {
	case <-ctx.Done():
		if closeErr := wt.writer.Close(ctx, finalMetadataState); closeErr != nil {
			return fmt.Errorf("failed to close writer: %s", closeErr)
		}
		return nil
	default:
		defer wt.stats.ThreadCount.Add(-1)
		defer func() {
			wt.streamArtifact.mu.Lock()
			defer wt.streamArtifact.mu.Unlock()
			closeErr := wt.writer.Close(ctx, finalMetadataState)
			if closeErr != nil {
				err = utils.Ternary(err == nil, closeErr, fmt.Errorf("%s: flush error: %w", closeErr, err)).(error)
			}
		}()
		wt.group.Add(func(ctx context.Context) error {
			return wt.flush(ctx, wt.buffer)
		})
		if err := wt.group.Block(); err != nil {
			return fmt.Errorf("failed to flush data while closing: %s", err)
		}
		return nil
	}
}
