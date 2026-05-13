package legacydst

import (
	"context"
	"fmt"
	"sync"

	dstcore "github.com/datazip-inc/olake/destination/core"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// streamSchema is the per-stream artifact shared across all threads of a
// Pool. The mu fence guards both schema and metadataState.
type streamSchema struct {
	mu            sync.RWMutex
	schema        any
	metadataState *types.MetadataState
}

// Pool is the row-based pool implementation. It calls Writer.Write per batch
// of types.RawRecord.
type Pool struct {
	configMutex sync.Mutex
	stats       *dstcore.Stats
	config      any
	init        NewFunc
	streams     sync.Map // streamID -> *streamSchema
	batchSize   int64
}

// Thread is the per-thread row writer.
type Thread struct {
	stats          *dstcore.Stats
	buffer         []types.RawRecord
	threadID       string
	writer         Writer
	batchSize      int64
	streamArtifact *streamSchema
	group          *utils.CxGroup
}

// NewPool constructs a Pool for the destination type configured in cfg. The
// destination's init() must have registered itself in RegisteredWriters.
func NewPool(ctx context.Context, cfg *types.WriterConfig, syncStreams []string, batchSize int64) (*Pool, error) {
	newfunc, ok := RegisteredWriters[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("invalid destination type has been passed [%s]", cfg.Type)
	}

	adapter := newfunc()
	if err := utils.Unmarshal(cfg.WriterConfig, adapter.GetConfigRef()); err != nil {
		return nil, err
	}
	if err := adapter.Check(ctx); err != nil {
		return nil, fmt.Errorf("failed to test destination: %s", err)
	}

	pool := &Pool{
		stats:     dstcore.NewStats(),
		config:    cfg.WriterConfig,
		init:      newfunc,
		batchSize: batchSize,
	}
	for _, s := range syncStreams {
		pool.streams.Store(s, &streamSchema{})
	}
	return pool, nil
}

func (p *Pool) AddRecordsToSyncStats(n int64) { p.stats.TotalRecordsToSync.Add(n) }
func (p *Pool) GetStats() *dstcore.Stats      { return p.stats }

func (p *Pool) loadStreamArtifact(streamID string) (*streamSchema, error) {
	raw, ok := p.streams.Load(streamID)
	if !ok {
		return nil, fmt.Errorf("failed to get stream artifacts for stream[%s]", streamID)
	}
	s, ok := raw.(*streamSchema)
	if !ok {
		return nil, fmt.Errorf("failed to convert raw stream artifact[%T] to *streamSchema", raw)
	}
	return s, nil
}

// NewWriter constructs a new Thread for the given stream, unmarshalling
// the destination config into a fresh Writer instance.
func (p *Pool) NewWriter(ctx context.Context, stream types.StreamInterface, options ...dstcore.ThreadOptions) (dstcore.Thread, *types.MetadataState, error) {
	p.stats.ThreadCount.Add(1)
	opts := dstcore.ApplyThreadOpts(options)

	artifact, err := p.loadStreamArtifact(stream.ID())
	if err != nil {
		return nil, nil, err
	}

	var writerImpl Writer
	prevStreamState, err := func() (*types.MetadataState, error) {
		writerImpl = p.init()
		p.configMutex.Lock()
		err := utils.Unmarshal(p.config, writerImpl.GetConfigRef())
		p.configMutex.Unlock()
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
			artifact.schema = output
			artifact.metadataState = prev
		}
		return artifact.metadataState, nil
	}()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup writer thread: %s", err)
	}

	return &Thread{
		buffer:         []types.RawRecord{},
		batchSize:      p.batchSize,
		threadID:       opts.ThreadID,
		writer:         writerImpl,
		stats:          p.stats,
		streamArtifact: artifact,
		group:          utils.NewCGroupWithLimit(ctx, 1),
	}, prevStreamState, nil
}

// Push buffers a record and triggers an async flush when the batch is full.
func (t *Thread) Push(ctx context.Context, record types.RawRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.group.Ctx().Done():
		return t.group.Block()
	default:
		t.stats.ReadCount.Add(1)
		t.buffer = append(t.buffer, record)
		if len(t.buffer) >= int(t.batchSize) {
			buf := make([]types.RawRecord, len(t.buffer))
			copy(buf, t.buffer)
			t.buffer = t.buffer[:0]
			t.group.Add(func(ctx context.Context) error { return t.flush(ctx, buf) })
		}
		return nil
	}
}

func (t *Thread) flush(ctx context.Context, buf []types.RawRecord) error {
	if len(buf) == 0 {
		return nil
	}
	return dstcore.RunRecoveredFlush(func() error {
		flushCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		recordsBefore := len(buf)
		evolution, buf, threadSchema, err := t.writer.FlattenAndCleanData(flushCtx, buf)
		if err != nil {
			return fmt.Errorf("failed to flatten and clean data: %s", err)
		}
		t.stats.RecordsFiltered.Add(int64(recordsBefore - len(buf)))

		if evolution {
			t.streamArtifact.mu.Lock()
			newSchema, evErr := t.writer.EvolveSchema(flushCtx, t.streamArtifact.schema, threadSchema)
			if evErr == nil && newSchema != nil {
				t.streamArtifact.schema = newSchema
			}
			t.streamArtifact.mu.Unlock()
			if evErr != nil {
				return fmt.Errorf("failed to evolve schema: %s", evErr)
			}
		}

		if err := t.writer.Write(flushCtx, buf); err != nil {
			return fmt.Errorf("failed to write records: %s", err)
		}
		logger.Infof("Thread[%s]: successfully wrote %d records", t.threadID, len(buf))
		return nil
	})
}

// Close drains the buffer, flushes the final batch and closes the writer.
func (t *Thread) Close(ctx context.Context, finalMetadataState any) (err error) {
	select {
	case <-ctx.Done():
		if closeErr := t.writer.Close(ctx, finalMetadataState); closeErr != nil {
			return fmt.Errorf("failed to close writer: %s", closeErr)
		}
		return nil
	default:
		defer t.stats.ThreadCount.Add(-1)
		defer func() {
			t.streamArtifact.mu.Lock()
			defer t.streamArtifact.mu.Unlock()
			closeErr := t.writer.Close(ctx, finalMetadataState)
			if closeErr != nil {
				err = utils.Ternary(err == nil, closeErr, fmt.Errorf("%s: flush error: %w", closeErr, err)).(error)
			}
		}()
		t.group.Add(func(ctx context.Context) error { return t.flush(ctx, t.buffer) })
		if err := t.group.Block(); err != nil {
			return fmt.Errorf("failed to flush data while closing: %s", err)
		}
		return nil
	}
}

// ClearDestination instantiates a single Writer (no stream setup) and invokes
// DropStreams. It is the helper called by `olake clear`.
func ClearDestination(ctx context.Context, cfg *types.WriterConfig, dropStreams []types.StreamInterface) error {
	newfunc, ok := RegisteredWriters[cfg.Type]
	if !ok {
		return fmt.Errorf("invalid destination type has been passed [%s]", cfg.Type)
	}
	adapter := newfunc()
	if err := utils.Unmarshal(cfg.WriterConfig, adapter.GetConfigRef()); err != nil {
		return err
	}
	if dropStreams != nil {
		if err := adapter.DropStreams(ctx, dropStreams); err != nil {
			return fmt.Errorf("failed to drop the streams: %s", err)
		}
	}
	return nil
}
