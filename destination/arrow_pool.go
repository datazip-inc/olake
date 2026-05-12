package destination

import (
	"context"
	"fmt"
	"sync"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	arrowdst "github.com/datazip-inc/olake/destination/arrow"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

// ---------------------------------------------------------------------------
// Per-stream artifact shared by all threads of an arrowPool.
// ---------------------------------------------------------------------------

type arrowStreamArtifact struct {
	mu          sync.RWMutex
	schema      arrowdst.OLakeSchema
	shape       arrowdst.SchemaShape
	arrowSchema *arrowlib.Schema
	state       *types.MetadataState
	preShape    []arrowdst.PartitionPreShape
}

// ---------------------------------------------------------------------------
// arrowPool — the Pool implementation for the arrow write path.
// ---------------------------------------------------------------------------

type arrowPool struct {
	configMutex  sync.Mutex
	stats        *Stats
	cfg          any
	adapterInit  arrowdst.AdapterInit
	writerSchema sync.Map // streamID -> *arrowStreamArtifact
	batchSize    int64
}

// newArrowWriterPool looks up the registered DestinationAdapter for cfg.Type
// (populated by each adapter package's init() via side-imports in connector.go)
// and returns a fully wired arrowPool.
func newArrowWriterPool(_ context.Context, cfg *types.WriterConfig, syncStreams []string, batchSize int64) (Pool, error) {
	adapterInit, ok := arrowdst.RegisteredAdapters[string(cfg.Type)]
	if !ok {
		return nil, fmt.Errorf("arrow writer requested but no arrow adapter registered for destination type [%s]; "+
			"ensure the adapter package is imported and its init() has run", cfg.Type)
	}
	pool := &arrowPool{
		stats:       newStats(),
		cfg:         cfg.WriterConfig,
		adapterInit: adapterInit,
		batchSize:   batchSize,
	}
	for _, s := range syncStreams {
		pool.writerSchema.Store(s, &arrowStreamArtifact{})
	}
	return pool, nil
}

func (p *arrowPool) AddRecordsToSyncStats(n int64) { p.stats.TotalRecordsToSync.Add(n) }
func (p *arrowPool) GetStats() *Stats               { return p.stats }

// ---------------------------------------------------------------------------
// arrowThread — the Thread implementation for the arrow write path.
// ---------------------------------------------------------------------------

type arrowThread struct {
	adapter        arrowdst.DestinationAdapter
	stream         types.StreamInterface
	applyFilter    bool
	upsertMode     bool
	streamArtifact *arrowStreamArtifact
	schema         arrowdst.OLakeSchema // thread-local clone, refreshed under the schema fence
	shape          arrowdst.SchemaShape
	arrowSchema    *arrowlib.Schema
	preShape       []arrowdst.PartitionPreShape
	buffer         []types.RawRecord
	batchSize      int64
	group          *utils.CxGroup
	stats          *Stats
	threadID       string
}

// NewWriter creates a new arrowThread for the given stream.
func (p *arrowPool) NewWriter(ctx context.Context, stream types.StreamInterface,
	options ...ThreadOptions,
) (Thread, *types.MetadataState, error) {
	p.stats.ThreadCount.Add(1)

	src := ApplyThreadOpts(options)
	upsertMode := !src.Backfill && !stream.Self().StreamMetadata.AppendMode

	rawArtifact, ok := p.writerSchema.Load(stream.ID())
	if !ok {
		return nil, nil, fmt.Errorf("no stream artifact for stream[%s]; was it registered?", stream.ID())
	}
	artifact := rawArtifact.(*arrowStreamArtifact)

	adapter := p.adapterInit()

	// If the adapter exposes GetConfigRef, unmarshal the raw writer config into it.
	p.configMutex.Lock()
	if cfgGetter, ok := adapter.(interface{ GetConfigRef() any }); ok {
		_ = utils.Unmarshal(p.cfg, cfgGetter.GetConfigRef())
	}
	p.configMutex.Unlock()

	artifact.mu.Lock()
	defer artifact.mu.Unlock()

	setup, err := adapter.Setup(ctx, stream, upsertMode, artifact.schema)
	if err != nil {
		return nil, nil, fmt.Errorf("adapter Setup: %s", err)
	}

	// First thread for this stream populates the shared artifact.
	if artifact.schema == nil {
		artifact.schema = setup.Schema
		artifact.shape = setup.Shape
		artifact.state = setup.State
		artifact.preShape = setup.PartitionFields
		artifact.arrowSchema = arrowdst.ToArrowSchema(setup.Schema, setup.Shape.FieldIDs, setup.Shape.IdentifierField)
	}

	return &arrowThread{
		adapter:        adapter,
		stream:         stream,
		applyFilter:    src.ApplyFilter,
		upsertMode:     upsertMode,
		streamArtifact: artifact,
		schema:         arrowdst.CloneSchema(artifact.schema),
		shape:          artifact.shape,
		arrowSchema:    artifact.arrowSchema,
		preShape:       artifact.preShape,
		buffer:         make([]types.RawRecord, 0),
		batchSize:      p.batchSize,
		group:          utils.NewCGroupWithLimit(ctx, 1),
		stats:          p.stats,
		threadID:       src.ThreadID,
	}, artifact.state, nil
}

// Push buffers a record and triggers an async flush when the batch is full.
func (t *arrowThread) Push(ctx context.Context, record types.RawRecord) error {
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
			t.group.Add(func(ctx context.Context) error { return t.flushArrow(ctx, buf) })
		}
		return nil
	}
}

func (t *arrowThread) flushArrow(ctx context.Context, buf []types.RawRecord) (err error) {
	if len(buf) == 0 {
		return nil
	}
	return runRecoveredFlush(func() error {
		fctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// 1) Destination-neutral compute: flatten, detect schema, filter.
		before := len(buf)
		diff, batchSchema, kept, ferr := arrowdst.FlattenAndDetect(fctx, t.stream, t.applyFilter, t.schema, t.preShape, buf)
		if ferr != nil {
			return fmt.Errorf("flatten: %s", ferr)
		}
		t.stats.RecordsFiltered.Add(int64(before - len(kept)))

		// 2) Schema evolution fence.
		if diff {
			t.streamArtifact.mu.Lock()
			merged, changed := arrowdst.MergeSchemas(t.streamArtifact.schema, batchSchema)
			if changed {
				newShape, evErr := t.adapter.OnSchemaEvolved(fctx, merged)
				if evErr != nil {
					t.streamArtifact.mu.Unlock()
					return fmt.Errorf("evolve schema: %s", evErr)
				}
				t.streamArtifact.schema = merged
				t.streamArtifact.shape = newShape
				t.streamArtifact.arrowSchema = arrowdst.ToArrowSchema(merged, newShape.FieldIDs, newShape.IdentifierField)
			}
			t.schema = arrowdst.CloneSchema(t.streamArtifact.schema)
			t.shape = t.streamArtifact.shape
			t.arrowSchema = t.streamArtifact.arrowSchema
			t.streamArtifact.mu.Unlock()
		}

		// 3) Hand off to the destination adapter.
		if err := t.adapter.WriteBatch(fctx, kept, t.arrowSchema); err != nil {
			return fmt.Errorf("adapter WriteBatch: %s", err)
		}
		logger.Infof("arrowThread[%s]: wrote %d records", t.threadID, len(kept))
		return nil
	})
}

// Close flushes the remaining buffer then delegates to the adapter.
func (t *arrowThread) Close(ctx context.Context, finalMetadataState any) (err error) {
	defer t.stats.ThreadCount.Add(-1)
	defer func() {
		t.streamArtifact.mu.Lock()
		defer t.streamArtifact.mu.Unlock()
		cerr := t.adapter.Close(ctx, finalMetadataState)
		if cerr != nil {
			err = utils.Ternary(err == nil, cerr, fmt.Errorf("%s; prior error: %w", cerr, err)).(error)
		}
	}()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	t.group.Add(func(ctx context.Context) error { return t.flushArrow(ctx, t.buffer) })
	if err := t.group.Block(); err != nil {
		return fmt.Errorf("final flush: %s", err)
	}
	return nil
}

