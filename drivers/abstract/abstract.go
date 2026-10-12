package abstract

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
)

// codeWriterPanicRecovered names a panic recovered inside a writer thread, distinct from sync command.
const codeWriterPanicRecovered = "sync.writer_panic_recovered"

type CDCChange struct {
	Stream       types.StreamInterface
	Timestamp    time.Time
	Kind         string
	Data         map[string]any
	ExtraColumns map[string]any // Driver-specific CDC metadata (e.g., LSN, binlog position, resume token)
	Bytes        int64
}

func NewCDCChange(stream types.StreamInterface, timestamp time.Time, kind string, data, extraColumns map[string]any, sourceBytes int64) CDCChange {
	return CDCChange{
		Stream:       stream,
		Timestamp:    timestamp,
		Kind:         kind,
		Data:         data,
		ExtraColumns: extraColumns,
		Bytes:        sourceBytes,
	}
}

type AbstractDriver struct { //nolint:gosec,revive
	driver          DriverInterface
	state           *types.State
	GlobalConnGroup *utils.CxGroup
	GlobalCtxGroup  *utils.CxGroup
}

var DefaultColumns = map[string]types.DataType{
	constants.OlakeID:        types.String,
	constants.OlakeTimestamp: types.TimestampMicro,
	constants.OpType:         types.String,
	constants.CdcTimestamp:   types.TimestampMicro,
}

func NewAbstractDriver(ctx context.Context, driver DriverInterface) *AbstractDriver {
	return &AbstractDriver{
		driver:          driver,
		GlobalCtxGroup:  utils.NewCGroup(ctx),
		GlobalConnGroup: utils.NewCGroupWithLimit(ctx, constants.DefaultThreadCount), // default max connections
	}
}

func (a *AbstractDriver) SetupState(state *types.State) {
	a.state = state
	a.driver.SetupState(state)
}

func (a *AbstractDriver) GetConfigRef() Config {
	return a.driver.GetConfigRef()
}

func (a *AbstractDriver) Spec() any {
	return a.driver.Spec()
}

func (a *AbstractDriver) Type() string {
	return a.driver.Type()
}

func (a *AbstractDriver) Discover(ctx context.Context, maxDiscoverThreads int, skipSchema bool) ([]*types.Stream, error) {
	streams, err := a.driver.GetStreamNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream names: %w", err)
	}

	// During sync, skip ProduceSchema entirely streams.json already holds
	// the full schema from discover run. GetStreamNames still runs
	// above because S3 uses it to populate discoveredFiles (needed for chunking
	// and incremental sync). Returning nil signals classifyStreams to skip
	// source-side validation and trust the catalog directly.
	//
	// Pass --discover-schema to re-discover and validate.
	if skipSchema {
		return nil, nil
	}

	var streamMap sync.Map

	if sampler, ok := a.driver.(SampledSchemaProducer); ok {
		if err := a.produceSampledSchemas(ctx, sampler, maxDiscoverThreads, streams, &streamMap); err != nil {
			return nil, err
		}
	} else {
		// Set max connections for the ProduceSchema
		if maxDiscoverThreads > 0 {
			a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, maxDiscoverThreads)
		} else if a.driver.MaxConnections() > 0 {
			a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
		}

		utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, streams, a.driver.MaxRetries(), func(ctx context.Context, _ int, stream types.StreamID) error {
			streamSchema, err := a.driver.ProduceSchema(ctx, stream) // use conn group context which is discoverCtx
			if err != nil {
				return fmt.Errorf("%w: failed to produce schema for stream %s: %w", constants.ErrNonRetryable, stream, err)
			}
			streamMap.Store(streamSchema.ID(), streamSchema)
			return nil
		})

		if err := a.GlobalConnGroup.Block(); err != nil {
			return nil, fmt.Errorf("error occurred while waiting for connection group: %w", err)
		}
	}

	var finalStreams []*types.Stream
	streamMap.Range(func(_, value any) bool {
		convStream, _ := value.(*types.Stream)

		// add default columns
		for column, typ := range DefaultColumns {
			if column == constants.CdcTimestamp && !a.supportsCdcColumn() {
				continue
			}
			convStream.UpsertField(column, typ, true, true)
		}

		// priority to default sync mode (cdc -> incremental -> strict_cdc)
		if convStream.SupportedSyncModes.Exists(types.CDC) && a.driver.CDCSupported() {
			convStream.SyncMode = types.CDC
		} else if convStream.SupportedSyncModes.Exists(types.INCREMENTAL) {
			convStream.SyncMode = types.INCREMENTAL
			// Default cursor field: lexicographically smallest available field, for deterministic output.
			if convStream.AvailableCursorFields.Len() > 0 {
				convStream.CursorField = slices.Min(convStream.AvailableCursorFields.Array())
			}
		} else if convStream.SupportedSyncModes.Exists(types.STRICTCDC) {
			convStream.SyncMode = types.STRICTCDC
		} else {
			convStream.SyncMode = types.FULLREFRESH
		}

		// add default stream properties
		convStream.DefaultStreamProperties = &types.DefaultStreamProperties{
			Normalization: types.IsDriverRelational(a.driver.Type()),
			AppendMode:    types.IsDriverAppendOnly(a.driver.Type()),
			UpdateType:    types.UpdateTypeEquality,
		}

		finalStreams = append(finalStreams, convStream)
		return true
	})

	return finalStreams, nil
}

// produceSampledSchemas fills streamMap with each stream's schema, built from DiscoverSampleBuckets
// records one bucket at a time: every stream finishes a bucket before any stream starts the next, and
// a stream's schema from a bucket replaces its schema from the previous bucket. When the discover
// timeout hits after the first bucket, each stream keeps the last bucket it completed. A timeout during
// the first bucket fails discover: returning without the unfinished streams would drop them from a
// merged catalog (--streams), losing their selection.
func (a *AbstractDriver) produceSampledSchemas(ctx context.Context, sampler SampledSchemaProducer, maxDiscoverThreads int, streams []types.StreamID, streamMap *sync.Map) error {
	threads := constants.DefaultThreadCount
	if maxDiscoverThreads > 0 {
		threads = maxDiscoverThreads
	} else if a.driver.MaxConnections() > 0 {
		threads = a.driver.MaxConnections()
	}

	for bucket, limit := range DiscoverSampleBuckets {
		// the context can end between buckets, when no running sample is left to report it
		if bucket > 0 && ctx.Err() != nil {
			return stopAfterFirstBucket(ctx, limit, ctx.Err())
		}
		logger.Infof("discover: sampling up to %d records for %d streams", limit, len(streams))

		// a finished errgroup cannot be reused, so every bucket gets a new group
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, threads)
		utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, streams, a.driver.MaxRetries(), func(ctx context.Context, _ int, streamID types.StreamID) error {
			stream, err := sampler.ProduceSampledSchema(ctx, streamID, limit)
			// a sample that returns after the timeout may be cut short without an error (Kafka
			// reads a poll deadline as "caught up"), so it never replaces the previous bucket
			if err == nil && ctx.Err() != nil {
				err = ctx.Err()
			}
			if err != nil {
				return fmt.Errorf("%w: failed to produce schema for stream %s: %w", constants.ErrNonRetryable, streamID, err)
			}

			streamMap.Store(stream.ID(), stream) // replaces the previous bucket's schema
			return nil
		})

		err := a.GlobalConnGroup.Block()
		// the context can stop scheduling a bucket without failing any started sample, so Block
		// returns nil: a first bucket still has to cover every stream, a later one ended early
		if err == nil && ctx.Err() != nil {
			if bucket > 0 {
				err = ctx.Err()
			} else if unsampled := unsampledStreams(streams, streamMap); len(unsampled) > 0 {
				logger.Errorf("discover timeout reached before sampling streams: %v", unsampled)
				err = fmt.Errorf("%d streams were not sampled: %w", len(unsampled), ctx.Err())
			}
		}
		if err != nil {
			if bucket > 0 {
				return stopAfterFirstBucket(ctx, limit, err)
			}
			return fmt.Errorf("error occurred while waiting for connection group: %w", err)
		}
	}
	return nil
}

// stopAfterFirstBucket ends sampling after the first bucket. Every stream then has a schema, so the
// discover timeout keeps each stream's last completed bucket instead of failing discover; any other
// error, including a canceled parent context, still fails it.
func stopAfterFirstBucket(ctx context.Context, limit int, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		logger.Warnf("discover timeout reached before completing the %d-record bucket; streams keep their last completed bucket (increase --timeout for a larger sample)", limit)
		return nil
	}
	return fmt.Errorf("error occurred while waiting for connection group: %w", err)
}

// unsampledStreams returns the streams that have no schema in streamMap.
func unsampledStreams(streams []types.StreamID, streamMap *sync.Map) []string {
	var unsampled []string
	for _, streamID := range streams {
		if _, found := streamMap.Load(streamID.String()); !found {
			unsampled = append(unsampled, streamID.String())
		}
	}
	return unsampled
}

func (a *AbstractDriver) Setup(ctx context.Context) error {
	return a.driver.Setup(ctx)
}

func (a *AbstractDriver) ClearState(streams []types.StreamInterface) (*types.State, error) {
	if a.state == nil {
		return &types.State{}, nil
	}

	dropStreams := make(map[string]bool)
	for _, stream := range streams {
		dropStreams[stream.ID()] = true
	}

	// if global state exists (in case of relational sources)
	if a.state.Global != nil && a.state.Global.Streams != nil {
		for streamID := range dropStreams {
			a.state.Global.Streams.Remove(streamID)
		}
	}

	if len(a.state.Streams) > 0 {
		for _, streamState := range a.state.Streams {
			if dropStreams[fmt.Sprintf("%s.%s", streamState.Namespace, streamState.Stream)] {
				streamState.HoldsValue.Store(false)
				streamState.State = sync.Map{}
			}
		}
	}
	return a.state, nil
}

func (a *AbstractDriver) Read(ctx context.Context, pool *destination.WriterPool, backfillStreams, cdcStreams, incrementalStreams []types.StreamInterface) error {
	// set max read connections
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}

	// run cdc sync
	if len(cdcStreams) > 0 {
		if a.driver.CDCSupported() {
			if err := a.RunChangeStream(ctx, pool, cdcStreams...); err != nil {
				return fmt.Errorf("failed to run change stream: %w", err)
			}
		} else {
			return errs.Precondition(errs.CDCPreconditionFailed,
				fmt.Sprintf("%s.cdc_not_configured", a.driver.Type()),
				fmt.Errorf("%s cdc configuration not provided, use full refresh for all streams", a.driver.Type()))
		}
	}

	// run incremental sync
	if len(incrementalStreams) > 0 {
		if err := a.Incremental(ctx, pool, incrementalStreams...); err != nil {
			return fmt.Errorf("failed to run incremental sync: %w", err)
		}
	}

	// handle standard streams (full refresh)
	for _, stream := range backfillStreams {
		a.GlobalCtxGroup.Add(func(ctx context.Context) error {
			return a.Backfill(ctx, nil, pool, stream)
		})
	}

	// wait for all threads to finish
	if err := a.GlobalCtxGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for context groups: %w", err)
	}

	// wait for all threads to finish
	if err := a.GlobalConnGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for connections: %w", err)
	}
	return nil
}

// waitForBackfillCompletion waits for all backfill processes to complete and processes each completed stream
func (a *AbstractDriver) waitForBackfillCompletion(mainCtx context.Context, backfillWaitChannel chan string, streams []types.StreamInterface, processStream func(streamID string) error) error {
	backfilledStreams := make([]string, 0, len(streams))
	for len(backfilledStreams) < len(streams) {
		select {
		case <-mainCtx.Done():
			// if main context stuck in error
			return mainCtx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			// if global conn group stuck in error
			return constants.ErrGlobalContextGroup
		case streamID, ok := <-backfillWaitChannel:
			if !ok {
				return fmt.Errorf("backfill channel closed unexpectedly")
			}
			backfilledStreams = append(backfilledStreams, streamID)

			if processStream != nil {
				if err := processStream(streamID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// generateThreadID creates a unique thread ID for a stream
func generateThreadID(streamID, hash string) string {
	suffix := utils.Ternary(hash != "", hash, utils.ULID())
	return fmt.Sprintf("%s_%s", streamID, suffix)
}

// handleWriterCleanup is a helper that creates a defer function for common writer cleanup operations
// It handles writer close (single or multiple), panic recovery, and calls the provided postProcess function
// The err parameter should be a pointer to the error variable that will be returned from the function
// The cancel parameter is used to cancel the context when an error occurs, so other threads can detect the failure
// The writer parameter can be either:
//   - *destination.WriterThread for a single writer
//   - map[string]*destination.WriterThread for multiple writers keyed by stream ID
func handleWriterCleanup(ctx context.Context, cancel context.CancelFunc, err *error, writer any, threadID string, mtState *any, dedupInserts *bool) {
	if r := recover(); r != nil {
		// panic is classified as internal error
		*err = utils.Ternary(*err == nil,
			errs.Precondition(errs.InternalError, codeWriterPanicRecovered, fmt.Errorf("panic recovered: %v", r)),
			fmt.Errorf("%w: panic recovered: %v", *err, r)).(error)
	}

	if *err != nil {
		cancel()
	}

	var closeErr error

	closeWriter := func(w *destination.WriterThread, metadataValue any) error {
		var metadataState any
		var setErr error

		if metadataValue != nil {
			ms, err := types.SetMetadataState(metadataValue, threadID)
			if err != nil {
				setErr = fmt.Errorf("failed to set metadata state: %w", err)
				cancel()
			}
			types.SetDedupInserts(ms, dedupInserts)
			metadataState = ms
		}
		if threadErr := w.Close(ctx, metadataState); threadErr != nil {
			setErr = errors.Join(setErr, fmt.Errorf("failed to close writer: %w", threadErr))
		}

		return setErr
	}

	switch w := writer.(type) {
	case *destination.WriterThread:
		var mtStateValue any
		if mtState != nil {
			// Incremental stores cursor metadata as map[string]any; use *mtState directly (not per-stream lookup).
			mtStateValue = *mtState
		}
		closeErr = closeWriter(w, mtStateValue)
	case map[string]*destination.WriterThread:
		// Multiple writers keyed by stream ID
		for streamID, inserter := range w {
			if inserter != nil {
				var mtStateValue any
				if mtState != nil {
					if mtStateValueByStream, ok := (*mtState).(map[string]any); ok {
						mtStateValue = mtStateValueByStream[streamID]
					} else {
						mtStateValue = *mtState
					}
				}
				closeErr = errors.Join(closeErr, closeWriter(inserter, mtStateValue))
			}
		}
	default:
		closeErr = fmt.Errorf("unsupported writer type")
	}

	if closeErr != nil {
		*err = utils.Ternary(*err == nil, closeErr, fmt.Errorf("%s: prev error: %w", closeErr, *err)).(error)
	}
	if *err != nil {
		cancel()
	}

	if *err != nil && threadID != "" {
		*err = fmt.Errorf("thread[%s]: %w", threadID, *err)
	}
}

func (a *AbstractDriver) supportsCdcColumn() bool {
	if a.driver.CDCSupported() && a.driver.Type() != string(constants.Kafka) {
		// kafka driver does not support cdc column
		return true
	}
	return false
}
