package abstract

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type CDCChange struct {
	Stream       types.StreamInterface
	Timestamp    time.Time
	Kind         string
	Data         map[string]any
	ExtraColumns map[string]any // Driver-specific CDC metadata (e.g., LSN, binlog position, resume token)
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

func (a *AbstractDriver) Discover(ctx context.Context, maxDiscoverThreads int, isSync bool) ([]*types.Stream, error) {
	streams, err := a.driver.GetStreamNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream names: %s", err)
	}

	// During sync, skip ProduceSchema entirely streams.json already holds
	// the full schema from discover run. GetStreamNames still runs
	// above because S3 uses it to populate discoveredFiles (needed for chunking
	// and incremental sync). Returning nil signals classifyStreams to skip
	// source-side validation and trust the catalog directly.
	if isSync {
		return nil, nil
	}

	// Set max connections for the ProduceSchema
	if maxDiscoverThreads > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, maxDiscoverThreads)
	} else if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}

	var streamMap sync.Map

	utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, streams, a.driver.MaxRetries(), func(ctx context.Context, _ int, stream string) error {
		streamSchema, err := a.driver.ProduceSchema(ctx, stream) // use conn group context which is discoverCtx
		if err != nil {
			return fmt.Errorf("%w: failed to produce schema for stream %s: %s", constants.ErrNonRetryable, stream, err)
		}
		streamMap.Store(streamSchema.ID(), streamSchema)
		return nil
	})

	if err := a.GlobalConnGroup.Block(); err != nil {
		return nil, fmt.Errorf("error occurred while waiting for connection group: %s", err)
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
		} else if convStream.SupportedSyncModes.Exists(types.STRICTCDC) {
			convStream.SyncMode = types.STRICTCDC
		} else {
			convStream.SyncMode = types.FULLREFRESH
		}

		// add default stream properties
		convStream.DefaultStreamProperties = &types.DefaultStreamProperties{
			Normalization: types.IsDriverRelational(a.driver.Type()),
			AppendMode:    a.driver.Type() == string(constants.Kafka),
		}

		finalStreams = append(finalStreams, convStream)
		return true
	})

	return finalStreams, nil
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
				return fmt.Errorf("failed to run change stream: %s", err)
			}
		} else {
			return fmt.Errorf("%s cdc configuration not provided, use full refresh for all streams", a.driver.Type())
		}
	}

	// run incremental sync
	if len(incrementalStreams) > 0 {
		if err := a.Incremental(ctx, pool, incrementalStreams...); err != nil {
			return fmt.Errorf("failed to run incremental sync: %s", err)
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
		return fmt.Errorf("error occurred while waiting for context groups: %s", err)
	}

	// wait for all threads to finish
	if err := a.GlobalConnGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for connections: %s", err)
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
		*err = utils.Ternary(*err == nil, fmt.Errorf("panic recovered: %v", r), fmt.Errorf("%s: panic recovered: %v", *err, r)).(error)
	}

	if *err != nil {
		cancel()
	}

	var metadataState any
	var closeErr error
	if mtState != nil {
		ms, setErr := types.SetMetadataState(*mtState, threadID)
		if setErr != nil {
			closeErr = fmt.Errorf("failed to set metadata state: %s", setErr)
			cancel()
		}
		types.SetDedupInserts(ms, dedupInserts)
		metadataState = ms
	}

	switch w := writer.(type) {
	case *destination.WriterThread:
		if threadErr := w.Close(ctx, metadataState); threadErr != nil {
			closeErr = fmt.Errorf("failed to close writer: %s", threadErr)
		}
	case map[string]*destination.WriterThread:
		// Multiple writers keyed by stream ID
		for streamID, inserter := range w {
			if inserter != nil {
				if threadErr := inserter.Close(ctx, metadataState); threadErr != nil {
					closeErr = fmt.Errorf("%s; failed closing writer[%s]: %s", closeErr, streamID, threadErr)
				}
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
		*err = fmt.Errorf("thread[%s]: %s", threadID, *err)
	}
}

func (a *AbstractDriver) supportsCdcColumn() bool {
	if a.driver.CDCSupported() && a.driver.Type() != string(constants.Kafka) {
		// kafka driver does not support cdc column
		return true
	}
	return false
}
