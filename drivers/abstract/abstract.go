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
	"github.com/datazip-inc/olake/utils/logger"
)

type CDCChange struct {
	Stream    types.StreamInterface
	Timestamp time.Time
	Kind      string
	Data      map[string]interface{}
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

func (a *AbstractDriver) Discover(ctx context.Context) ([]*types.Stream, error) {
	// set max connections
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}

	streams, err := a.driver.GetStreamNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream names: %s", err)
	}
	var streamMap sync.Map

	utils.ConcurrentInGroup(a.GlobalConnGroup, streams, func(ctx context.Context, _ int, stream string) error {
		streamSchema, err := a.driver.ProduceSchema(ctx, stream) // use conn group context which is discoverCtx
		if err != nil {
			return err
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
		if convStream.SupportedSyncModes.Len() == 0 {
			convStream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
		}
		convStream.SyncMode = utils.Ternary(convStream.SyncMode == "", types.FULLREFRESH, convStream.SyncMode).(types.SyncMode)

		// add default columns
		for column, typ := range DefaultColumns {
			convStream.UpsertField(column, typ, true)
		}

		if a.driver.CDCSupported() && a.driver.Type() != string(constants.Kafka) {
			convStream.WithSyncMode(types.CDC, types.STRICTCDC)
			convStream.SyncMode = types.CDC
		} else {
			// remove cdc column as it is not supported
			convStream.Schema.Properties.Delete(constants.CdcTimestamp)
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
		// if all global streams are dropped, no point for global state itself, making it null
		if len(a.state.Global.Streams.Array()) == 0 {
			a.state.Global.State = nil
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
	if a.driver.MaxConnections() > 0 {
		a.GlobalConnGroup = utils.NewCGroupWithLimit(ctx, a.driver.MaxConnections())
	}

	return utils.RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func(attempt int) error {
		if attempt > 0 {
			logger.Infof("Retrying Read operation (attempt %d)", attempt)
		}

		a.GlobalCtxGroup = utils.NewCGroup(ctx)

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
			stream := stream // capture for closure
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
	})
}

// generateThreadID creates a unique thread ID for a stream
func generateThreadID(streamID string) string {
	return fmt.Sprintf("%s_%s", streamID, utils.ULID())
}

// handleWriterCleanup is a helper that creates a defer function for common writer cleanup operations
// It handles writer close, panic recovery, and calls the provided postProcess function
// The err parameter should be a pointer to the error variable that will be returned from the function
// The cancel parameter is used to cancel the context when an error occurs, so other threads can detect the failure
func (a *AbstractDriver) handleWriterCleanup(ctx context.Context, cancel context.CancelFunc, err *error, writer *destination.WriterThread, threadID, panicMessage, closeMessage string, postProcess func(ctx context.Context, success bool) error) func() {
	return func() {
		if writer == nil || cancel == nil {
			*err = fmt.Errorf("%s: writer or cancel is nil, prev error: %w", constants.DestError, *err)
			return
		}
		// Cancel context if there's an error, so other threads using this context can detect the failure
		if *err != nil {
			cancel()
		}

		if threadErr := writer.Close(ctx); threadErr != nil {
			if closeMessage != "" {
				*err = fmt.Errorf("%s: %s, insert func error: %v", closeMessage, threadErr, *err)
			} else {
				*err = fmt.Errorf("failed to close writer: %s, prev error: %v", threadErr, *err)
			}
		}

		// check for panics before post-processing
		if r := recover(); r != nil {
			if panicMessage != "" {
				*err = fmt.Errorf("panic recovered in %s: %v, prev error: %v", panicMessage, r, *err)
			} else {
				*err = fmt.Errorf("panic recovered: %v, prev error: %v", r, *err)
			}
		}

		if postProcess != nil {
			postErr := postProcess(ctx, *err == nil)
			if postErr != nil {
				*err = fmt.Errorf("post process error: %s, prev error: %v", postErr, *err)
			}
		}

		if *err != nil && threadID != "" {
			*err = fmt.Errorf("thread[%s]: %s", threadID, *err)
		}
	}
}

// handleMultipleWritersCleanup is a helper that creates a defer function for cleaning up multiple writers
// It handles closing all writers, panic recovery, context cancellation on error, and calls the provided postProcess function
// The err parameter should be a pointer to the error variable that will be returned from the function
// The cancel parameter is used to cancel the context when an error occurs, so other threads can detect the failure
func (a *AbstractDriver) handleMultipleWritersCleanup(ctx context.Context, cancel context.CancelFunc, err *error, inserters interface{}, panicMessage string, postProcess func(ctx context.Context, success bool) error) func() {
	return func() {
		if cancel == nil {
			*err = fmt.Errorf("%s: cancel is nil, prev error: %w", constants.DestError, *err)
			return
		}
		// Cancel context if there's an error, so other threads using this context can detect the failure
		if *err != nil {
			cancel()
		}

		// Close all writers
		var closeErr error
		switch writers := inserters.(type) {
		case map[string]*destination.WriterThread:
			for streamID, inserter := range writers {
				if inserter != nil {
					if threadErr := inserter.Close(ctx); threadErr != nil {
						if closeErr == nil {
							closeErr = fmt.Errorf("failed closing writer[%s]: %s", streamID, threadErr)
						} else {
							closeErr = fmt.Errorf("%s; failed closing writer[%s]: %s", closeErr, streamID, threadErr)
						}
					}
				}
			}
		case map[types.StreamInterface]*destination.WriterThread:
			for stream, inserter := range writers {
				if inserter != nil {
					if threadErr := inserter.Close(ctx); threadErr != nil {
						if closeErr == nil {
							closeErr = fmt.Errorf("failed closing writer for stream %s: %s", stream.ID(), threadErr)
						} else {
							closeErr = fmt.Errorf("%s; failed closing writer for stream %s: %s", closeErr, stream.ID(), threadErr)
						}
					}
				}
			}
		default:
			closeErr = fmt.Errorf("unsupported inserters type")
		}

		if closeErr != nil {
			*err = fmt.Errorf("%s, prev error: %v", closeErr, *err)
		}

		// check for panics before post-processing
		if r := recover(); r != nil {
			if panicMessage != "" {
				*err = fmt.Errorf("panic recovered in %s: %v, prev error: %v", panicMessage, r, *err)
			} else {
				*err = fmt.Errorf("panic recovered: %v, prev error: %v", r, *err)
			}
		}

		if postProcess != nil {
			postErr := postProcess(ctx, *err == nil)
			if postErr != nil {
				*err = fmt.Errorf("post process error: %s, prev error: %v", postErr, *err)
			}
		}
	}
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
			return nil
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
