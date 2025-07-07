package abstract

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) RunIncrementalSync(ctx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {

	if err := a.driver.PreIncremental(ctx, streams...); err != nil {
		return fmt.Errorf("failed in pre incremental run for driver[%s]: %s", a.driver.Type(), err)
	}

	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)

	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		cursorField := stream.Cursor()
		if cursorField == "" {
			return fmt.Errorf("cursor field is required for incremental sync")
		}
		lastCursorValue := a.state.GetCursor(stream.Self(), cursorField)
		if lastCursorValue == nil {
			return a.Backfill(ctx, backfillWaitChannel, pool, stream)
		}
		logger.Infof("Cursor found for stream[%s], skipping backfill", stream.ID())
		backfillWaitChannel <- stream.ID()
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to run backfill for incremental: %s", err)
	}

	// Process each stream in parallel after backfill
	backfilledStreams := make([]string, 0, len(streams))
	for len(backfilledStreams) < len(streams) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			return nil
		case streamID := <-backfillWaitChannel:
			backfilledStreams = append(backfilledStreams, streamID)
			a.GlobalConnGroup.Add(func(ctx context.Context) error {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool {
					return s.ID() == streamID
				})
				return ExecuteIncremental(ctx, pool, streams[index], a.state, func(cb BackfillMsgFn) error {
					return a.driver.IncrementalChanges(ctx, streams[index], cb)
				}, func(success bool) error {
					return a.driver.PostIncremental(ctx, streams[index], success)
				})
			})
		}
	}
	return nil
}

func ExecuteIncremental(
	ctx context.Context,
	pool *destination.WriterPool,
	stream types.StreamInterface,
	state *types.State,
	fetchFn func(cb BackfillMsgFn) error,
	postFn func(success bool) error,
) (err error) {
	cursorField := stream.Cursor()
	if cursorField == "" {
		return fmt.Errorf("cursor field required for incremental sync")
	}

	errChan := make(chan error, 1)
	inserter := pool.NewThread(ctx, stream, errChan)
	defer func() {
		inserter.Close()
		if threadErr := <-errChan; threadErr != nil {
			err = fmt.Errorf("inserter thread error: %s", threadErr)
		}
		if postErr := postFn(err == nil); postErr != nil {
			err = fmt.Errorf("post incremental error: %s, sync error: %s", postErr, err)
		}
	}()

	var maxCursor any
	count := 0

	err = fetchFn(func(record map[string]any) error {
		cursorVal, exists := record[cursorField]
		if !exists {
			logger.Warnf("Cursor field %s missing in stream %s", cursorField, stream.ID())
			return nil
		}
		if maxCursor == nil || utils.CompareInterfaceValue(cursorVal, maxCursor) > 0 {
			maxCursor = cursorVal
		}

		pk := stream.GetStream().SourceDefinedPrimaryKey.Array()
		id := utils.GetKeysHash(record, pk...)

		count++
		return inserter.Insert(types.CreateRawRecord(id, record, "r", time.Now().UTC()))
	})

	// Set cursor state after successful incremental processing
	if maxCursor != nil {
		state.SetCursor(stream.Self(), cursorField, maxCursor)
		logger.Infof("Incremental for stream[%s] completed. Records read: %d, Records written: %d, Max cursor: %v",
			stream.ID(), count, pool.SyncedRecords(), maxCursor)
	} else {
		logger.Infof("Incremental for stream[%s] completed, records: %d (no cursor value found)", stream.ID(), count)
	}
	return nil
}
