package abstract

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) RunIncrementalSync(ctx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)

	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		// Check if we have cursor state for this stream
		cursorField := stream.Cursor()
		if cursorField == "" {
			return fmt.Errorf("cursor field is required for incremental sync")
		}

		lastCursorValue := a.state.GetCursor(stream.Self(), cursorField)
		if lastCursorValue == nil {
			// No cursor state exists, do backfill first
			logger.Infof("No cursor state found for stream[%s], performing initial backfill", stream.ID())
			err := a.Backfill(ctx, backfillWaitChannel, pool, stream)
			if err != nil {
				return err
			}
		} else {
			// Cursor state exists, skip backfill
			logger.Infof("Cursor state found for stream[%s], skipping backfill", stream.ID())
			backfillWaitChannel <- stream.ID()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to run backfill for incremental streams: %s", err)
	}

	// Wait for all backfill processes to complete
	backfilledStreams := make([]string, 0, len(streams))
	for len(backfilledStreams) < len(streams) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			return nil
		case streamID, ok := <-backfillWaitChannel:
			if !ok {
				return fmt.Errorf("backfill channel closed unexpectedly")
			}
			backfilledStreams = append(backfilledStreams, streamID)

			// Start incremental sync for this stream
			a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })
				return a.driver.IncrementalSync(ctx, pool, streams[index])
			})
		}
	}

	return nil
}
