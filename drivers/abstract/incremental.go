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
	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)

	if err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		cursorField := stream.Cursor()
		if cursorField == "" {
			return fmt.Errorf("cursor field required for stream: %s", stream.ID())
		}

		// PRE: store last known cursor into in-memory cache
		prevCursor := a.state.GetCursor(stream.Self(), cursorField)
		a.driver.SetIncrementalCursor(stream.ID(), prevCursor)
		logger.Infof("Incremental: loaded cursor for stream[%s] = %v", stream.ID(), prevCursor)

		if prevCursor == nil {
			return a.Backfill(ctx, backfillWaitChannel, pool, stream)
		}

		logger.Infof("Backfill skipped for stream[%s], cursor already present", stream.ID())
		backfillWaitChannel <- stream.ID()
		return nil
	}); err != nil {
		return fmt.Errorf("backfill setup failed: %s", err)
	}

	// Step 2: Run Incremental fetch per-stream
	completed := 0
	for completed < len(streams) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			return nil
		case streamID := <-backfillWaitChannel:
			if val, ok := a.driver.GetIncrementalCursor(streamID); ok && val != nil {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool {
					return s.ID() == streamID
				})
				stream := streams[index]
				cursorField := stream.Cursor()
				a.state.SetCursor(stream.Self(), cursorField, val)
				logger.Infof("Final backfill cursor locked in for stream[%s] = %v", stream.ID(), val)
			}
			completed++
			a.GlobalConnGroup.Add(func(ctx context.Context) error {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool {
					return s.ID() == streamID
				})
				stream := streams[index]
				cursorField := stream.Cursor()

				errChan := make(chan error, 1)
				writer := pool.NewThread(ctx, stream, errChan)
				defer func() {
					writer.Close()
					if threadErr := <-errChan; threadErr != nil {
						logger.Errorf("Writer error for stream %s: %v", stream.ID(), threadErr)
					}
				}()

				count := 0
				err := a.driver.IncrementalChanges(ctx, stream, func(record map[string]any) error {
					val, ok := record[cursorField]
					if ok {
						a.driver.SetIncrementalCursor(stream.ID(), val)
					}
					pk := stream.GetStream().SourceDefinedPrimaryKey.Array()
					id := utils.GetKeysHash(record, pk...)
					count++
					return writer.Insert(types.CreateRawRecord(id, record, "r", time.Now().UTC()))
				})
				if err != nil {
					return fmt.Errorf("incremental fetch failed for stream %s: %w", stream.ID(), err)
				}

				val, ok := a.driver.GetIncrementalCursor(stream.ID())
				if ok && val != nil {
					a.state.SetCursor(stream.Self(), cursorField, val)
					logger.Infof("Cursor updated for stream[%s] = %v (records: %d)", stream.ID(), val, count)
				} else {
					logger.Infof("Incremental sync done for stream[%s] (records: %d, no cursor)", stream.ID(), count)
				}
				return nil
			})
		}
	}
	return nil
}
