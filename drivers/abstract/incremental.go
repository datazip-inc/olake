package abstract

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
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

		prevCursor := a.state.GetCursor(stream.Self(), cursorField)
		a.state.SetCursor(stream.Self(), cursorField, prevCursor)
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

	completed := 0
	for completed < len(streams) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			return nil
		case streamID, ok := <-backfillWaitChannel:
			if !ok {
				return fmt.Errorf("backfill channel closed unexpectedly")
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

				var count int
				var cursorVal any

				defer func() {
					writer.Close()
					if threadErr := <-errChan; threadErr != nil {
						logger.Errorf("Writer error for stream %s: %v", stream.ID(), threadErr)
					}
					if cursorVal != nil {
						a.state.SetCursor(stream.Self(), cursorField, cursorVal)
						logger.Infof("Cursor updated for stream[%s] = %v (records: %d)", stream.ID(), cursorVal, count)
					} else {
						logger.Infof("Incremental sync done for stream[%s] (records: %d, no cursor)", stream.ID(), count)
					}
				}()

				_ = RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
					innerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
					defer cancel()

					return a.driver.IncrementalChanges(innerCtx, stream, func(record map[string]any) error {
						if val, ok := record[cursorField]; ok {
							cursorVal = val
						}
						pk := stream.GetStream().SourceDefinedPrimaryKey.Array()
						id := utils.GetKeysHash(record, pk...)
						count++
						return writer.Insert(types.CreateRawRecord(id, record, "r", time.Now().UTC()))
					})
				})
				return nil
			})

		}
	}
	return nil
}
