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
	"github.com/datazip-inc/olake/utils/typeutils"
)

func (a *AbstractDriver) Incremental(ctx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)

	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		prevCursor := a.state.GetCursor(stream.Self(), stream.Cursor())
		if a.state.HasCompletedBackfill(stream.Self()) && prevCursor != nil {
			logger.Infof("Backfill skipped for stream[%s], already completed", stream.ID())
			backfillWaitChannel <- stream.ID()
			return nil
		}
		return a.Backfill(ctx, backfillWaitChannel, pool, stream)
	})
	if err != nil {
		return fmt.Errorf("backfill failed: %s", err)
	}

	// Wait for all backfill processes to complete
	backfilledStreams := make([]string, 0, len(streams))
	for len(backfilledStreams) < len(streams) {
		select {
		case <-ctx.Done():
			// if main context stuck in error
			return ctx.Err()
		case <-a.GlobalConnGroup.Ctx().Done():
			// if global conn group stuck in error
			return nil
		case streamID, ok := <-backfillWaitChannel:
			if !ok {
				return fmt.Errorf("backfill channel closed unexpectedly")
			}
			backfilledStreams = append(backfilledStreams, streamID)
			a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })
				stream := streams[index]
				cursorField := stream.Cursor()
				// TODO: make inremental state consistent save it as string and typecast while reading
				// get cursor column from state and typecast it to cursor column type for comparisons
				stateCursorValue := a.state.GetCursor(stream.Self(), cursorField)
				cursorColType, err := stream.Schema().GetType(cursorField)
				if err != nil {
					return fmt.Errorf("failed to get cursor column type: %s", err)
				}
				maxCursorValue, err := typeutils.ReformatValue(cursorColType, stateCursorValue)
				if err != nil {
					return fmt.Errorf("failed to reformat value of cursor received from state, col[%s] into type[%s]: %s", cursorField, cursorColType, err)
				}

				inserter, err := pool.NewWriter(ctx, stream)
				if err != nil {
					return fmt.Errorf("failed to create new writer thread in pool: %s", err)
				}
				defer func() {
					if threadErr := inserter.Close(ctx); threadErr != nil {
						err = fmt.Errorf("failed to insert incremental record of stream %s, insert func error: %s, thread error: %s", streamID, err, threadErr)
					}

					// check for panics before saving state
					if r := recover(); r != nil {
						err = fmt.Errorf("panic recovered in incremental sync: %v, prev error: %s", r, err)
					}

					// set state (no comparison)
					if err == nil {
						a.state.SetCursor(stream.Self(), cursorField, maxCursorValue)
					}
				}()
				return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
					return a.driver.StreamIncrementalChanges(ctx, stream, func(record map[string]any) error {
						cursorValue := record[cursorField]
						maxCursorValue = utils.Ternary(utils.CompareInterfaceValue(cursorValue, maxCursorValue) == 1, cursorValue, maxCursorValue)
						pk := stream.GetStream().SourceDefinedPrimaryKey.Array()
						id := utils.GetKeysHash(record, pk...)
						return inserter.Push(types.CreateRawRecord(id, record, "r", time.Unix(0, 0)))
					})
				})
			})
		}
	}
	return nil
}
