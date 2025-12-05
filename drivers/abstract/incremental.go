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

func (a *AbstractDriver) Incremental(mainCtx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)

	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		primaryCursor, secondaryCursor := stream.Cursor()
		prevPrimaryCursor := a.state.GetCursor(stream.Self(), primaryCursor)
		prevSecondaryCursor := a.state.GetCursor(stream.Self(), secondaryCursor)
		if a.state.HasCompletedBackfill(stream.Self()) && (prevPrimaryCursor != nil && (secondaryCursor == "" || prevSecondaryCursor != nil)) {
			logger.Infof("Backfill skipped for stream[%s], already completed", stream.ID())
			backfillWaitChannel <- stream.ID()
			return nil
		} else if chunks := a.state.GetChunks(stream.Self()); chunks == nil || chunks.Len() == 0 {
			// This else if condition is added, so that the cursor reset and new cursor fetch is done only if there are no pending chunks
			// other wise it might cause loss of data if some chunks were already processed with old max cursor values

			// Reset only mentioned cursor state while preserving other state values
			a.state.ResetCursor(stream.Self())

			maxPrimaryCursorValue, maxSecondaryCursorValue, err := a.driver.FetchMaxCursorValues(mainCtx, stream)
			if err != nil {
				return fmt.Errorf("failed to fetch max cursor values: %s", err)
			}

			a.state.SetCursor(stream.Self(), primaryCursor, a.formatTimestampToUTC(maxPrimaryCursorValue))
			if maxPrimaryCursorValue == nil {
				logger.Warnf("max primary cursor value is nil for stream: %s", stream.ID())
			}
			if secondaryCursor != "" {
				a.state.SetCursor(stream.Self(), secondaryCursor, a.formatTimestampToUTC(maxSecondaryCursorValue))
				if maxSecondaryCursorValue == nil {
					logger.Warnf("max secondary cursor value is nil for stream: %s", stream.ID())
				}
			}
		}

		return a.Backfill(mainCtx, backfillWaitChannel, pool, stream)
	})
	if err != nil {
		return fmt.Errorf("backfill failed: %s", err)
	}

	// Wait for all backfill processes to complete
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
			a.GlobalConnGroup.Add(func(gCtx context.Context) (err error) {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })
				stream := streams[index]
				primaryCursor, secondaryCursor := stream.Cursor()
				// TODO: make inremental state consistent save it as string and typecast while reading
				// get cursor column from state and typecast it to cursor column type for comparisons
				maxPrimaryCursorValue, maxSecondaryCursorValue, err := a.getIncrementCursorFromState(primaryCursor, secondaryCursor, stream)
				if err != nil {
					return fmt.Errorf("failed to get incremental cursor value from state: %s", err)
				}

				// create incremental context, so that main context not affected if incremental retries
				incrementalCtx, incrementalCtxCancel := context.WithCancel(gCtx)
				defer incrementalCtxCancel()

				threadID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
				inserter, err := pool.NewWriter(incrementalCtx, stream, destination.WithThreadID(threadID))
				if err != nil {
					return fmt.Errorf("failed to create new writer thread: %s", err)
				}

				logger.Infof("Thread[%s]: created incremental writer for stream %s", threadID, streams[index].ID())

				defer func() {
					if threadErr := inserter.Close(incrementalCtx, err != nil); threadErr != nil {
						err = fmt.Errorf("failed to insert incremental record of stream %s, insert func error: %s, thread error: %s", streamID, err, threadErr)
					}

					// check for panics before saving state
					if r := recover(); r != nil {
						err = fmt.Errorf("panic recovered in incremental sync: %v, prev error: %s", r, err)
					}

					// set state (no comparison)
					if err == nil {
						a.state.SetCursor(stream.Self(), primaryCursor, a.formatTimestampToUTC(maxPrimaryCursorValue))
						a.state.SetCursor(stream.Self(), secondaryCursor, a.formatTimestampToUTC(maxSecondaryCursorValue))
					} else {
						err = fmt.Errorf("thread[%s]: %s", threadID, err)
					}
				}()

				return utils.RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func(attempt int) error {
					if attempt > 0 {
						// close prev writer
						_ = inserter.Close(incrementalCtx, true)

						// create new incremental context
						incrementalCtx, incrementalCtxCancel = context.WithCancel(gCtx)
						threadID = utils.Ternary(attempt == 1, fmt.Sprintf("%s-retry-attempt", threadID), threadID).(string)

						// re-initialize inserter
						inserter, err = pool.NewWriter(incrementalCtx, stream, destination.WithThreadID(threadID))
						if err != nil {
							return fmt.Errorf("failed to create new writer thread: %s", err)
						}
					}

					return a.driver.StreamIncrementalChanges(incrementalCtx, stream, func(ctx context.Context, record map[string]any) error {
						maxPrimaryCursorValue, maxSecondaryCursorValue = a.getMaxIncrementCursorFromData(primaryCursor, secondaryCursor, maxPrimaryCursorValue, maxSecondaryCursorValue, record)
						return inserter.Push(ctx, types.CreateRawRecord(utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...), record, "u", nil))
					})
				})
			})
		}
	}
	return nil
}

// RefomratCursorValue to parse the cursor value to the correct type
func ReformatCursorValue(cursorField string, cursorValue any, stream types.StreamInterface) (any, error) {
	if cursorField == "" {
		return cursorValue, nil
	}
	cursorColType, err := stream.Schema().GetType(cursorField)
	if err != nil {
		return nil, fmt.Errorf("failed to get cursor column type: %s", err)
	}
	return typeutils.ReformatValue(cursorColType, cursorValue)
}

// returns typecasted increment cursor
func (a *AbstractDriver) getIncrementCursorFromState(primaryCursorField string, secondaryCursorField string, stream types.StreamInterface) (any, any, error) {
	primaryStateCursorValue := a.state.GetCursor(stream.Self(), primaryCursorField)
	secondaryStateCursorValue := a.state.GetCursor(stream.Self(), secondaryCursorField)

	// typecast in case state was read from file
	primaryCursorValue, err := ReformatCursorValue(primaryCursorField, primaryStateCursorValue, stream)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to typecast primary cursor value: %s", err)
	}
	secondaryCursorValue, err := ReformatCursorValue(secondaryCursorField, secondaryStateCursorValue, stream)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to typecast secondary cursor value: %s", err)
	}
	return primaryCursorValue, secondaryCursorValue, nil
}

func (a *AbstractDriver) getMaxIncrementCursorFromData(primaryCursor, secondaryCursor string, maxPrimaryCursorValue, maxSecondaryCursorValue any, data map[string]any) (any, any) {
	primaryCursorValue := data[primaryCursor]
	primaryCursorValue = utils.Ternary(typeutils.Compare(primaryCursorValue, maxPrimaryCursorValue) == 1, primaryCursorValue, maxPrimaryCursorValue)

	var secondaryCursorValue any
	if secondaryCursor != "" {
		secondaryCursorValue = data[secondaryCursor]
		secondaryCursorValue = utils.Ternary(typeutils.Compare(secondaryCursorValue, maxSecondaryCursorValue) == 1, secondaryCursorValue, maxSecondaryCursorValue)
	}
	return primaryCursorValue, secondaryCursorValue
}

// formatTimestampToUTC is used to make time format consistent in state (Removing timezone info)
func (a *AbstractDriver) formatTimestampToUTC(cursorValue any) any {
	if _, ok := cursorValue.(time.Time); ok {
		return cursorValue.(time.Time).UTC().Format("2006-01-02T15:04:05.000000000Z")
	}
	return cursorValue
}
