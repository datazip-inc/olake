package abstract

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (a *AbstractDriver) Incremental(mainCtx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)

	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		primaryCursor, secondaryCursor := stream.Cursor()
		prevPrimaryCursor := a.state.GetCursor(stream.Self(), primaryCursor)
		prevSecondaryCursor := a.state.GetCursor(stream.Self(), secondaryCursor)

		// Check for potential recovery from previous run
		nextPrimaryCursorKey := fmt.Sprintf("olake_next_cursor_%s", primaryCursor)
		if nextPrimaryCursorVal := a.state.GetCursor(stream.Self(), nextPrimaryCursorKey); nextPrimaryCursorVal != nil {
			nextPrim, err := ReformatCursorValue(primaryCursor, nextPrimaryCursorVal, stream)
			if err == nil {
				var nextSec any
				if secondaryCursor != "" {
					nextSecKey := fmt.Sprintf("olake_next_cursor_%s", secondaryCursor)
					if val := a.state.GetCursor(stream.Self(), nextSecKey); val != nil {
						nextSec, _ = ReformatCursorValue(secondaryCursor, val, stream)
					}
				}

				startPrim, rErr := ReformatCursorValue(primaryCursor, prevPrimaryCursor, stream)
				var startSec any
				if rErr == nil && secondaryCursor != "" {
					startSec, rErr = ReformatCursorValue(secondaryCursor, prevSecondaryCursor, stream)
				}

				if rErr == nil {
					rawCtx := fmt.Sprintf("primaryCursor_%v_secondaryCursor_%v", startPrim, startSec)
					hash := sha256.Sum256([]byte(rawCtx))
					threadID := fmt.Sprintf("%s_%x", stream.ID(), hash)
					logger.Debugf("Recovering incremental state for stream %s with threadID %s", stream.ID(), threadID)

					inserter, err := pool.NewWriter(mainCtx, stream, destination.WithThreadID(threadID))
					if err == nil {
						committed, err := inserter.IsThreadCommitted(mainCtx, threadID)
						_ = inserter.Close(mainCtx)
						if err == nil && committed {
							logger.Infof("Recovering committed state for stream %s", stream.ID())

							a.commitIncrementalState(stream, primaryCursor, nextPrim, secondaryCursor, nextSec)

							prevPrimaryCursor = nextPrim
							if secondaryCursor != "" {
								prevSecondaryCursor = nextSec
							}
						}
					}
				}
			}
		}
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

			a.state.SetCursor(stream.Self(), primaryCursor, a.FormatCursorValue(maxPrimaryCursorValue))
			if maxPrimaryCursorValue == nil {
				logger.Warnf("max primary cursor value is nil for stream: %s", stream.ID())
			}
			if secondaryCursor != "" {
				a.state.SetCursor(stream.Self(), secondaryCursor, a.FormatCursorValue(maxSecondaryCursorValue))
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
	err = a.waitForBackfillCompletion(mainCtx, backfillWaitChannel, streams, func(streamID string) error {
		a.GlobalConnGroup.AddWithRetry(a.driver.MaxRetries(), func(gCtx context.Context) (err error) {
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

			rawCtx := fmt.Sprintf("primaryCursor_%v_secondaryCursor_%v", maxPrimaryCursorValue, maxSecondaryCursorValue)
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(rawCtx)))
			threadID := generateThreadID(stream.ID(), hash)
			inserter, err := pool.NewWriter(incrementalCtx, stream, destination.WithThreadID(threadID))
			if err != nil {
				return fmt.Errorf("failed to create new writer thread: %s", err)
			}

			logger.Infof("Thread[%s]: created incremental writer for stream %s", threadID, streams[index].ID())

			defer handleWriterCleanup(incrementalCtx, incrementalCtxCancel, &err, inserter, threadID,
				func(ctx context.Context) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					a.state.SetCursor(stream.Self(), fmt.Sprintf("olake_next_cursor_%s", primaryCursor), a.FormatCursorValue(maxPrimaryCursorValue))
					if secondaryCursor != "" {
						a.state.SetCursor(stream.Self(), fmt.Sprintf("olake_next_cursor_%s", secondaryCursor), a.FormatCursorValue(maxSecondaryCursorValue))
					}
					return nil
				},
				func(ctx context.Context) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					// Save cursor state on success
					a.commitIncrementalState(stream, primaryCursor, maxPrimaryCursorValue, secondaryCursor, maxSecondaryCursorValue)
					return nil
				}, nil)()

			// No retry logic here - retry happens at Read level
			return a.driver.StreamIncrementalChanges(incrementalCtx, stream, func(ctx context.Context, record map[string]any) error {
				maxPrimaryCursorValue, maxSecondaryCursorValue = a.getMaxIncrementCursorFromData(primaryCursor, secondaryCursor, maxPrimaryCursorValue, maxSecondaryCursorValue, record)
				return inserter.Push(ctx, types.CreateRawRecord(utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...), record, "u", nil))
			})
		})
		return nil
	})
	if err == constants.ErrGlobalContextGroup {
		// err will be captured in err group block statement
		return nil
	}

	return err
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

// FormatCursorValue is used to make time format and object id format consistent to be saved in state
func (a *AbstractDriver) FormatCursorValue(cursorValue any) any {
	switch v := cursorValue.(type) {
	case time.Time:
		// db2 timestamp does NOT store timezone information. Applying v.UTC() changes the actual time value for db2.
		if a.driver.Type() == string(constants.DB2) {
			return v.Format(constants.DB2StateTimestampFormat)
		}
		return v.UTC().Format(constants.DefaultStateTimestampFormat)
	case primitive.ObjectID:
		return v.Hex()
	default:
		return cursorValue
	}
}

func (a *AbstractDriver) commitIncrementalState(stream types.StreamInterface, primaryCursor string, primaryValue any, secondaryCursor string, secondaryValue any) {
	a.state.SetCursor(stream.Self(), primaryCursor, a.FormatCursorValue(primaryValue))
	if secondaryCursor != "" {
		a.state.SetCursor(stream.Self(), secondaryCursor, a.FormatCursorValue(secondaryValue))
		a.state.DeleteCursor(stream.Self(), fmt.Sprintf("olake_next_cursor_%s", secondaryCursor))
	}
	a.state.DeleteCursor(stream.Self(), fmt.Sprintf("olake_next_cursor_%s", primaryCursor))
}
