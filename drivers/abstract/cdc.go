package abstract

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) RunChangeStream(ctx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	// run pre cdc of drivers
	if err := a.driver.PreCDC(ctx, streams); err != nil {
		return fmt.Errorf("failed in pre cdc run for driver[%s]: %s", a.driver.Type(), err)
	}

	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)
	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		isStrictCDC := stream.GetStream().SyncMode == types.STRICTCDC
		if a.state.HasCompletedBackfill(stream.Self()) || isStrictCDC {
			logger.Infof("backfill %s for stream[%s], skipping", utils.Ternary(isStrictCDC, "not enabled", "completed").(string), stream.ID())
			backfillWaitChannel <- stream.ID()
		} else {
			err := a.Backfill(ctx, backfillWaitChannel, pool, stream)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to run backfill: %s", err)
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

			// run parallel change stream
			// TODO: remove duplicate code
			if isParallelChangeStream(a.driver.Type()) {
				a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
					index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })

					// create cdc context, so that main context not affected if cdc retries
					cdcCtx, cdcCtxCancel := context.WithCancel(ctx)
					defer cdcCtxCancel()

					threadID := fmt.Sprintf("%s_%s", streams[index].ID(), utils.ULID())
					inserter, err := pool.NewWriter(cdcCtx, streams[index], destination.WithThreadID(threadID))
					if err != nil {
						return fmt.Errorf("failed to create new thread in pool, error: %s", err)
					}
					logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, streams[index].ID())
					defer func() {
						if threadErr := inserter.Close(cdcCtx); threadErr != nil {
							err = fmt.Errorf("failed to insert cdc record of stream %s, insert func error: %s, thread error: %s", streamID, err, threadErr)
						}

						// check for panics before saving state
						if r := recover(); r != nil {
							err = fmt.Errorf("panic recovered in cdc: %v, prev error: %s", r, err)
						}

						postCDCErr := a.driver.PostCDC(ctx, streams[index], err == nil)
						if postCDCErr != nil {
							err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %s", postCDCErr, err)
						}

						if err != nil {
							err = fmt.Errorf("thread[%s]: %s", threadID, err)
						}
					}()
					return utils.RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func(attempt int) error {
						if attempt > 0 {
							// close prev writer
							inserter.Close(cdcCtx)

							// create new cdc context
							cdcCtx, cdcCtxCancel = context.WithCancel(ctx)
							threadID = fmt.Sprintf("%s-retry-attempt-%d", threadID, attempt)

							// re-initialize inserter
							inserter, err = pool.NewWriter(cdcCtx, streams[index], destination.WithBackfill(true), destination.WithThreadID(threadID))
							if err != nil {
								return fmt.Errorf("failed to create new writer thread: %s", err)
							}
						}

						return a.driver.StreamChanges(cdcCtx, streams[index], func(ctx context.Context, change CDCChange) error {
							pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
							opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
							return inserter.Push(ctx, types.CreateRawRecord(
								utils.GetKeysHash(change.Data, pkFields...),
								change.Data,
								opType,
								&change.Timestamp,
							))
						})
					})
				})
			} else {
				a.state.SetGlobal(nil, streamID)
			}
		}
	}
	if isParallelChangeStream(a.driver.Type()) {
		// parallel change streams already processed
		return nil
	}
	// TODO: For a big table cdc (for all tables) will not start until backfill get finished, need to study alternate ways to do cdc sync
	a.GlobalConnGroup.Add(func(ctx context.Context) (err error) {
		// create cdc context, so that main context not affected if cdc retries
		cdcCtx, cdcCtxCancel := context.WithCancel(ctx)
		defer cdcCtxCancel()

		// Set up inserters for each stream
		inserters := make(map[types.StreamInterface]*destination.WriterThread)
		err = utils.ForEach(streams, func(stream types.StreamInterface) error {
			threadID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
			inserters[stream], err = pool.NewWriter(cdcCtx, stream, destination.WithThreadID(threadID))
			if err != nil {
				logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, stream.ID())
			}
			return err
		})
		if err != nil {
			return fmt.Errorf("failed to create writer thread: %s", err)
		}
		defer func() {
			for stream, insert := range inserters {
				if threadErr := insert.Close(cdcCtx); threadErr != nil {
					err = fmt.Errorf("failed to insert cdc record of stream %s, insert func error: %s, thread error: %s", stream.ID(), err, threadErr)
				}
			}

			// check for panics before saving state
			if r := recover(); r != nil {
				err = fmt.Errorf("panic recovered in cdc: %v, prev error: %s", r, err)
			}

			postCDCErr := a.driver.PostCDC(ctx, nil, err == nil)
			if postCDCErr != nil {
				err = fmt.Errorf("post cdc error: %s, cdc insert thread error: %s", postCDCErr, err)
			}
		}()
		return utils.RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func(attempt int) error {
			if attempt > 0 {
				// close all prev writers
				for _, insert := range inserters {
					insert.Close(cdcCtx)
				}

				// create new cdc context
				cdcCtx, cdcCtxCancel = context.WithCancel(ctx)

				// re-initialize all inserters
				for stream := range inserters {
					threadID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
					inserters[stream], err = pool.NewWriter(cdcCtx, stream, destination.WithThreadID(threadID))
					if err != nil {
						return fmt.Errorf("failed to create new writer thread: %s", err)
					}
				}
			}

			return a.driver.StreamChanges(cdcCtx, nil, func(ctx context.Context, change CDCChange) error {
				pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
				opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
				return inserters[change.Stream].Push(ctx, types.CreateRawRecord(
					utils.GetKeysHash(change.Data, pkFields...),
					change.Data,
					opType,
					&change.Timestamp,
				))
			})
		})
	})
	return nil
}

func isParallelChangeStream(driverType string) bool {
	return driverType == string(constants.MongoDB)
}
