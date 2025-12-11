package abstract

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) RunChangeStream(mainCtx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	// run pre cdc of drivers
	if err := a.driver.PreCDC(mainCtx, streams); err != nil {
		return fmt.Errorf("failed in pre cdc run for driver[%s]: %s", a.driver.Type(), err)
	}

	sequential, parallel, concurrent := a.driver.ChangeStreamConfig()

	backfillWaitChannel := make(chan string, len(streams))
	defer close(backfillWaitChannel)
	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		isStrictCDC := stream.GetStream().SyncMode == types.STRICTCDC
		if a.state.HasCompletedBackfill(stream.Self()) || isStrictCDC {
			logger.Infof("backfill %s for stream[%s], skipping", utils.Ternary(isStrictCDC, "not enabled", "completed").(string), stream.ID())
			backfillWaitChannel <- stream.ID()
		} else {
			err := a.Backfill(mainCtx, backfillWaitChannel, pool, stream)
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
	err = a.waitForBackfillCompletion(mainCtx, backfillWaitChannel, streams, func(streamID string) error {
		// run parallel change stream
		// TODO: remove duplicate code
		if concurrent {
			a.GlobalConnGroup.Add(func(gCtx context.Context) (err error) {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })

				// create cdc context, so that main context not affected if cdc retries
				cdcCtx, cdcCtxCancel := context.WithCancel(gCtx)
				defer cdcCtxCancel()

				threadID := generateThreadID(streams[index].ID())
				inserter, err := pool.NewWriter(cdcCtx, streams[index], destination.WithThreadID(threadID))
				if err != nil {
					return fmt.Errorf("failed to create new thread in pool, error: %s", err)
				}

				logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, streams[index].ID())

				defer a.handleWriterCleanup(cdcCtx, cdcCtxCancel, &err, inserter, threadID,
					func(ctx context.Context) error {
						postCDCErr := a.driver.PostCDC(ctx, index)
						if postCDCErr != nil {
							return fmt.Errorf("post cdc error: %s", postCDCErr)
						}
						return nil
					})()

				return a.driver.StreamChanges(cdcCtx, index, func(ctx context.Context, change CDCChange) error {
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
		} else {
			a.state.SetGlobal(nil, streamID)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to process cdc streams: %s", err)
	}

	if parallel {
		// reset the global connection group
		a.GlobalConnGroup = utils.NewCGroupWithLimit(mainCtx, a.driver.MaxConnections())
		utils.ConcurrentInGroup(a.GlobalConnGroup, make([]int, a.driver.MaxConnections()), func(gCtx context.Context, index int, _ int) (err error) {
			inserters := make(map[string]*destination.WriterThread)

			// create cdc context, so that main context not affected if cdc retries
			cdcCtx, cdcCtxCancel := context.WithCancel(gCtx)
			defer cdcCtxCancel()

			defer a.handleWriterCleanup(cdcCtx, cdcCtxCancel, &err, inserters, "",
				func(ctx context.Context) error {
					postCDCErr := a.driver.PostCDC(ctx, index)
					if postCDCErr != nil {
						return fmt.Errorf("post cdc error: %s", postCDCErr)
					}
					return nil
				})()

			return a.driver.StreamChanges(cdcCtx, index, func(ctx context.Context, message CDCChange) error {
				inserter := inserters[message.Stream.ID()]
				if inserter == nil {
					// reader can read from multiple streams in kafka which we cant decide (r1 -> s1, s2, r2 -> s2, s3)
					// so creating writer for stream which read by reader in kafka
					threadID := fmt.Sprintf("%d_%s", index, message.Stream.ID())
					inserter, err = pool.NewWriter(ctx, message.Stream, destination.WithThreadID(threadID))
					if err != nil {
						return fmt.Errorf("failed to create writer for stream %s: %s", message.Stream.ID(), err)
					}
					inserters[message.Stream.ID()] = inserter
					logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, message.Stream.ID())
				}
				return inserter.Push(ctx, types.CreateRawRecord(
					utils.GetKeysHash(message.Data, message.Stream.GetStream().SourceDefinedPrimaryKey.Array()...),
					message.Data,
					"c", // kafka message is always a create operation
					&message.Timestamp,
				))
			})
		})
		return nil
	} else if sequential {
		// TODO: For a big table cdc (for all tables) will not start until backfill get finished, need to study alternate ways to do cdc sync
		a.GlobalConnGroup.Add(func(gCtx context.Context) (err error) {
			// create cdc context, so that main context not affected if cdc retries
			cdcCtx, cdcCtxCancel := context.WithCancel(gCtx)
			defer cdcCtxCancel()

			// Set up inserters for each stream
			inserters := make(map[string]*destination.WriterThread)
			err = utils.ForEach(streams, func(stream types.StreamInterface) error {
				threadID := generateThreadID(stream.ID())
				inserters[stream.ID()], err = pool.NewWriter(cdcCtx, stream, destination.WithThreadID(threadID))
				if err != nil {
					logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, stream.ID())
				}
				return err
			})
			if err != nil {
				return fmt.Errorf("failed to create writer thread: %s", err)
			}

			defer a.handleWriterCleanup(cdcCtx, cdcCtxCancel, &err, inserters, "",
				func(ctx context.Context) error {
					postCDCErr := a.driver.PostCDC(ctx, 0)
					if postCDCErr != nil {
						return fmt.Errorf("post cdc error: %s", postCDCErr)
					}
					return nil
				})()

			return a.driver.StreamChanges(cdcCtx, 0, func(ctx context.Context, change CDCChange) error {
				pkFields := change.Stream.GetStream().SourceDefinedPrimaryKey.Array()
				opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
				return inserters[change.Stream.ID()].Push(ctx, types.CreateRawRecord(
					utils.GetKeysHash(change.Data, pkFields...),
					change.Data,
					opType,
					&change.Timestamp,
				))
			})
		})
	}
	return nil
}
