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

	// sequential : all stream cdc sync will run sequentially
	// parallel : all stream cdc sync will run in parallel
	// concurrent : all stream cdc sync can run just after backfill is completed
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
		if concurrent {
			a.GlobalConnGroup.Add(a.driver.MaxRetries(), func(gCtx context.Context) error {
				index, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })
				return a.streamChanges(gCtx, pool, index)
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
		utils.ConcurrentInGroup(a.GlobalConnGroup, make([]int, a.driver.MaxConnections()), a.driver.MaxRetries(), func(ctx context.Context, index int, _ int) error {
			return a.streamChanges(ctx, pool, index)
		})
		return nil
	} else if sequential {
		// TODO: For a big table cdc (for all tables) will not start until backfill get finished, need to study alternate ways to do cdc sync
		a.GlobalConnGroup.Add(a.driver.MaxRetries(), func(gCtx context.Context) error {
			return a.streamChanges(gCtx, pool, 0)
		})
	}
	return nil
}

func (a *AbstractDriver) streamChanges(ctx context.Context, pool *destination.WriterPool, index int) (err error) {
	inserters := make(map[string]*destination.WriterThread)

	// create cdc context, so that main context not affected if cdc retries
	cdcCtx, cdcCtxCancel := context.WithCancel(ctx)
	defer cdcCtxCancel()

	defer handleWriterCleanup(cdcCtx, cdcCtxCancel, &err, inserters, "",
		func(ctx context.Context) error {
			postCDCErr := a.driver.PostCDC(ctx, index)
			if postCDCErr != nil {
				return fmt.Errorf("post cdc error: %s", postCDCErr)
			}
			return nil
		})()

	return a.driver.StreamChanges(cdcCtx, index, func(ctx context.Context, change CDCChange) error {
		inserter := inserters[change.Stream.ID()]
		if inserter == nil {
			// reader can read from multiple streams in kafka which we cant decide (r1 -> s1, s2, r2 -> s2, s3)
			// so creating writer for stream which read by reader in kafka
			threadID := fmt.Sprintf("%d_%s", index, change.Stream.ID())
			inserter, err = pool.NewWriter(ctx, change.Stream, destination.WithThreadID(threadID))
			if err != nil {
				return fmt.Errorf("failed to create writer for stream %s: %s", change.Stream.ID(), err)
			}
			inserters[change.Stream.ID()] = inserter
			logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, change.Stream.ID())
		}
		opType := utils.Ternary(change.Kind == "delete", "d", utils.Ternary(change.Kind == "update", "u", "c")).(string)
		return inserter.Push(ctx, types.CreateRawRecord(
			utils.GetKeysHash(change.Data, change.Stream.GetStream().SourceDefinedPrimaryKey.Array()...),
			change.Data,
			opType,
			&change.Timestamp,
		))
	})
}
