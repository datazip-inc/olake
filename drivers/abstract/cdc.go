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

// RunChangeStream orchestrates the CDC sync process:
// 1. Pre-CDC: Initialize driver-specific CDC state
// 2. Backfill: Load historical data for streams that need it
// 3. CDC: Start change data capture based on execution mode:
//   - Sequential: Process streams one at a time after all backfills complete
//   - Parallel: Process all streams simultaneously after all backfills complete
//   - Concurrent: Start each stream's CDC immediately after its backfill completes (can overlap)
func (a *AbstractDriver) RunChangeStream(mainCtx context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	// run pre cdc of drivers
	if err := a.driver.PreCDC(mainCtx, streams); err != nil {
		return fmt.Errorf("failed in pre cdc run for driver[%s]: %s", a.driver.Type(), err)
	}

	isSequentialMode, isParallelMode, isConcurrentMode := a.driver.ChangeStreamConfig()

	// backfillCompletionChannel coordinates backfill completion:
	// - Streams with completed backfills or STRICTCDC mode signal immediately
	// - Other streams signal after their backfill completes
	// - waitForBackfillCompletion waits for all signals before starting CDC
	backfillCompletionChannel := make(chan string, len(streams))
	defer close(backfillCompletionChannel)
	err := utils.ForEach(streams, func(stream types.StreamInterface) error {
		isStrictCDC := stream.GetStream().SyncMode == types.STRICTCDC
		if a.state.HasCompletedBackfill(stream.Self()) || isStrictCDC {
			logger.Infof("backfill %s for stream[%s], skipping", utils.Ternary(isStrictCDC, "not enabled", "completed").(string), stream.ID())
			backfillCompletionChannel <- stream.ID()
		} else {
			err := a.Backfill(mainCtx, backfillCompletionChannel, pool, stream)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: failed to run backfill: %s", constants.ErrNonRetryable, err)
	}

	// Wait for all backfill processes to complete
	err = a.waitForBackfillCompletion(mainCtx, backfillCompletionChannel, streams, func(streamID string) error {
		// Start CDC stream immediately after backfill completes (concurrent mode)
		if isConcurrentMode {
			a.GlobalConnGroup.AddWithRetry(a.driver.MaxRetries(), func(connGroupCtx context.Context) error {
				streamIndex, _ := utils.ArrayContains(streams, func(s types.StreamInterface) bool { return s.ID() == streamID })
				return a.streamChanges(connGroupCtx, pool, streamIndex)
			})
		} else {
			// In sequential/parallel modes, track completion but don't start CDC yet
			// CDC will be started later based on the execution mode
			a.state.SetGlobal(nil, streamID)
		}
		return nil
	})
	if err != nil {
		if err == constants.ErrGlobalContextGroup {
			// err will be captured in err group block statement
			return nil
		}
		return fmt.Errorf("failed to process cdc streams: %s", err)
	}

	// TODO: cdc will not start until backfill get finished, need to study alternate ways (watermarking used by debezium) to do cdc sync parallelly to reduce backpressure on db

	if isParallelMode {
		// reset the global connection group
		a.GlobalConnGroup = utils.NewCGroupWithLimit(mainCtx, a.driver.MaxConnections())
		utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, make([]int, a.driver.MaxConnections()), a.driver.MaxRetries(), func(ctx context.Context, streamIndex int, _ int) error {
			return a.streamChanges(ctx, pool, streamIndex)
		})
		return nil
	} else if isSequentialMode {
		a.GlobalConnGroup.AddWithRetry(a.driver.MaxRetries(), func(connGroupCtx context.Context) error {
			return a.streamChanges(connGroupCtx, pool, 0)
		})
	}
	return nil
}

// streamChanges processes CDC changes for a stream identified by streamIndex.
// The streamIndex is passed to the driver to identify which stream to monitor.
// Note: The meaning of streamIndex varies by driver implementation:
//   - For MongoDB: index into the streams array
//   - For Kafka: reader ID
//   - For Postgres: ignored (uses global replication slot)
func (a *AbstractDriver) streamChanges(mainCtx context.Context, pool *destination.WriterPool, streamIndex int) (err error) {
	writers := make(map[string]*destination.WriterThread)

	// create cdc context, so that main context not affected if cdc retries
	cdcCtx, cdcCtxCancel := context.WithCancel(mainCtx)
	defer cdcCtxCancel()

	defer handleWriterCleanup(cdcCtx, cdcCtxCancel, &err, writers, "",
		func(ctx context.Context) error {
			postCDCErr := a.driver.PostCDC(ctx, streamIndex)
			if postCDCErr != nil {
				return fmt.Errorf("post cdc error: %s", postCDCErr)
			}
			return nil
		})()

	return a.driver.StreamChanges(cdcCtx, streamIndex, func(ctx context.Context, change CDCChange) error {
		writer := writers[change.Stream.ID()]
		if writer == nil {
			threadID := generateThreadID(change.Stream.ID(), "")
			writer, err = pool.NewWriter(ctx, change.Stream, destination.WithThreadID(threadID), destination.WithDriverType(constants.DriverType(a.driver.Type())))
			if err != nil {
				return fmt.Errorf("failed to create writer for stream %s: %s", change.Stream.ID(), err)
			}
			writers[change.Stream.ID()] = writer
			logger.Infof("Thread[%s]: created cdc writer for stream %s", threadID, change.Stream.ID())
		}
		return writer.Push(ctx, types.CreateRawRecord(
			utils.GetKeysHash(change.Data, change.Stream.GetStream().SourceDefinedPrimaryKey.Array()...),
			change.Data,
			mapChangeKindToOperationType(change.Kind),
			&change.Timestamp,
		))
	})
}

// mapChangeKindToOperationType converts CDC change kind to operation type code.
// "delete" -> "d", "update" -> "u", "insert"/"create" -> "c"
func mapChangeKindToOperationType(kind string) string {
	switch kind {
	case "delete":
		return "d"
	case "update":
		return "u"
	default: // "insert", "create", etc.
		return "c"
	}
}
