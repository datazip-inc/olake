package abstract

import (
	"context"
	"crypto/sha256"
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
	// This needs to run before PreCDC so that if all streams are committed,
	// we can update current LSN from next_cdc_pos before driver initializes CDC
	isRecoveryMode, processingSet, err := a.PrepareRecovery(mainCtx, pool, streams)
	if err != nil {
		return fmt.Errorf("failed to prepare recovery for driver[%s]: %s", a.driver.Type(), err)
	}
	// Store for streamChanges to use
	a.recoveryMode = isRecoveryMode
	a.recoveryProcessingSet = processingSet

	// run pre cdc of drivers (initializes driver state, connections, etc.)
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
	err = utils.ForEach(streams, func(stream types.StreamInterface) error {
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
//
// Recovery Mode: If recoveryMode=true, only streams in recoveryProcessingSet are synced (up to target position set by PrepareRecovery).
func (a *AbstractDriver) streamChanges(mainCtx context.Context, pool *destination.WriterPool, streamIndex int) (err error) {
	writers := make(map[string]*destination.WriterThread)

	// Get starting position once at the beginning for consistent thread IDs
	startingPosition := a.driver.GetCDCStartPosition()

	// Use recovery state prepared by PrepareRecovery
	isRecoveryMode := a.recoveryMode
	processingSet := a.recoveryProcessingSet

	// create cdc context, so that main context not affected if cdc retries
	cdcCtx, cdcCtxCancel := context.WithCancel(mainCtx)
	defer cdcCtxCancel()

	defer handleWriterCleanup(cdcCtx, cdcCtxCancel, &err, writers, "",
		func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// In recovery mode, don't update global next_cdc_pos and processing_streams
			// We use the existing state from previous run until recovery completes
			if isRecoveryMode {
				return nil
			}

			// Saving next cdc position for mongodb 2pc
			for streamID := range writers {
				a.driver.SaveNextCDCPositionForStream(streamID)
			}

			// Normal mode: Save the final CDC position and processing streams to state
			// This is for 2PC recovery - if writers fail, we know where to restart from
			streamIDs := make([]string, 0, len(writers))
			for streamID := range writers {
				streamIDs = append(streamIDs, streamID)
			}

			// Save the final CDC position and processing streams to state
			// To identify the next position to start from in case of failure
			cdcPosition := a.driver.GetCDCPosition()
			a.driver.SetNextCDCPosition(cdcPosition)
			// Stores the yet to be committed streams to state
			a.driver.SetProcessingStreams(streamIDs)
			return nil
		},
		func(ctx context.Context) error {
			postCDCErr := a.driver.PostCDC(ctx, streamIndex)
			if postCDCErr != nil {
				return fmt.Errorf("post cdc error: %s", postCDCErr)
			}
			return nil
		},
		func(streamID string) {
			// After successful commit: update _data from next_data (MongoDB 2PC)
			a.driver.CommitCDCPositionForStream(streamID)
			// Remove stream from processing after successful commit (Postgres and MySQL 2PC)
			a.driver.RemoveProcessingStream(streamID)
		})()

	return a.driver.StreamChanges(cdcCtx, streamIndex, func(ctx context.Context, change CDCChange) error {
		streamID := change.Stream.ID()

		// In recovery mode, only sync streams that are in the processing array
		if isRecoveryMode && !processingSet[streamID] {
			return nil // Skip this stream - not in processing array
		}

		writer := writers[streamID]
		if writer == nil {
			logger.Infof("Creating new writer thread for stream index: %d, stream ID: %s, position: %s", streamIndex, streamID, change.Position)
			// Use startingPosition (global for MySQL/Postgres) or change.Position (per-stream for MongoDB)
			positionForHash := startingPosition
			if positionForHash == "" {
				positionForHash = change.Position // MongoDB per-stream position
			}
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(positionForHash)))
			threadID := fmt.Sprintf("%s_%s", streamID, hash)

			logger.Debugf("Thread[%s]: creating CDC writer for stream %s", threadID, streamID)

			writer, err = pool.NewWriter(ctx, change.Stream, destination.WithThreadID(threadID))
			if err != nil {
				return fmt.Errorf("failed to create writer for stream %s: %s", streamID, err)
			}

			writers[streamID] = writer
			logger.Infof("Thread[%s]: created CDC writer for stream %s", threadID, streamID)
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

// PrepareRecovery checks processing streams commit status and prepares recovery state.
// Returns: isRecoveryMode (true if uncommitted streams exist), processingSet (map for O(1) lookup)
// If all streams are committed, updates current position from next_cdc_pos and clears processing array from state.
func (a *AbstractDriver) PrepareRecovery(ctx context.Context, pool *destination.WriterPool, streams []types.StreamInterface) (bool, map[string]bool, error) {
	// First, check per-stream recovery (MongoDB next_data pattern)
	// This handles streams that have next_data but may/may not be committed
	for _, stream := range streams {
		if err := a.driver.CheckPerStreamRecovery(ctx, pool, stream); err != nil {
			return false, nil, fmt.Errorf("failed per-stream recovery check for %s: %s", stream.ID(), err)
		}
	}

	// Now check global processing streams (MySQL/Postgres pattern)
	processingStreams := a.driver.GetProcessingStreams()
	if len(processingStreams) == 0 {
		// No processing streams, but check if next_cdc_pos exists
		// This means all streams were committed but state wasn't fully cleaned
		nextPos := a.driver.GetNextCDCPosition()
		if nextPos != "" {
			// Acknowledge the position to source (for Postgres LSN acknowledgment)
			if err := a.driver.AcknowledgeCDCPosition(ctx, nextPos); err != nil {
				logger.Warnf("Failed to acknowledge CDC position during recovery: %s", err)
			}
			// Update current position from next_cdc_pos and clear it
			a.driver.SetCurrentCDCPosition(nextPos)
			a.driver.SetNextCDCPosition("")
			logger.Infof("Recovery complete: updated current CDC position to %s", nextPos)
		}
		return false, nil, nil // No recovery needed
	}

	startingPosition := a.driver.GetCDCStartPosition()
	if startingPosition == "" {
		logger.Warnf("Cannot prepare recovery: no starting position available")
		return false, nil, nil
	}

	logger.Infof("Preparing recovery for %d processing streams", len(processingStreams))

	// Build stream lookup map
	streamMap := make(map[string]types.StreamInterface)
	for _, stream := range streams {
		streamMap[stream.ID()] = stream
	}

	// Check each processing stream's commit status
	for _, streamID := range processingStreams {
		stream, ok := streamMap[streamID]
		if !ok {
			logger.Warnf("Processing stream %s not found in streams list, removing", streamID)
			a.driver.RemoveProcessingStream(streamID)
			continue
		}

		// Generate same thread ID as we would during CDC
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(startingPosition)))
		threadID := fmt.Sprintf("%s_%s", streamID, hash)

		// Create a temporary writer to check commit status
		writer, err := pool.NewWriter(ctx, stream, destination.WithThreadID(threadID))
		if err != nil {
			return false, nil, fmt.Errorf("failed to create writer for recovery check: %s", err)
		}

		committed, err := writer.IsThreadCommitted(ctx, threadID)
		if err != nil {
			return false, nil, fmt.Errorf("failed to check commit status for stream %s: %s", streamID, err)
		}

		if committed {
			logger.Infof("Stream %s (thread %s) already committed, removing from processing", streamID, threadID)
			a.driver.RemoveProcessingStream(streamID)
		}
	}

	// Check if all streams were committed
	remainingProcessing := a.driver.GetProcessingStreams()
	if len(remainingProcessing) == 0 {
		// All committed - acknowledge and update current position from next_cdc_pos
		nextPos := a.driver.GetNextCDCPosition()
		if nextPos != "" {
			// Acknowledge the position to source FIRST (for Postgres LSN acknowledgment)
			if err := a.driver.AcknowledgeCDCPosition(ctx, nextPos); err != nil {
				logger.Warnf("Failed to acknowledge CDC position during recovery: %s", err)
			}
			a.driver.SetCurrentCDCPosition(nextPos)
			logger.Infof("All streams committed, updated current CDC position to: %s", nextPos)
		}
		a.driver.SetNextCDCPosition("")    // Clear next position
		a.driver.SetProcessingStreams(nil) // Clear processing array
		logger.Info("Recovery complete, all streams committed")
		return false, nil, nil // No recovery mode needed
	}

	// Build processingSet for O(1) lookup in streamChanges
	processingSet := make(map[string]bool)
	for _, streamID := range remainingProcessing {
		processingSet[streamID] = true
	}

	// Set target position for bounded sync
	nextPos := a.driver.GetNextCDCPosition()
	if nextPos != "" {
		logger.Infof("Recovery mode: will sync %d streams to target position %s", len(remainingProcessing), nextPos)
		a.driver.SetTargetCDCPosition(nextPos)
	} else {
		logger.Infof("Recovery mode: will sync %d streams to latest (no target position)", len(remainingProcessing))
	}

	return true, processingSet, nil
}
