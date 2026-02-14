package abstract

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"time"

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
	isRecoveryMode, processingSet, err := a.cdcRecovery(mainCtx, pool, streams)
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
// Recovery Mode: If recoveryMode=true, only streams in recoveryProcessingSet are synced (up to target position set by cdcRecovery).
func (a *AbstractDriver) streamChanges(mainCtx context.Context, pool *destination.WriterPool, streamIndex int) (err error) {
	writers := make(map[string]*destination.WriterThread)

	// Type assertions done once for driver specific methods
	global2PC, supportsGlobalPosition2PC := a.driver.(GlobalPosition2PC)
	perStream2PC, supportsPerStreamPosition2PC := a.driver.(PerStreamRecovery2PC)

	// Get starting position once at the beginning for consistent thread IDs
	// For global position drivers (MySQL/Postgres), use GetCDCStartPosition
	// For per-stream drivers (MongoDB), use change.Position per stream
	var startingPosition string
	if supportsGlobalPosition2PC {
		startingPosition = global2PC.GetCDCStartPosition()
	}

	// Use recovery state prepared by cdcRecovery
	isRecoveryMode := a.recoveryMode
	processingSet := a.recoveryProcessingSet

	// create cdc context, so that main context not affected if cdc retries
	cdcCtx, cdcCtxCancel := context.WithCancel(mainCtx)
	defer cdcCtxCancel()

	// Map to hold payload references for each stream to update captured_cdc_pos dynamically
	streamPayloads := make(map[string]map[string]string)

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

			// Update payload with final position for all streams before closing writers (committing)
			for streamID, writer := range writers {
				if writer == nil {
					continue
				}
				var finalPos string
				if supportsPerStreamPosition2PC {
					finalPos = perStream2PC.GetCDCPositionForStream(streamID)
				} else if supportsGlobalPosition2PC {
					finalPos = global2PC.GetCDCPosition()
				}

				if finalPos != "" {
					if p, ok := streamPayloads[streamID]; ok {
						p["captured_cdc_pos"] = finalPos
						logger.Debugf("Updated payload for stream %s with final CDC position: %s", streamID, finalPos)
					}
				}
			}
			return nil
		},
		func(ctx context.Context) error {
			postCDCErr := a.driver.PostCDC(ctx, streamIndex)
			if postCDCErr != nil {
				return fmt.Errorf("post cdc error: %s", postCDCErr)
			}
			return nil
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

			payload := map[string]string{
				"captured_cdc_pos": change.Position,
			}
			streamPayloads[streamID] = payload

			logger.Debugf("Thread[%s]: creating CDC writer for stream %s", threadID, streamID)

			writer, err = pool.NewWriter(ctx, change.Stream, destination.WithThreadID(threadID), destination.WithSyncMode("cdc"), destination.WithPayload(payload))
			if err != nil {
				return fmt.Errorf("failed to create writer for stream %s: %s", streamID, err)
			}

			writers[streamID] = writer
			logger.Infof("Thread[%s]: created CDC writer for stream %s", threadID, streamID)
		}

		olakeColumns := map[string]any{
			constants.OlakeID:        utils.GetKeysHash(change.Data, change.Stream.GetStream().SourceDefinedPrimaryKey.Array()...),
			constants.OpType:         mapChangeKindToOperationType(change.Kind),
			constants.CdcTimestamp:   change.Timestamp,
			constants.OlakeTimestamp: time.Now().UTC(),
		}
		maps.Copy(olakeColumns, change.ExtraColumns)
		return writer.Push(ctx, types.CreateRawRecord(change.Data, olakeColumns))
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

const (
	commitStateLatestThreadIDKey = "latest_threadId"
	commitStateCapturedCDCPosKey = "captured_cdc_pos"
)

func cdcThreadID(streamID, positionForHash string) string {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(positionForHash)))
	return fmt.Sprintf("%s_%s", streamID, hash)
}

func arrayToSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		set[v] = true
	}
	return set
}

func (a *AbstractDriver) runPerStreamRecoveryChecks(ctx context.Context, pool *destination.WriterPool, streams []types.StreamInterface, perStreamRecovery PerStreamRecovery2PC) error {
	limit := a.driver.MaxConnections()
	if limit <= 0 {
		limit = constants.DefaultThreadCount
	}
	checkGroup := utils.NewCGroupWithLimit(ctx, limit)
	for _, stream := range streams {
		s := stream
		checkGroup.AddWithRetry(a.driver.MaxRetries(), func(groupCtx context.Context) error {
			startPos, err := perStreamRecovery.GetCDCStartPositionForStream(s)
			if err != nil {
				return fmt.Errorf("failed to get start position for %s: %s", s.ID(), err)
			}
			if startPos == "" {
				return nil
			}

			threadID := cdcThreadID(s.ID(), startPos)
			committed, capturedPos, err := a.isCommittedThread(groupCtx, pool, s, threadID)
			if err != nil {
				return err
			}
			if committed && capturedPos != "" {
				if err := perStreamRecovery.SetRecoveredCDCPositionForStream(s, capturedPos); err != nil {
					return fmt.Errorf("failed to set recovered position for %s: %s", s.ID(), err)
				}
			}
			return nil
		})
	}
	return checkGroup.Block()
}

func (a *AbstractDriver) isCommittedThread(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface, threadID string) (committed bool, capturedPos string, err error) {
	writer, err := pool.NewWriter(ctx, stream, destination.WithThreadID(threadID), destination.WithSyncMode("cdc"))
	if err != nil {
		return false, "", fmt.Errorf("failed to create writer for recovery check: %s", err)
	}
	defer func() {
		if closeErr := writer.Close(ctx); closeErr != nil {
			logger.Warnf("Failed to close recovery check writer for stream %s: %s", stream.ID(), closeErr)
		}
	}()

	statePayload, err := writer.GetCommitState(ctx)
	if err != nil {
		return false, "", fmt.Errorf("failed to check commit status for stream %s: %s", stream.ID(), err)
	}
	if statePayload == "" {
		return false, "", nil
	}

	var payloadMap map[string]string
	if err := json.Unmarshal([]byte(statePayload), &payloadMap); err != nil {
		return false, "", nil
	}

	latestID, ok := payloadMap[commitStateLatestThreadIDKey]
	if !ok || latestID != threadID {
		return false, "", nil
	}

	return true, payloadMap[commitStateCapturedCDCPosKey], nil
}

// cdcRecovery checks processing streams commit status and prepares recovery state.
func (a *AbstractDriver) cdcRecovery(ctx context.Context, pool *destination.WriterPool, streams []types.StreamInterface) (bool, map[string]bool, error) {
	global2PC, supportsGlobalPosition2PC := a.driver.(GlobalPosition2PC)
	perStreamRecovery, supportsPerStreamRecovery2PC := a.driver.(PerStreamRecovery2PC)
	posAck, supportsPositionAcknowledgment := a.driver.(PositionAcknowledgment)

	// Per-stream position drivers should not enable "recovery mode"
	if supportsPerStreamRecovery2PC && !supportsGlobalPosition2PC {
		if err := a.runPerStreamRecoveryChecks(ctx, pool, streams, perStreamRecovery); err != nil {
			return false, nil, err
		}
		return false, nil, nil
	}

	if !supportsGlobalPosition2PC {
		return false, nil, nil
	}

	startingPosition := global2PC.GetCDCStartPosition()
	if startingPosition == "" {
		logger.Warnf("Cannot prepare recovery: no starting position available")
		return false, nil, nil
	}

	logger.Infof("Preparing recovery for %d processing streams", len(streams))

	recoveredGlobalPos := ""
	uncommittedStreams := make([]string, 0, len(streams))

	for _, stream := range streams {
		streamID := stream.ID()
		threadID := cdcThreadID(streamID, startingPosition)

		committed, capturedPos, err := a.isCommittedThread(ctx, pool, stream, threadID)
		if err != nil {
			return false, nil, err
		}

		if committed {
			logger.Infof("Stream %s (thread %s) already committed, skipping", streamID, threadID)
			if recoveredGlobalPos == "" && capturedPos != "" {
				// Capture first valid position (assuming all streams share the same global pos).
				recoveredGlobalPos = capturedPos
			}
			continue
		}

		uncommittedStreams = append(uncommittedStreams, streamID)
	}

	// If all streams committed, update source/driver state and proceed normally (no recovery gating).
	if len(uncommittedStreams) == 0 {
		if recoveredGlobalPos != "" {
			if supportsPositionAcknowledgment {
				if err := posAck.AcknowledgeCDCPosition(ctx, recoveredGlobalPos); err != nil {
					logger.Warnf("Failed to acknowledge CDC position during recovery: %s", err)
				}
			}
			global2PC.SetCurrentCDCPosition(recoveredGlobalPos)
		}
		return false, nil, nil
	}

	// If we can't determine a target position, treat as normal start (no bounded recovery).
	if recoveredGlobalPos == "" {
		return false, nil, nil
	}

	logger.Infof("Recovery mode: will sync %d streams to target position %s", len(uncommittedStreams), recoveredGlobalPos)
	global2PC.SetTargetCDCPosition(recoveredGlobalPos)
	return true, arrayToSet(uncommittedStreams), nil
}
