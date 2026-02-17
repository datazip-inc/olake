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
				return a.streamChanges(connGroupCtx, pool, streamIndex, streams)
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
			return a.streamChanges(ctx, pool, streamIndex, streams)
		})
		return nil
	} else if isSequentialMode {
		a.GlobalConnGroup.AddWithRetry(a.driver.MaxRetries(), func(connGroupCtx context.Context) error {
			return a.streamChanges(connGroupCtx, pool, 0, streams)
		})
	}
	return nil
}

// streamChanges processes CDC changes. It does not receive the streams list; the driver
// has streams from PreCDC(streams). Each change event from the driver includes change.Stream,
// so we learn which stream a change belongs to per event. In recovery mode, we only sync
func (a *AbstractDriver) streamChanges(mainCtx context.Context, pool *destination.WriterPool, streamIndex int, streams []types.StreamInterface) (err error) {
	writers := make(map[string]*destination.WriterThread)

	// Get starting position once at the beginning for consistent thread IDs
	// For global position drivers (MySQL/Postgres), use GetCDCStartPosition
	// For per-stream drivers (MongoDB), use change.Position per stream
	var startingPosition string

	// Use recovery state prepared by cdcRecovery
	var isRecoveryMode bool

	// create cdc context, so that main context not affected if cdc retries
	cdcCtx, cdcCtxCancel := context.WithCancel(mainCtx)
	defer cdcCtxCancel()

	// Map to hold payload references for each stream to update captured_cdc_pos dynamically
	streamPayloads := make(map[string]*SyncState)

	streamsToPreCreate := a.streamsToProcessForCDC(streams, streamIndex)

	uncommittedStreams := types.NewSet[string]()
	checkCommit := func(stream types.StreamInterface, threadID string, w *destination.WriterThread) (bool, error) {
		committed, capturedPos, err := a.isCommittedThread(cdcCtx, stream, threadID, w)
		if err != nil {
			return false, err
		}
		if committed && capturedPos != "" {
			a.driver.SetTargetCDCPosition(stream, capturedPos)
		}

		return committed, nil
	}

	if utils.ExistInArray(constants.TwoPCSupportedDrivers, constants.DriverType(a.driver.Type())) {
		for _, stream := range streamsToPreCreate {
			streamID := stream.ID()
			startPos, posErr := a.driver.GetCDCStartPosition(stream, streamIndex)
			if posErr != nil {
				return fmt.Errorf("failed to get start position for stream %s: %w", streamID, posErr)
			}
			threadID := cdcThreadID(streamID, startPos)
			payload := &SyncState{CapturedCDCPos: startPos}
			streamPayloads[streamID] = payload
			w, createErr := pool.NewWriter(cdcCtx, stream, destination.WithThreadID(threadID), destination.WithSyncMode("cdc"), destination.WithPayload(payload))
			if createErr != nil {
				return fmt.Errorf("failed to create CDC writer for stream %s: %s", streamID, createErr)
			}
			writers[streamID] = w

			committed, err := checkCommit(stream, threadID, w)
			if err != nil {
				return fmt.Errorf("failed to check commit for stream %s: %s", streamID, err)
			}
			if committed {
				continue
			}
			uncommittedStreams.Insert(streamID)
		}

		if uncommittedStreams.Len() != len(streamsToPreCreate) {
			isRecoveryMode = true
		}
	}

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
				finalPos := a.driver.GetCDCPosition(streamID)

				if finalPos != "" {
					if p, ok := streamPayloads[streamID]; ok && p != nil {
						p.CapturedCDCPos = finalPos
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

		// In recovery mode, only sync streams that are in the processing set
		if isRecoveryMode && (uncommittedStreams.Len() == 0 || !uncommittedStreams.Exists(streamID)) {
			return nil // Skip this stream - not in processing set
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

			payload := &SyncState{CapturedCDCPos: change.Position}
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

func cdcThreadID(streamID, positionForHash string) string {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(positionForHash)))
	return fmt.Sprintf("%s_%s", streamID, hash)
}

// streamsToProcessForCDC returns the slice of streams to pre-create writers for in streamChanges
func (a *AbstractDriver) streamsToProcessForCDC(streams []types.StreamInterface, streamIndex int) []types.StreamInterface {
	if len(streams) == 0 {
		return nil
	}
	if utils.ExistInArray(constants.ParallelCDCDrivers, constants.DriverType(a.driver.Type())) {
		return streams[streamIndex : streamIndex+1]
	}
	return streams
}

func (a *AbstractDriver) isCommittedThread(ctx context.Context, stream types.StreamInterface, threadID string, writer *destination.WriterThread) (committed bool, capturedPos string, err error) {
	statePayload, err := writer.GetCommitState(ctx)
	if err != nil {
		return false, "", fmt.Errorf("failed to check commit status for stream %s: %s", stream.ID(), err)
	}
	if statePayload == "" {
		return false, "", nil
	}

	var state SyncState
	_ = json.Unmarshal([]byte(statePayload), &state)
	if state.LatestThreadID != threadID {
		return false, "", nil
	}
	return true, state.CapturedCDCPos, nil
}
