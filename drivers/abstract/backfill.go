package abstract

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
)

func (a *AbstractDriver) Backfill(ctx context.Context, backfilledStreams chan string, pool *destination.WriterPool, stream types.StreamInterface) error {
	chunksSet := a.state.GetChunks(stream.Self())
	var err error
	if chunksSet == nil || chunksSet.Len() == 0 {
		chunksSet, err = a.driver.GetOrSplitChunks(ctx, pool, stream)
		if err != nil {
			return fmt.Errorf("failed to get or split chunks: %s", err)
		}
		// set state chunks
		a.state.SetChunks(stream.Self(), chunksSet)
	}
	chunks := chunksSet.Array()
	if len(chunks) == 0 {
		if backfilledStreams != nil {
			backfilledStreams <- stream.ID()
		}
		return nil
	}

	// Sort chunks by their minimum value
	sort.Slice(chunks, func(i, j int) bool {
		return utils.CompareInterfaceValue(chunks[i].Min, chunks[j].Min) < 0
	})
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))

	// For consistent backfill, we'll capture a snapshot time/SCN before processing chunks
	// This ensures cursor consistency without breaking concurrent processing or resumability
	var snapshotTime time.Time
	var snapshotData map[string]interface{}

	// Capture consistent snapshot information before chunk processing
	if stream.GetSyncMode() == types.INCREMENTAL {
		snapshotTime = time.Now()
		snapshotData = make(map[string]interface{})
		snapshotData["snapshot_time"] = snapshotTime
	}

	// TODO: create writer instance again on retry
	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		var maxPrimaryCursorValue, maxSecondaryCursorValue any
		primaryCursor, secondaryCursor := stream.Cursor()
		errorChannel := make(chan error, 1)
		inserter := pool.NewThread(ctx, stream, errorChannel, destination.WithBackfill(true))
		defer func() {
			inserter.Close()
			// wait for chunk completion
			if writerErr := <-errorChannel; writerErr != nil {
				err = fmt.Errorf("failed to insert chunk min[%s] and max[%s] of stream %s, insert func error: %s, thread error: %s", chunk.Min, chunk.Max, stream.ID(), err, writerErr)
			}

			// check for panics before saving state
			if r := recover(); r != nil {
				err = fmt.Errorf("panic recovered in backfill: %v, prev error: %s", r, err)
			}

			if err == nil {
				logger.Infof("finished chunk min[%v] and max[%v] of stream %s", chunk.Min, chunk.Max, stream.ID())
				chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
				if chunksLeft == 0 && backfilledStreams != nil {
					backfilledStreams <- stream.ID()
				}

				// if it is incremental update the max cursor value received in chunk
				if stream.GetSyncMode() == types.INCREMENTAL && (maxPrimaryCursorValue != nil || maxSecondaryCursorValue != nil) {
					prevPrimaryCursor, prevSecondaryCursor, cursorErr := a.getIncrementCursorFromState(primaryCursor, secondaryCursor, stream)
					if cursorErr != nil {
						err = cursorErr
						return
					}
					if typeutils.Compare(maxPrimaryCursorValue, prevPrimaryCursor) == 1 {
						a.state.SetCursor(stream.Self(), primaryCursor, a.reformatCursorValue(maxPrimaryCursorValue))
					}
					if typeutils.Compare(maxSecondaryCursorValue, prevSecondaryCursor) == 1 {
						a.state.SetCursor(stream.Self(), secondaryCursor, a.reformatCursorValue(maxSecondaryCursorValue))
					}
				}
			}
		}()
		return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
			// Create per-chunk transaction for database consistency
			// Each chunk gets its own transaction to avoid concurrent access issues
			tx, txErr := a.beginBackfillTransaction(ctx)
			if txErr != nil {
				return fmt.Errorf("failed to begin chunk transaction: %s", txErr)
			}

			// Ensure transaction cleanup with proper error handling
			var chunkErr error
			defer func() {
				if tx != nil {
					if chunkErr == nil {
						// Successful processing - commit transaction
						if commitErr := tx.Commit(); commitErr != nil {
							logger.Warnf("chunk transaction commit failed: %s", commitErr)
							chunkErr = fmt.Errorf("failed to commit chunk transaction: %s", commitErr)
						}
					} else {
						// Error occurred - rollback transaction
						if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
							logger.Warnf("chunk transaction rollback failed: %s", rollbackErr)
						}
					}
				}
			}()

			chunkErr = a.driver.ChunkIterator(ctx, stream, chunk, tx, func(data map[string]any) error {
				// if incremental enabled check cursor value
				if stream.GetSyncMode() == types.INCREMENTAL {
					maxPrimaryCursorValue, maxSecondaryCursorValue = a.getMaxIncrementCursorFromData(primaryCursor, secondaryCursor, maxPrimaryCursorValue, maxSecondaryCursorValue, data)
				}
				olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
				return inserter.Insert(types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0)))
			})
			return chunkErr
		})
	}
	utils.ConcurrentInGroup(a.GlobalConnGroup, chunks, chunkProcessor)

	return nil
}

// beginBackfillTransaction starts a transaction with appropriate isolation level for backfill
func (a *AbstractDriver) beginBackfillTransaction(ctx context.Context) (*sql.Tx, error) {
	return a.driver.BeginBackfillTransaction(ctx)
}
