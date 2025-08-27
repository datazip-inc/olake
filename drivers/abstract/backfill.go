package abstract

import (
	"context"
	"fmt"
	"sort"

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
		return typeutils.Compare(chunks[i].Min, chunks[j].Min) < 0
	})
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))
	// TODO: create writer instance again on retry
	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		var maxPrimaryCursorValue, maxSecondaryCursorValue any
		primaryCursor, secondaryCursor := stream.Cursor()
		threadID := utils.ULID()
		inserter, err := pool.NewWriter(ctx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %s", err)
		}
		logger.Infof("created writer with threadID[%s] for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())
		defer func() {
			// wait for chunk completion
			if writerErr := inserter.Close(ctx); writerErr != nil {
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
			return a.driver.ChunkIterator(ctx, stream, chunk, func(ctx context.Context, data map[string]any) error {
				// if incremental enabled check cursor value
				if stream.GetSyncMode() == types.INCREMENTAL {
					maxPrimaryCursorValue, maxSecondaryCursorValue = a.getMaxIncrementCursorFromData(primaryCursor, secondaryCursor, maxPrimaryCursorValue, maxSecondaryCursorValue, data)
				}
				olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
				return inserter.Push(ctx, types.CreateRawRecord(olakeID, data, "r", nil))
			})
		})
	}
	utils.ConcurrentInGroup(a.GlobalConnGroup, chunks, chunkProcessor)
	return nil
}
