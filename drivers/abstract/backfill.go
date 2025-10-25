package abstract

import (
	"context"
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
		return typeutils.Compare(chunks[i].Min, chunks[j].Min) < 0
	})
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))
	// TODO: create writer instance again on retry
	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		// create backfill context, so that main context not affected if backfill retries
		backfillCtx, backfillCtxCancel := context.WithCancel(ctx)
		defer backfillCtxCancel()

		threadID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
		inserter, err := pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %s", err)
		}
		logger.Infof("Thread[%s]: created writer for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())
		defer func() {
			// wait for chunk completion
			if writerErr := inserter.Close(backfillCtx); writerErr != nil {
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
			} else {
				err = fmt.Errorf("thread[%s]: %s", threadID, err)
			}
		}()
		return utils.RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func(cur int) error {
			if cur > 0 {
				// close prev writer
				inserter.Close(backfillCtx)

				// create new backfill context
				backfillCtx, backfillCtxCancel = context.WithCancel(ctx)
				threadID = fmt.Sprintf("%s-retry-attempt-%d", threadID, cur)

				// re-initialize inserter with backfillCtx for consistency
				inserter, err = pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID))
				if err != nil {
					return fmt.Errorf("failed to create new writer thread: %s", err)
				}
			}

			return a.driver.ChunkIterator(ctx, stream, chunk, func(ctx context.Context, data map[string]any) error {
				olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
				// persist cdc timestamp for cdc full load
				var cdcTimestamp *time.Time
				if stream.GetSyncMode() == types.CDC {
					t := time.Unix(0, 0)
					cdcTimestamp = &t
				}

				return inserter.Push(ctx, types.CreateRawRecord(olakeID, data, "r", cdcTimestamp))
			})
		})
	}
	utils.ConcurrentInGroup(a.GlobalConnGroup, chunks, chunkProcessor)
	return nil
}
