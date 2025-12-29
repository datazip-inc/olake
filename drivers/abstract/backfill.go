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

func (a *AbstractDriver) Backfill(mainCtx context.Context, backfilledStreams chan string, pool *destination.WriterPool, stream types.StreamInterface) error {
	chunksSet := a.state.GetChunks(stream.Self())
	var err error
	if chunksSet == nil || chunksSet.Len() == 0 {
		chunksSet, err = a.driver.GetOrSplitChunks(mainCtx, pool, stream)
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

	chunkProcessor := func(gCtx context.Context, _ int, chunk types.Chunk) (err error) {
		// create backfill context, so that main context not affected if backfill retries
		backfillCtx, backfillCtxCancel := context.WithCancel(gCtx)
		defer backfillCtxCancel()

		threadID := generateThreadID(stream.ID())
		inserter, err := pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %s", err)
		}

		logger.Infof("Thread[%s]: created writer for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())
		defer func() {
			if err != nil {
				err = fmt.Errorf("%w: %s", constants.ErrorBackfillFailed, err)
			}
		}()

		defer handleWriterCleanup(backfillCtx, backfillCtxCancel, &err, inserter, threadID,
			func(ctx context.Context) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
				if chunksLeft == 0 && backfilledStreams != nil {
					backfilledStreams <- stream.ID()
				}
				logger.Infof("finished chunk min[%v] and max[%v] of stream %s", chunk.Min, chunk.Max, stream.ID())
				return nil
			})()
		return a.driver.ChunkIterator(backfillCtx, stream, chunk, func(ctx context.Context, data map[string]any) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			// persist cdc timestamp for cdc full load
			var cdcTimestamp *time.Time
			if stream.GetSyncMode() == types.CDC {
				t := time.Unix(0, 0)
				cdcTimestamp = &t
			}

			return inserter.Push(ctx, types.CreateRawRecord(olakeID, data, "r", cdcTimestamp))
		})
	}
	utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, chunks, a.driver.MaxRetries(), chunkProcessor)
	return nil
}
