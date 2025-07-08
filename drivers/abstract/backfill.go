package abstract

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) Backfill(ctx context.Context, backfilledStreams chan string, pool *destination.WriterPool, stream types.StreamInterface) error {
	chunksSet := a.state.GetChunks(stream.Self())
	var err error
	if chunksSet == nil || chunksSet.Len() == 0 {
		chunksSet, err = a.driver.GetOrSplitChunks(ctx, pool, stream)
		if err != nil {
			return fmt.Errorf("failed to get or split chunks: %s", err)
		}
		a.state.SetChunks(stream.Self(), chunksSet)
	}

	chunks := chunksSet.Array()
	if len(chunks) == 0 {
		if backfilledStreams != nil {
			backfilledStreams <- stream.ID()
		}
		return nil
	}

	sort.Slice(chunks, func(i, j int) bool {
		return utils.CompareInterfaceValue(chunks[i].Min, chunks[j].Min) < 0
	})

	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))
	cursorField := stream.Cursor()

	var (
		finalMaxCursor     any
		chunkMaxCursorLock sync.Mutex
	)

	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		errorChannel := make(chan error, 1)
		inserter := pool.NewThread(ctx, stream, errorChannel, destination.WithBackfill(true))

		defer func() {
			inserter.Close()

			if writerErr := <-errorChannel; writerErr != nil {
				err = fmt.Errorf("failed to insert chunk min[%s] and max[%s] of stream %s, insert func error: %s, thread error: %s", chunk.Min, chunk.Max, stream.ID(), err, writerErr)
			}

			if err == nil {
				logger.Infof("finished chunk min[%v] and max[%v] of stream %s", chunk.Min, chunk.Max, stream.ID())
				chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
				if chunksLeft == 0 && backfilledStreams != nil {
					backfilledStreams <- stream.ID()
				}
			}
		}()

		return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
			maxCursorVal, err := a.driver.ChunkIterator(ctx, stream, chunk, func(data map[string]any) error {
				olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
				return inserter.Insert(types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0)))
			})
			if err != nil {
				return err
			}

			a.driver.SetIncrementalCursor(stream.ID(), maxCursorVal)
			if cursorField != "" && maxCursorVal != nil {
				chunkMaxCursorLock.Lock()
				if finalMaxCursor == nil || utils.CompareInterfaceValue(maxCursorVal, finalMaxCursor) > 0 {
					finalMaxCursor = maxCursorVal
				}
				chunkMaxCursorLock.Unlock()
			}
			return nil
		})
	}

	utils.ConcurrentInGroup(a.GlobalConnGroup, chunks, chunkProcessor)

	// Wait for all concurrent operations to complete
	if err := a.GlobalConnGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while waiting for connection group: %s", err)
	}

	if cursorField != "" && finalMaxCursor != nil {
		a.driver.SetIncrementalCursor(stream.ID(), finalMaxCursor)
		a.state.SetCursor(stream.Self(), cursorField, finalMaxCursor)
		logger.Infof("Backfill cursor set for stream[%s] = %v", stream.ID(), finalMaxCursor)
	}

	return nil
}
