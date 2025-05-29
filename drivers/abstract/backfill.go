package abstract

import (
	"context"
	"fmt"
	"sort"
	"time"

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
		} //////////////////////////
		// set state chunks
		a.state.SetChunks(stream.Self(), chunksSet)
	}
	chunks := chunksSet.Array()
	if len(chunks) == 0 {
		backfilledStreams <- stream.ID()
		return nil
	}
	sort.Slice(chunks, func(i, j int) bool {
		return utils.CompareInterfaceValue(chunks[i].Min, chunks[j].Min) < 0
	})
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))

	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		errorChannel := make(chan error, 1)
		inserter, err := pool.NewThread(ctx, stream, destination.WithErrorChannel(errorChannel), destination.WithBackfill(true))
		if err != nil {
			return err
		}
		defer func() {
			inserter.Close()
			if err == nil {
				// wait for chunk completion
				err = <-errorChannel
			}
			if err == nil {
				logger.Infof("finished chunk min[%v] and max[%v] of stream %s", chunk.Min, chunk.Max, stream.ID())
				remCount := a.state.RemoveChunk(stream.Self(), chunk)
				if remCount == 0 && backfilledStreams != nil {
					backfilledStreams <- stream.ID()
				}
			}
		}()
		// TODO: add backoff for connection errors
		return a.driver.ChunkIterator(ctx, stream, chunk, func(data map[string]any) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			rawRecord := types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0))
			err := inserter.Insert(types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0)))
			if err != nil {
				return fmt.Errorf("failed to insert raw record[%v]: %s", rawRecord, err)
			}
			return nil
		})
	}
	utils.ConcurrentInGroup(a.GlobalConnGroup, chunks, chunkProcessor)
	return nil
}
