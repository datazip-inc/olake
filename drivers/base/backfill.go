package base

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

type GetOrSplitChunkFunc func(ctx context.Context, pool *protocol.WriterPool, stream protocol.Stream) ([]types.Chunk, error)
type BackfillMessageProcessFunc func(data map[string]any) error
type ChunkIteratorFunc func(ctx context.Context, chunk types.Chunk, callback BackfillMessageProcessFunc) error

func (d *Driver) Backfill(ctx context.Context, chunkSplitter GetOrSplitChunkFunc, chunkIterator ChunkIteratorFunc, backfilledStreams chan string, pool *protocol.WriterPool, stream protocol.Stream) error {
	chunks, err := chunkSplitter(ctx, pool, stream)
	if err != nil {
		return fmt.Errorf("failed to get or split chunks")
	}
	sort.Slice(chunks, func(i, j int) bool {
		return utils.CompareInterfaceValue(chunks[i].Min, chunks[j].Min) < 0
	})
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))

	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		errorChannel := make(chan error, 1)
		inserter, err := pool.NewThread(ctx, stream, protocol.WithErrorChannel(errorChannel), protocol.WithBackfill(true))
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
				remCount := d.State.RemoveChunk(stream.Self(), chunk)
				if remCount == 0 && backfilledStreams != nil {
					backfilledStreams <- stream.ID()
				}
			}
		}()
		return chunkIterator(ctx, chunk, func(data map[string]any) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			rawRecord := types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0))
			err := inserter.Insert(types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0)))
			if err != nil {
				return fmt.Errorf("failed to insert raw record[%v]: %s", rawRecord, err)
			}
			return nil
		})
	}
	utils.ConcurrentInGroup(protocol.GlobalConnGroup, chunks, chunkProcessor)
	return nil
}
