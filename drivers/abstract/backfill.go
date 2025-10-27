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

		// Persist total record count from pool stats to state for resume capability
		if pool.GetStats().TotalRecordsToSync.Load() > 0 && a.state != nil {
			a.state.SetTotalRecordCount(stream.Self(), pool.GetStats().TotalRecordsToSync.Load())
		}
	} else {
		// This is a resumed sync - restore stats from state
		if a.state.GetTotalRecordCount(stream.Self()) > 0 {
			logger.Infof("Resuming sync for stream %s: total records = %d", stream.ID(), a.state.GetTotalRecordCount(stream.Self()))
			// Restore total count to pool stats for progress tracking
			pool.AddRecordsToSyncStats(a.state.GetTotalRecordCount(stream.Self()))
		}
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

	logger.Infof("Processing backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))

	// TODO: create writer instance again on retry
	chunkProcessor := func(ctx context.Context, chunk types.Chunk) (err error) {
		threadID := fmt.Sprintf("%s_%s", stream.ID(), utils.ULID())
		inserter, err := pool.NewWriter(ctx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %s", err)
		}
		logger.Infof("Thread[%s]: created writer for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())
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

				// Update synced record count in state based on committed records only
				// This represents records that have been successfully written to the destination
				a.state.SetSyncedRecordCount(stream.Self(), a.state.GetSyncedRecordCount(stream.Self())+inserter.GetCommittedCount())
				logger.Infof("Stream %s: chunk completed with %d committed records (total synced: %d)", stream.ID(), inserter.GetCommittedCount(), a.state.GetSyncedRecordCount(stream.Self()))

				if chunksLeft == 0 && backfilledStreams != nil {
					backfilledStreams <- stream.ID()
				}
			} else {
				err = fmt.Errorf("thread[%s]: %s", threadID, err)
			}
		}()
		return RetryOnBackoff(a.driver.MaxRetries(), constants.DefaultRetryTimeout, func() error {
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
