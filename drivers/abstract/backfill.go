package abstract

import (
	"context"
	"crypto/sha256"
	"encoding/json"
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

type backfillCommitState struct {
	FullRefreshCommittedIDs []string `json:"full_refresh_committed_ids"`
}

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

	// Helper to generate threadID for a chunk
	threadIDCache := make(map[string]string)
	getThreadID := func(chunk types.Chunk) string {
		keys := fmt.Sprintf("%v%v", chunk.Min, chunk.Max)
		if id, ok := threadIDCache[keys]; ok {
			return id
		}
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(keys)))
		id := generateThreadID(stream.ID(), hash)
		threadIDCache[keys] = id
		return id
	}

	logger.Infof("Processing %d chunks for stream[%s]", len(chunks), stream.ID())

	committedChunks := a.fetchBackfillCommittedThreadIDs(mainCtx, pool, stream)

	chunkProcessor := func(gCtx context.Context, _ int, chunk types.Chunk) (err error) {
		threadID := getThreadID(chunk)

		if committedChunks[threadID] {
			logger.Infof("Chunk min[%v] max[%v] (thread %s) already committed, skipping", chunk.Min, chunk.Max, threadID)
			chunksLeft := a.state.RemoveChunk(stream.Self(), chunk)
			if chunksLeft == 0 && backfilledStreams != nil {
				backfilledStreams <- stream.ID()
			}
			return nil
		}

		// create backfill context, so that main context not affected if backfill retries
		backfillCtx, backfillCtxCancel := context.WithCancel(gCtx)
		defer backfillCtxCancel()

		inserter, err := pool.NewWriter(backfillCtx, stream, destination.WithBackfill(true), destination.WithThreadID(threadID), destination.WithSyncMode("backfill"))
		if err != nil {
			return fmt.Errorf("failed to create new writer thread: %s", err)
		}

		defer handleWriterCleanup(backfillCtx, backfillCtxCancel, &err, inserter, threadID,
			func(ctx context.Context) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return nil
			},
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

		logger.Infof("Thread[%s]: created writer for chunk min[%s] and max[%s] of stream %s", threadID, chunk.Min, chunk.Max, stream.ID())
		return a.driver.ChunkIterator(backfillCtx, stream, chunk, func(ctx context.Context, data map[string]any) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			olakeColumns := map[string]any{
				constants.OlakeID:        olakeID,
				constants.OpType:         "r",
				constants.OlakeTimestamp: time.Now().UTC(),
			}

			// Add CDC specific columns only for CDC mode
			if stream.GetSyncMode() == types.CDC {
				olakeColumns[constants.CdcTimestamp] = time.Unix(0, 0)
			}
			return inserter.Push(ctx, types.CreateRawRecord(data, olakeColumns))
		})
	}
	utils.ConcurrentInGroupWithRetry(a.GlobalConnGroup, chunks, a.driver.MaxRetries(), chunkProcessor)
	return nil
}

func (a *AbstractDriver) fetchBackfillCommittedThreadIDs(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) map[string]bool {
	committed := make(map[string]bool)

	stateFetcher, err := pool.NewWriter(ctx, stream, destination.WithThreadID("recovery_check"), destination.WithBackfill(true))
	if err != nil {
		logger.Warnf("Failed to create writer for state fetch, skipping optimization: %s", err)
		return committed
	}
	defer stateFetcher.Close(ctx)

	statePayload, err := stateFetcher.GetCommitState(ctx)
	if err != nil {
		logger.Warnf("Failed to fetch commit state, skipping optimization: %s", err)
		return committed
	}
	if statePayload == "" {
		return committed
	}

	var state backfillCommitState
	if err := json.Unmarshal([]byte(statePayload), &state); err != nil {
		logger.Warnf("Failed to parse commit state JSON, skipping optimization: %s", err)
		return committed
	}

	for _, id := range state.FullRefreshCommittedIDs {
		if id != "" {
			committed[id] = true
		}
	}
	return committed
}
