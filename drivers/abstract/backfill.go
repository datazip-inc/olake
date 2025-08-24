/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
		return utils.CompareInterfaceValue(chunks[i].Min, chunks[j].Min) < 0
	})
	logger.Infof("Starting backfill for stream[%s] with %d chunks", stream.GetStream().Name, len(chunks))
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
			return a.driver.ChunkIterator(ctx, stream, chunk, func(data map[string]any) error {
				// if incremental enabled check cursor value
				if stream.GetSyncMode() == types.INCREMENTAL {
					maxPrimaryCursorValue, maxSecondaryCursorValue = a.getMaxIncrementCursorFromData(primaryCursor, secondaryCursor, maxPrimaryCursorValue, maxSecondaryCursorValue, data)
				}
				olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
				return inserter.Insert(types.CreateRawRecord(olakeID, data, "r", time.Unix(0, 0)))
			})
		})
	}
	utils.ConcurrentInGroup(a.GlobalConnGroup, chunks, chunkProcessor)
	return nil
}
