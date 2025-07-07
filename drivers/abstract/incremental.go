package abstract

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
)

func (a *AbstractDriver) Incremental(_ context.Context, pool *destination.WriterPool, streams ...types.StreamInterface) error {
	if len(streams) == 0 {
		logger.Infof("No streams provided for incremental sync")
		return nil
	}

	logger.Infof("Starting incremental sync for %d streams", len(streams))

	// Process each stream concurrently
	streamProcessor := func(ctx context.Context, stream types.StreamInterface) (err error) {
		//Kafka Specific
		var lastOffsets sync.Map

		if stream.GetSyncMode() != types.INCREMENTAL && stream.GetSyncMode() != types.CDC {
			return fmt.Errorf("stream %s is not configured for incremental or CDC sync mode", stream.ID())
		}

		logger.Infof("Starting incremental sync for stream %s", stream.ID())

		// Create a new writer thread for this stream
		errorChannel := make(chan error, 1)
		inserter := pool.NewThread(ctx, stream, errorChannel, destination.WithBackfill(false))
		defer func() {
			inserter.Close()
			if writerErr := <-errorChannel; writerErr != nil {
				logger.Errorf("Error in writer thread for stream %s: %v", stream.ID(), writerErr)
			}

			if err == nil {
				// Kafka Incremental Specific
				lastOffsets.Range(func(key, value interface{}) bool {
					partition := key.(int)
					offset := value.(int64)
					keyStr := fmt.Sprintf("partition_%d_offset", partition)
					a.state.SetCursor(stream.Self(), keyStr, offset)
					logger.Infof("Persisted final cursor for stream %s: %s=%d", stream.ID(), keyStr, offset)
					return true
				})
			}
		}()

		// Call the driver's StreamIncremental method with a process function
		return a.driver.StreamIncremental(ctx, stream, func(data map[string]interface{}) error {
			olakeID := utils.GetKeysHash(data, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
			timestamp, ok := data[constants.CdcTimestamp].(int64)
			if !ok {
				timestamp = time.Now().UnixMilli()
			}
			// Kafka Specific
			if err := inserter.Insert(types.CreateRawRecord(olakeID, data, "r", time.UnixMilli(timestamp))); err != nil {
				return err
			}
			// TODO: Make it parallel in nature
			if partition, ok := data["partition"].(int); ok {
				if offset, ok := data["offset"].(int64); ok {
					lastOffsets.Store(partition, offset)
				}
			}
			return nil
		})
	}

	// Run stream processors concurrently
	utils.ConcurrentInGroup(a.GlobalConnGroup, streams, streamProcessor)

	// Wait for all streams to complete or context to be canceled
	if err := a.GlobalConnGroup.Block(); err != nil {
		return fmt.Errorf("error occurred while processing incremental streams: %s", err)
	}

	logger.Infof("Completed incremental sync for %d streams", len(streams))
	return nil
}
