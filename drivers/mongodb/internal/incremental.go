package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IncrementalSync performs incremental sync for MongoDB collections
// It uses a cursor field to track the last synced position and only syncs new/updated records
func (m *Mongo) IncrementalSync(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) error {
	cursorField := stream.Cursor()
	if cursorField == "" {
		return fmt.Errorf("cursor field is required for incremental sync")
	}

	// Get the last cursor value from state
	lastCursorValue := m.state.GetCursor(stream.Self(), cursorField)

	collection := m.client.Database(stream.Namespace()).Collection(stream.Name())

	// Build the filter for incremental sync
	filter := bson.M{}
	if lastCursorValue != nil {
		// Add filter to get records after the last cursor value
		filter[cursorField] = bson.M{"$gt": lastCursorValue}
	}

	// Set up find options with optimized settings
	findOpts := options.Find().
		SetSort(bson.D{{Key: cursorField, Value: 1}}).
		SetBatchSize(int32(m.config.BatchSize)).
		SetAllowDiskUse(false).      // Don't allow disk use for better performance
		SetMaxTime(30 * time.Second) // Set max query time

	logger.Infof("Starting incremental sync for stream[%s] with cursor field[%s], last value[%v]",
		stream.ID(), cursorField, lastCursorValue)

	// Create inserter for this stream
	errChan := make(chan error, 1)
	inserter := pool.NewThread(ctx, stream, errChan)
	defer func() {
		inserter.Close()
		if threadErr := <-errChan; threadErr != nil {
			logger.Errorf("Failed to insert incremental record of stream %s: %s", stream.ID(), threadErr)
		}
	}()

	var maxCursorValue interface{}
	recordCount := int64(0)

	// Execute the query with index hint for better performance
	// Try to use an index on the cursor field if available
	indexHint := bson.M{}
	if cursorField != "_id" {
		// For non-_id fields, try to hint the index
		indexHint = bson.M{"hint": cursorField}
	}

	cursor, err := collection.Find(ctx, filter, findOpts.SetHint(indexHint))
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer cursor.Close(ctx)

	// Process each record
	for cursor.Next(ctx) {
		var record bson.M
		if err := cursor.Decode(&record); err != nil {
			return fmt.Errorf("failed to decode record: %s", err)
		}

		// Filter MongoDB-specific types
		filterMongoObject(record)

		// Get the cursor value for this record
		cursorValue, exists := record[cursorField]
		if !exists {
			logger.Warnf("Cursor field %s not found in record for stream %s", cursorField, stream.ID())
			continue
		}

		// Update max cursor value
		if maxCursorValue == nil || utils.CompareInterfaceValue(cursorValue, maxCursorValue) > 0 {
			maxCursorValue = cursorValue
		}

		// Create raw record
		olakeID := utils.GetKeysHash(record, stream.GetStream().SourceDefinedPrimaryKey.Array()...)
		rawRecord := types.CreateRawRecord(olakeID, record, "r", time.Now().UTC())

		// Insert the record
		if err := inserter.Insert(rawRecord); err != nil {
			return fmt.Errorf("failed to insert incremental record: %s", err)
		}

		recordCount++
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error during incremental sync: %s", err)
	}

	// Update the cursor value in state if we processed any records
	if maxCursorValue != nil {
		m.state.SetCursor(stream.Self(), cursorField, maxCursorValue)
		logger.Infof("Updated cursor value for stream[%s] to %v", stream.ID(), maxCursorValue)
	}

	logger.Infof("Completed incremental sync for stream[%s], processed %d records", stream.ID(), recordCount)
	return nil
}

// GetOrSplitChunksForIncremental creates chunks for incremental sync
// For incremental sync, we typically don't need complex chunking since we're only processing new records
func (m *Mongo) GetOrSplitChunksForIncremental(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// For incremental sync, we create a single chunk that represents the entire incremental range
	// The actual filtering will be done in the IncrementalSync method based on the cursor value

	chunks := types.NewSet[types.Chunk]()
	chunks.Insert(types.Chunk{
		Min: nil,
		Max: nil,
	})

	return chunks, nil
}

// ChunkIteratorForIncremental handles chunk processing for incremental sync
func (m *Mongo) ChunkIteratorForIncremental(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, OnMessage abstract.BackfillMsgFn) error {
	// For incremental sync, we delegate to the main incremental sync method
	// This is a simplified approach since incremental sync doesn't typically need complex chunking
	return m.IncrementalSync(ctx, nil, stream)
}
