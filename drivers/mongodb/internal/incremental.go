package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

// IncrementalSync performs a query-based sync without Change Streams
func (m *Mongo) incrementalSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	incrementalCtx := context.TODO()

	// Get the last saved cursor value from the state
	lastCursorValue := m.State.GetCursor(stream.Self(), "cursorField")

	// Define the find options
	findOpts := options.Find().
		SetSort(bson.D{{Key: "cursorField", Value: 1}}).  // Sort by cursor field for consistency
		SetBatchSize(1000).                               // Paginate results
		SetProjection(bson.M{"cursorField": 1, "_id": 1}) // Project only necessary fields

	// Define the filter
	filter := bson.D{}
	if lastCursorValue != nil {
		filter = append(filter, bson.E{Key: "cursorField", Value: bson.D{{Key: "$gt", Value: lastCursorValue}}})
	}

	logger.Infof("Running incremental sync for stream [%s] with filter: %s", stream.Name(), filter)

	cursor, err := collection.Find(incrementalCtx, filter, findOpts)
	if err != nil {
		return fmt.Errorf("failed to execute find query: %s", err)
	}
	defer cursor.Close(incrementalCtx)

	insert, err := pool.NewThread(incrementalCtx, stream)
	if err != nil {
		return err
	}
	defer insert.Close()

	var maxCursorValue interface{}
	for cursor.Next(incrementalCtx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("error while decoding: %s", err)
		}

		// Process the document
		handleObjectID(doc)
		err := insert.Insert(types.CreateRawRecord(utils.GetKeysHash(doc, constants.MongoPrimaryID), doc, 0))
		if err != nil {
			return fmt.Errorf("failed to insert document: %s", err)
		}

		// Update maxCursorValue
		if cursorValue, ok := doc["cursorField"]; ok {
			maxCursorValue = cursorValue
		}
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("failed to iterate cursor: %s", err)
	}

	// Save the max cursor value to the state
	if maxCursorValue != nil {
		m.State.SetCursor(stream.Self(), "cursorField", maxCursorValue)
	}

	return nil
}
