package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *Mongo) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	primaryCursor, _ := stream.Cursor()
	collection := m.client.Database(stream.Namespace()).Collection(stream.Name())
	lastCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	filter := buildMongoCondition(types.Condition{Column: primaryCursor, Value: fmt.Sprintf("%v", lastCursorValue), Operator: ">="})
	// TODO: check performance improvements based on the batch size
	findOpts := options.Find().SetBatchSize(10000)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %v", stream.ID(), filter)

	cursor, err := collection.Find(ctx, filter, findOpts)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decode error: %w", err)
		}
		filterMongoObject(doc)
		if err := processFn(doc); err != nil {
			return fmt.Errorf("process error: %w", err)
		}
	}

	return cursor.Err()
}
