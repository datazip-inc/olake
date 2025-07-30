package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// StreamIncrementalChanges implements incremental sync for MongoDB
func (m *Mongo) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	collection := m.client.Database(stream.Namespace()).Collection(stream.Name())
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	lastSecondaryCursorValue := m.state.GetCursor(stream.Self(), secondaryCursor)

	filter, err := buildFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to build filter: %w", err)
	}

	incrementalFilter, err := m.buildIncrementalCondition(primaryCursor, secondaryCursor, lastPrimaryCursorValue, lastSecondaryCursorValue)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}

	// Merge cursor filter with stream filter using $and
	filter = utils.Ternary(len(filter) > 0, bson.D{{Key: "$and", Value: bson.A{incrementalFilter, filter}}}, incrementalFilter).(bson.D)

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

// buildIncrementalCondition generates the incremental condition BSON for MongoDB based on datatype and cursor value.
func (m *Mongo) buildIncrementalCondition(primaryCursorField string, secondaryCursorField string, lastPrimaryCursorValue any, lastSecondaryCursorValue any) (bson.D, error) {
	var incrementalCondition bson.D

	if secondaryCursorField != "" {
		incrementalCondition = bson.D{
			{Key: "$or", Value: bson.A{
				buildMongoCondition(types.Condition{
					Column:   primaryCursorField,
					Value:    fmt.Sprintf("%v", lastPrimaryCursorValue),
					Operator: ">=",
				}),
				bson.D{
					{Key: "$and", Value: bson.A{
						bson.D{{Key: primaryCursorField, Value: nil}},
						buildMongoCondition(types.Condition{
							Column:   secondaryCursorField,
							Value:    fmt.Sprintf("%v", lastSecondaryCursorValue),
							Operator: ">=",
						}),
					}},
				},
			}},
		}
	} else {
		incrementalCondition = buildMongoCondition(types.Condition{
			Column:   primaryCursorField,
			Value:    fmt.Sprintf("%v", lastPrimaryCursorValue),
			Operator: ">=",
		})
	}

	return incrementalCondition, nil
}
