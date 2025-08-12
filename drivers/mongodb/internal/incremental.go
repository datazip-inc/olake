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

	filter, err := buildFilter(stream)
	if err != nil {
		return fmt.Errorf("failed to build filter: %s", err)
	}

	incrementalFilter, err := m.buildIncrementalCondition(stream)
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
		return fmt.Errorf("failed to execute incremental query: %s", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decode error: %s", err)
		}
		filterMongoObject(doc)
		if err := processFn(doc); err != nil {
			return fmt.Errorf("process error: %s", err)
		}
	}

	return cursor.Err()
}

// buildIncrementalCondition generates the incremental condition BSON for MongoDB based on datatype and cursor value.
func (m *Mongo) buildIncrementalCondition(stream types.StreamInterface) (bson.D, error) {
	primaryCursor, secondaryCursor := stream.Cursor()
	lastPrimaryCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	lastSecondaryCursorValue := m.state.GetCursor(stream.Self(), secondaryCursor)
	if lastPrimaryCursorValue == nil {
		logger.Warnf("Stored primary cursor value is nil for the stream [%s]", stream.ID())
	}
	if secondaryCursor != "" && lastSecondaryCursorValue == nil {
		logger.Warnf("Stored secondary cursor value is nil for the stream [%s]", stream.ID())
	}

	incrementalCondition := buildMongoCondition(types.Condition{
		Column:   primaryCursor,
		Value:    fmt.Sprintf("%v", lastPrimaryCursorValue),
		Operator: ">=",
	})

	// If secondary is enabled, build an OR condition with fallback
	if secondaryCursor != "" {
		incrementalCondition = bson.D{{
			Key: "$or", Value: bson.A{
				incrementalCondition,
				bson.D{{
					Key: "$and", Value: bson.A{
						bson.D{{Key: primaryCursor, Value: nil}},
						buildMongoCondition(types.Condition{
							Column:   secondaryCursor,
							Value:    fmt.Sprintf("%v", lastSecondaryCursorValue),
							Operator: ">=",
						}),
					},
				}},
			},
		}}
	}

	return incrementalCondition, nil
}
