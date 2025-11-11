package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

// StreamIncrementalChanges implements incremental sync for MongoDB
func (m *Mongo) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	collection := m.client.Database(stream.Namespace()).Collection(stream.Name())

	incrementalCondition, err := m.buildIncrementalCondition(stream)
	if err != nil {
		return fmt.Errorf("failed to build incremental condition: %s", err)
	}
	// TODO: check performance improvements based on the batch size
	findOpts := options.Find().SetBatchSize(10000)

	logger.Infof("Starting incremental sync for stream[%s] with filter: %v", stream.ID(), incrementalCondition)

	cursor, err := collection.Find(ctx, incrementalCondition, findOpts)
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
		if err := processFn(ctx, doc); err != nil {
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
		Operator: ">",
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
							Operator: ">",
						}),
					},
				}},
			},
		}}
	}

	return incrementalCondition, nil
}

func (m *Mongo) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	primaryCursor, secondaryCursor := stream.Cursor()

	groupStage := bson.D{
		{Key: "_id", Value: nil},
		{Key: "maxPrimaryCursor", Value: bson.D{{Key: "$max", Value: "$" + primaryCursor}}},
	}
	if secondaryCursor != "" {
		groupStage = append(groupStage, bson.E{
			Key:   "maxSecondaryCursor",
			Value: bson.D{{Key: "$max", Value: "$" + secondaryCursor}},
		})
	}

	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: groupStage}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute find max cursor values: %s", err)
	}
	defer cursor.Close(ctx)

	var result struct {
		MaxPrimaryCursor   any `bson:"maxPrimaryCursor"`
		MaxSecondaryCursor any `bson:"maxSecondaryCursor"`
	}

	if cursor.Next(ctx) {
		if err := cursor.Decode(&result); err != nil {
			return nil, nil, fmt.Errorf("failed to decode cursor result: %s", err)
		}

		if result.MaxPrimaryCursor == nil {
			logger.Warnf("max primary cursor value is nil for stream: %s", stream.ID())
		}
		if secondaryCursor != "" && result.MaxSecondaryCursor == nil {
			logger.Warnf("max secondary cursor value is nil for stream: %s", stream.ID())
		}
		return result.MaxPrimaryCursor, result.MaxSecondaryCursor, nil
	}

	return nil, nil, cursor.Err()
}

// ThresholdFilter creates MongoDB cursor conditions for incremental sync
// This is used to avoid duplication of records during backfill by filtering out
// documents with cursor values greater than the max cursor value
func (m *Mongo) ThresholdFilter(stream types.StreamInterface) (bson.A, error) {
	if stream.GetSyncMode() != types.INCREMENTAL {
		return bson.A{}, nil
	}

	primaryCursor, secondaryCursor := stream.Cursor()
	maxPrimaryCursorValue := m.state.GetCursor(stream.Self(), primaryCursor)
	maxSecondaryCursorValue := m.state.GetCursor(stream.Self(), secondaryCursor)

	var conditions bson.A

	if maxPrimaryCursorValue != nil {
		formattedPrimaryValue, err := abstract.ReformatCursorValue(primaryCursor, maxPrimaryCursorValue, stream)
		if err != nil {
			return nil, fmt.Errorf("failed to convert primary cursor value: %s", err)
		}
		condition := buildMongoCondition(types.Condition{
			Column:   primaryCursor,
			Value:    fmt.Sprintf("%v", formattedPrimaryValue),
			Operator: "<=",
		})
		conditions = append(conditions, condition)
	}

	if maxSecondaryCursorValue != nil && secondaryCursor != "" {
		formattedSecondaryValue, err := abstract.ReformatCursorValue(secondaryCursor, maxSecondaryCursorValue, stream)
		if err != nil {
			return nil, fmt.Errorf("failed to convert secondary cursor value: %s", err)
		}
		condition := buildMongoCondition(types.Condition{
			Column:   secondaryCursor,
			Value:    fmt.Sprintf("%v", formattedSecondaryValue),
			Operator: "<=",
		})
		conditions = append(conditions, condition)
	}

	return conditions, nil
}
