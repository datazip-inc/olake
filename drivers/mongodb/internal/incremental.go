package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PreIncremental initializes cursor state for incremental sync
func (m *Mongo) PreIncremental(ctx context.Context, streams ...types.StreamInterface) error {
	for _, stream := range streams {
		cursorField := stream.Cursor()
		if cursorField == "" {
			return fmt.Errorf("cursor field is required for incremental sync")
		}

		prevCursorValue := m.state.GetCursor(stream.Self(), cursorField)
		m.incrementalCursor.Store(stream.ID(), prevCursorValue)
		logger.Infof("PreIncremental: stored cursor value for stream=%s, value=%v", stream.ID(), prevCursorValue)
	}
	return nil
}

// PostIncremental updates state with cursor value
func (m *Mongo) PostIncremental(ctx context.Context, stream types.StreamInterface, success bool) error {
	if success {
		cursorField := stream.Cursor()
		val, ok := m.incrementalCursor.Load(stream.ID())
		if ok && val != nil {
			m.state.SetCursor(stream.Self(), cursorField, val)
			logger.Infof("PostIncremental: updated state cursor for stream[%s] to %v", stream.ID(), val)
		} else {
			logger.Debugf("PostIncremental: no new cursor value found for stream: %s", stream.ID())
		}
	}
	return nil
}

func (m *Mongo) IncrementalIterator(ctx context.Context, stream types.StreamInterface, lastCursorValue interface{}, processFn abstract.BackfillMsgFn) error {
	cursorField := stream.Cursor()
	collection := m.client.Database(stream.Namespace()).Collection(stream.Name())

	filter := bson.M{}
	if lastCursorValue != nil {
		switch v := lastCursorValue.(type) {
		case string:
			if cursorField == "_id" {
				oid, err := primitive.ObjectIDFromHex(v)
				if err != nil {
					return fmt.Errorf("invalid _id in state: %w", err)
				}
				filter[cursorField] = bson.M{"$gt": oid}
			} else {
				filter[cursorField] = bson.M{"$gt": v}
			}
		case primitive.ObjectID:
			filter[cursorField] = bson.M{"$gt": v}
		default:
			return fmt.Errorf("invalid type for %s cursor: %T", cursorField, v)
		}
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: cursorField, Value: 1}}).
		SetBatchSize(int32(m.config.BatchSize)).
		SetMaxTime(30 * time.Second)

	if cursorField != "_id" {
		if idxCur, err := collection.Indexes().List(ctx); err == nil {
			defer idxCur.Close(ctx)
			for idxCur.Next(ctx) {
				var idx bson.M
				if idxCur.Decode(&idx) == nil {
					if keys, ok := idx["key"].(bson.M); ok {
						if _, ok := keys[cursorField]; ok {
							findOpts.SetHint(bson.D{{Key: cursorField, Value: 1}})
							break
						}
					}
				}
			}
		}
	}

	logger.Infof("Starting incremental sync for stream[%s] with cursor field[%s], last value[%v]", stream.ID(), cursorField, lastCursorValue)
	logger.Debugf("Final Mongo filter for stream[%s]: %+v", stream.ID(), filter)

	cursor, err := collection.Find(ctx, filter, findOpts)
	if err != nil {
		return fmt.Errorf("failed to execute incremental query: %w", err)
	}
	defer cursor.Close(ctx)

	var maxCursor interface{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decode error: %w", err)
		}
		filterMongoObject(doc)

		// Track max cursor value
		if val, ok := doc[cursorField]; ok {
			if maxCursor == nil || utils.CompareInterfaceValue(val, maxCursor) > 0 {
				maxCursor = val
			}
		}

		if err := processFn(doc); err != nil {
			return fmt.Errorf("process error: %w", err)
		}
	}

	if maxCursor != nil {
		m.incrementalCursor.Store(stream.ID(), maxCursor)
		logger.Infof("Updated incremental cursor for stream[%s] to %v", stream.ID(), maxCursor)
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	return nil
}

func (m *Mongo) IncrementalChanges(
	ctx context.Context,
	stream types.StreamInterface,
	cb abstract.BackfillMsgFn,
) error {
	cursorField := stream.Cursor()
	if cursorField == "" {
		return fmt.Errorf("cursor field is required for incremental sync")
	}

	lastCursorValue := m.state.GetCursor(stream.Self(), cursorField)
	logger.Infof("IncrementalChanges: stream=%s, lastCursorValue=%v", stream.ID(), lastCursorValue)
	return m.IncrementalIterator(ctx, stream, lastCursorValue, cb)
}
