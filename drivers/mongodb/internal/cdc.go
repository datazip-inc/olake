package driver

import (
	"context"
	"fmt"
	"time"

	abstract "github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

type CDCDocument struct {
	OperationType string              `json:"operationType"`
	FullDocument  map[string]any      `json:"fullDocument"`
	ClusterTime   primitive.Timestamp `json:"clusterTime"`
	WallTime      primitive.DateTime  `json:"wallTime"`
	DocumentKey   map[string]any      `json:"documentKey"`
}

func (m *Mongo) PreCDC(cdcCtx context.Context, state *types.State, streams []types.StreamInterface) error {
	for _, stream := range streams {
		collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
		pipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
			}}},
		}

		prevResumeToken := state.GetCursor(stream.Self(), cdcCursorField)
		if prevResumeToken == nil {
			// get current resume token and do full load for stream
			resumeToken, err := m.getCurrentResumeToken(cdcCtx, collection, pipeline)
			if err != nil {
				return err
			}
			if resumeToken != nil {
				prevResumeToken = (*resumeToken).Lookup(cdcCursorField).StringValue()
				state.SetCursor(stream.Self(), cdcCursorField, prevResumeToken)
			}
		}
		m.cdcCursor = prevResumeToken
	}
	return nil
}

func (m *Mongo) StreamChanges(ctx context.Context, stream types.StreamInterface, OnMessage abstract.CDCMsgFn) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())

	changeStreamOpts = changeStreamOpts.SetResumeAfter(map[string]any{cdcCursorField: m.cdcCursor})
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), m.cdcCursor)

	cursor, err := collection.Watch(ctx, pipeline, changeStreamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(ctx)

	for cursor.TryNext(ctx) {
		var record CDCDocument
		if err := cursor.Decode(&record); err != nil {
			return fmt.Errorf("error while decoding: %s", err)
		}

		if record.OperationType == "delete" {
			// replace full document(null) with documentKey
			record.FullDocument = record.DocumentKey
		}
		filterMongoObject(record.FullDocument)
		ts := utils.Ternary(record.WallTime != 0,
			record.WallTime.Time(), // millisecond precision
			time.UnixMilli(int64(record.ClusterTime.T)*1000+int64(record.ClusterTime.I)), // seconds only
		).(time.Time)
		change := abstract.CDCChange{
			Stream:    stream,
			Timestamp: typeutils.Time{Time: ts},
			Data:      record.FullDocument,
			Kind:      record.OperationType,
		}
		m.cdcCursor = cursor.ResumeToken().Lookup(cdcCursorField).StringValue()
		if err := OnMessage(change); err != nil {
			return fmt.Errorf("failed to process message: %s", err)
		}
	}
	return cursor.Err()
}

func (m *Mongo) PostCDC(ctx context.Context, state *types.State, stream types.StreamInterface, noErr bool) error {
	if noErr {
		state.SetCursor(stream.Self(), cdcCursorField, m.cdcCursor)
	}
	return nil
}

func (m *Mongo) getCurrentResumeToken(cdcCtx context.Context, collection *mongo.Collection, pipeline []bson.D) (*bson.Raw, error) {
	cursor, err := collection.Watch(cdcCtx, pipeline, options.ChangeStream())
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(cdcCtx)

	resumeToken := cursor.ResumeToken()
	return &resumeToken, nil
}
