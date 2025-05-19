package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
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

func (m *Mongo) RunChangeStream(ctx context.Context, pool *protocol.WriterPool, streams ...protocol.Stream) error {
	return utils.Concurrent(ctx, streams, len(streams), func(ctx context.Context, stream protocol.Stream, executionNumber int) error {
		return m.changeStreamSync(ctx, stream, pool)
	})
}

// does full load on empty state
func (m *Mongo) changeStreamSync(cdcCtx context.Context, stream protocol.Stream, pool *protocol.WriterPool) error {
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}

	prevResumeToken := m.State.GetCursor(stream.Self(), cdcCursorField)
	chunks := m.State.GetChunks(stream.Self())
	if prevResumeToken == nil || chunks == nil || chunks.Len() != 0 {
		// get current resume token and do full load for stream
		resumeToken, err := m.getCurrentResumeToken(cdcCtx, collection, pipeline)
		if err != nil {
			return err
		}
		if resumeToken != nil {
			prevResumeToken = (*resumeToken).Lookup(cdcCursorField).StringValue()
		}

		// save resume token
		m.State.SetCursor(stream.Self(), cdcCursorField, prevResumeToken)
	}

	// resume cdc sync from prev resume token
	changeStreamer := func(ctx context.Context, callback base.MessageProcessingFunc) error {
		changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

		changeStreamOpts = changeStreamOpts.SetResumeAfter(map[string]any{cdcCursorField: prevResumeToken})
		logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), prevResumeToken)

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
			change := base.CDCChange{
				Stream:    stream,
				Timestamp: typeutils.Time{Time: ts},
				Data:      record.FullDocument,
				Kind:      record.OperationType,
			}
			prevResumeToken = cursor.ResumeToken().Lookup(cdcCursorField).StringValue()
			callback(change)
		}
		return cursor.Err()
	}
	postCDC := func(ctx context.Context, noErr bool) error {
		if noErr {
			m.State.SetCursor(stream.Self(), cdcCursorField, prevResumeToken)
		}
		return nil
	}

	return m.Driver.RunChangeStream(cdcCtx, m, changeStreamer, postCDC, pool, stream)
}

func (m *Mongo) getCurrentResumeToken(cdcCtx context.Context, collection *mongo.Collection, pipeline []bson.D) (*bson.Raw, error) {
	cursor, err := collection.Watch(cdcCtx, pipeline, options.ChangeStream())
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer cursor.Close(cdcCtx)

	resumeToken := cursor.ResumeToken()
	return &resumeToken, nil
}
