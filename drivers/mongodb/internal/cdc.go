package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

type CDCDocument struct {
	OperationType string         `json:"operationType"`
	FullDocument  map[string]any `json:"fullDocument"`
	DocumentKey   map[string]any `json:"documentKey"`
	ClusterTime   bson.Raw       `json:"clusterTime"`
	NS            struct {
		DB         string `json:"db"`
		Collection string `json:"coll"`
	} `json:"ns"`
}

// getCurrentResumeToken gets the current resume token for the change stream
func (m *Mongo) getCurrentResumeToken(ctx context.Context, collection *mongo.Collection, pipeline mongo.Pipeline) (*bson.Raw, error) {
	cursor, err := collection.Watch(ctx, pipeline, options.ChangeStream())
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer cursor.Close(ctx)

	resumeToken := cursor.ResumeToken()
	return &resumeToken, nil
}

func (m *Mongo) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	return utils.Concurrent(context.TODO(), streams, len(streams), func(ctx context.Context, stream protocol.Stream, executionNumber int) error {
		return m.changeStreamSync(stream, pool)
	})
}

func (m *Mongo) SetupGlobalState(state *types.State) error {
	return nil
}

func (m *Mongo) StateType() types.StateType {
	return types.StreamType
}

func (m *Mongo) changeStreamSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	cdcCtx := context.TODO()
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}

	prevResumeToken := stream.GetStateKey(cdcCursorField)
	if prevResumeToken == nil {
		resumeToken, err := m.getCurrentResumeToken(cdcCtx, collection, pipeline)
		if err != nil {
			return err
		}
		if resumeToken != nil {
			prevResumeToken = (*resumeToken).Lookup(cdcCursorField).StringValue()
		}
		if err := m.backfill(stream, pool); err != nil {
			return err
		}
		logger.Infof("backfill done for stream[%s]", stream.ID())
	}

	changeStreamOpts = changeStreamOpts.SetResumeAfter(map[string]any{cdcCursorField: prevResumeToken})
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), prevResumeToken)

	cursor, err := collection.Watch(cdcCtx, pipeline, changeStreamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(cdcCtx)

	insert, err := pool.NewThread(cdcCtx, stream)
	if err != nil {
		return err
	}
	defer insert.Close()

	for cursor.TryNext(cdcCtx) {
		var record CDCDocument
		if err := cursor.Decode(&record); err != nil {
			return fmt.Errorf("error while decoding: %s", err)
		}

		var documentToProcess map[string]any

		switch record.OperationType {
		case "delete":
			// Handle deleted documents by creating a special document
			documentToProcess = map[string]any{
				"_id":            record.DocumentKey["_id"],
				"cdc_type":       record.OperationType,
				"deleted_at":     time.Now().UTC().Format(time.RFC3339),
				"document_key":   record.DocumentKey,
				"namespace_db":   record.NS.DB,
				"namespace_coll": record.NS.Collection,
			}
		case "insert", "update":
			if record.FullDocument != nil {
				documentToProcess = record.FullDocument
				documentToProcess["cdc_type"] = record.OperationType
			}
		}

		if documentToProcess != nil {
			handleObjectID(documentToProcess)
			rawRecord := types.CreateRawRecord(
				utils.GetKeysHash(documentToProcess, constants.MongoPrimaryID),
				documentToProcess,
				0,
			)

			exit, err := insert.Insert(rawRecord)
			if err != nil {
				return err
			}
			if exit {
				return nil
			}
		}

		prevResumeToken = cursor.ResumeToken().Lookup(cdcCursorField).StringValue()
	}

	if err := cursor.Err(); err != nil {
		return fmt.Errorf("failed to iterate change streams cursor: %s", err)
	}

	stream.SetStateKey(cdcCursorField, prevResumeToken)
	return nil
}
