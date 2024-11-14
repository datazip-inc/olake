package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/safego"
	"github.com/datazip-inc/olake/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

func (m *Mongo) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	return nil
}

func (m *Mongo) SetupGlobalState(state *types.State) error {
	return nil
}

func (m *Mongo) StateType() types.StateType {
	return ""
}

// does full load on empty state
func (m *Mongo) changeStreamSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	// get current resume token
	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Snapshot())).Collection(stream.Name())

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}

	prevResumeToken := stream.GetStateCursor()
	if prevResumeToken == nil {
		// get current resume token and do full load for stream
		resumeToken, err := m.getCurrentResumeToken(collection, pipeline, stream)
		if err != nil {
			return err
		}
		if resumeToken != nil {
			prevResumeToken = *resumeToken
		}
		if err := m.backfill(stream, pool); err != nil {
			return err
		}

	}

	// resume cdc sync from prev resume token
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.Name(), prevResumeToken)
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetResumeAfter(prevResumeToken).SetMaxAwaitTime(5 * time.Second)

	cursor, err := collection.Watch(context.TODO(), pipeline, changeStreamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(context.TODO())

	thread, err := pool.NewThread(context.TODO(), stream)
	if err != nil {
		return err
	}
	defer safego.Close(thread)
	// Iterates over the cursor to print the change stream events
	for cursor.TryNext(context.TODO()) {
		var record bson.M
		if err := cursor.Decode(&record); err != nil {
			return fmt.Errorf("error while decoding: %s", err)
		}

		if !safego.Insert(thread, types.Record(record)) {
			break
		}
		prevResumeToken = cursor.ResumeToken().String()
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("Failed to iterate cursor on change streams: %s", err)
	}

	// save state for the current stream
	stream.SetStateCursor(prevResumeToken)
	return nil
}

func (m *Mongo) getCurrentResumeToken(collection *mongo.Collection, pipeline []bson.D, stream protocol.Stream) (*bson.Raw, error) {
	cursor, err := collection.Watch(context.TODO(), pipeline, options.ChangeStream())
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %w", err)
	}
	defer cursor.Close(context.TODO())

	// Fetch the first change event to get the resume token
	if cursor.Next(context.TODO()) {
		resumeToken := cursor.ResumeToken()
		return &resumeToken, nil
	}

	return nil, fmt.Errorf("no resume token found")
}
