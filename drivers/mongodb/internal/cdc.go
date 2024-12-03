package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/piyushsingariya/relec"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
)

func (m *Mongo) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	// TODO: concurrency based on configuration
	changeStreamCtx, cancelChangeStream := context.WithCancel(context.TODO())
	defer cancelChangeStream()

	return relec.Concurrent(changeStreamCtx, streams, 2, func(ctx context.Context, stream protocol.Stream, executionNumber int) error {
		err := m.changeStreamSync(ctx, stream, pool)
		if err != nil {
			cancelChangeStream()
		}
		return err
	})
}

func (m *Mongo) SetupGlobalState(state *types.State) error {
	state.Type = m.StateType()
	// Setup raw state
	// m.cdcState = types.NewGlobalState()
	return nil
}

func (m *Mongo) StateType() types.StateType {
	return ""
}

// does full load on empty state
func (m *Mongo) changeStreamSync(ctx context.Context, stream protocol.Stream, pool *protocol.WriterPool) error {
	logger.Infof("starting change stream for stream [%s]", stream.ID())

	cdcCtx, cancelCDC := context.WithCancel(ctx)
	defer cancelCDC()

	collection := m.client.Database(stream.Namespace(), options.Database().SetReadConcern(readconcern.Majority())).Collection(stream.Name())
	changeStreamOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "delete"}}}},
		}}},
	}

	var prevResumeToken interface{}
	if prevResumeToken == nil {
		// get current resume token and do full load for stream
		resumeToken, err := m.getCurrentResumeToken(cdcCtx, collection, pipeline)
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
	changeStreamOpts = changeStreamOpts.SetResumeAfter(prevResumeToken)
	// resume cdc sync from prev resume token
	logger.Infof("Starting CDC sync for stream[%s] with resume token[%s]", stream.ID(), prevResumeToken)

	cursor, err := collection.Watch(cdcCtx, pipeline, changeStreamOpts)
	if err != nil {
		return fmt.Errorf("failed to open change stream: %s", err)
	}
	defer cursor.Close(cdcCtx)

	insert, err := pool.NewThread(context.TODO(), stream)
	if err != nil {
		return err
	}
	// Iterates over the cursor to print the change stream events
	for cursor.TryNext(cdcCtx) {
		var record bson.M
		if err := cursor.Decode(&record); err != nil {
			return fmt.Errorf("error while decoding: %s", err)
		}
		// Only send full document from record received (record -> fullDocument -> record)
		exit, err := insert(types.Record(record))
		if err != nil {
			return err
		}
		if exit {
			return nil
		}

		prevResumeToken = cursor.ResumeToken().String()
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("failed to iterate cursor on change streams: %s", err)
	}

	// save state for the current stream
	// stream.SetStateCursor(prevResumeToken)
	return nil
}

func (m *Mongo) getCurrentResumeToken(cdcCtx context.Context, collection *mongo.Collection, pipeline []bson.D) (*bson.Raw, error) {
	cursor, err := collection.Watch(cdcCtx, pipeline, options.ChangeStream())
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %w", err)
	}
	defer cursor.Close(cdcCtx)

	resumeToken := cursor.ResumeToken()
	return &resumeToken, nil
}
