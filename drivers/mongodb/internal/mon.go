package driver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/piyushsingariya/relec"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	*base.Driver
	config *Config
	client *mongo.Client
}

// config reference; must be pointer
func (m *Mongo) GetConfigRef() any {
	m.config = &Config{}
	return m.config
}

func (m *Mongo) Spec() any {
	return Config{}
}

func (m *Mongo) Check() error {
	opts := options.Client()
	opts.ApplyURI(m.config.URI())
	opts.SetCompressors([]string{"snappy"}) // using Snappy compression; read here https://en.wikipedia.org/wiki/Snappy_(compression)
	opts.SetMaxPoolSize(1000)

	connectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := mongo.Connect(connectCtx, opts)
	if err != nil {
		return err
	}

	m.client = conn
	return conn.Ping(connectCtx, opts.ReadPreference)
}

func (m *Mongo) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *Mongo) Type() string {
	return "Mongo"
}

func (m *Mongo) Discover() ([]*types.Stream, error) {
	logger.Infof("Starting discover for MongoDB database %s", m.config.Database)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// TODO: Check must run before discover command
	if m.client == nil {
		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(m.config.URI()))
		if err != nil {
			return nil, err
		}

		m.client = client
	}

	database := m.client.Database(m.config.Database)
	if m == nil || m.SourceStreams == nil {
		fmt.Println("m is nil received")
	}
	collections, err := database.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	// Channel to collect results
	var streams []*types.Stream
	var streamLock sync.Mutex
	var streamNames []string
	// Iterate through collections and check if they are views
	for collections.Next(ctx) {
		var collectionInfo bson.M
		if err := collections.Decode(&collectionInfo); err != nil {
			return nil, fmt.Errorf("failed to decode collection: %s", err)
		}

		// Check if collection is a view
		if collectionType, ok := collectionInfo["type"].(string); ok && collectionType == "view" {
			continue
		}
		streamNames = append(streamNames, collectionInfo["name"].(string))
	}
	return streams, relec.Concurrent(context.TODO(), streamNames, len(streamNames), func(ctx context.Context, streamName string, execNumber int) error {
		stream, err := m.produceCollectionSchema(context.TODO(), database, streamName)
		if err != nil {
			return fmt.Errorf("failed to process collection[%s]: %s", streamName, err)
		}

		// cache stream
		m.SourceStreams[stream.ID()] = stream

		streamLock.Lock()
		streams = append(streams, stream)
		streamLock.Unlock()
		return nil
	})
}

func (m *Mongo) Read(pool *protocol.WriterPool, stream protocol.Stream) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return m.backfill(stream, pool)
	case types.CDC:
		return m.changeStreamSync(stream, pool)
	}

	return nil
}

// fetch schema types from mongo for streamName
func (m *Mongo) produceCollectionSchema(ctx context.Context, db *mongo.Database, streamName string) (*types.Stream, error) {
	logger.Infof("Producing catalog schema for stream [%s]", streamName)

	// Initialize stream
	collection := db.Collection(streamName)
	stream := types.NewStream(streamName, "")
	stream.WithSyncMode(types.CDC, types.FULLREFRESH)

	indexesCursor, err := collection.Indexes().List(ctx, options.ListIndexes())
	if err != nil {
		return nil, err
	}
	defer indexesCursor.Close(ctx)

	for indexesCursor.Next(ctx) {
		var indexes bson.M
		if err := indexesCursor.Decode(&indexes); err != nil {
			return nil, err
		}
		for key := range indexes["key"].(bson.M) {
			stream.WithPrimaryKey(key)
		}
	}

	// Define find options for fetching documents in ascending and descending order.
	findOpts := []*options.FindOptions{
		options.Find().SetLimit(100000).SetSort(bson.D{{Key: "$natural", Value: 1}}),
		options.Find().SetLimit(100000).SetSort(bson.D{{Key: "$natural", Value: -1}}),
	}

	return stream, relec.Concurrent(ctx, findOpts, len(findOpts), func(ctx context.Context, findOpt *options.FindOptions, execNumber int) error {
		cursor, err := collection.Find(ctx, bson.D{}, findOpt)
		if err != nil {
			return err
		}
		defer cursor.Close(ctx)

		for cursor.Next(ctx) {
			var row bson.M
			if err := cursor.Decode(&row); err != nil {
				return err
			}

			if err := typeutils.Resolve(stream, row); err != nil {
				return err
			}
		}
		return nil
	})
}
