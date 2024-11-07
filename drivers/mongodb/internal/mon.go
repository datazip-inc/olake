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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	database := m.client.Database(m.config.Database)
	collections, err := database.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	// Channel to collect results
	var streams []*types.Stream
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Iterate through collections and check if they are views
	for collections.Next(ctx) {
		var collectionInfo bson.M
		if err := collections.Decode(&collectionInfo); err != nil {
			return nil, fmt.Errorf("failed to decode collection ")
		}

		// Check if collection is a view
		if collectionType, ok := collectionInfo["type"].(string); ok && collectionType == "view" {
			continue
		}
		wg.Add(1)
		go func(colName string) {
			defer wg.Done()
			stream, err := m.produceCollectionSchema(context.TODO(), database, colName)
			if err != nil {
				logger.Errorf("failed to process collection[%s]: %s", colName, err)
				return
			}
			mu.Lock()
			streams = append(streams, stream)
			mu.Unlock()
		}(collectionInfo["name"].(string))
	}

	wg.Wait()

	return streams, nil
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

// fetch records from mongo and schema types
func (m *Mongo) produceCollectionSchema(ctx context.Context, db *mongo.Database, collectionName string) (*types.Stream, error) {
	collection := db.Collection(collectionName)
	stream := types.NewStream(collectionName, "")

	// default sync modes
	stream.WithSyncMode(types.CDC, types.FULLREFRESH)
	mongoIndexes, err := collection.Indexes().List(ctx, options.ListIndexes())
	if err != nil {
		return nil, err
	}

	for mongoIndexes.Next(ctx) {
		var indexes bson.M
		if err := mongoIndexes.Decode(&indexes); err != nil {
			return nil, err
		}
		for key, _ := range indexes["key"].(bson.M) {
			stream.WithPrimaryKey(key)
			stream.WithSyncMode(types.INCREMENTAL)
		}
	}
	// iterate over 1k records to get data types
	// currently only populating schema on level 1
	cursor, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(1000))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var row bson.M
		if err := cursor.Decode(&row); err != nil {
			return nil, err
		}

		if err := typeutils.Resolve(stream, row); err != nil {
			return nil, err
		}
	}

	m.SourceStreams[stream.ID()] = stream

	return stream, nil
}
