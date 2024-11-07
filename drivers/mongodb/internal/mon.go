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
	conn, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return err
	}

	m.client = conn
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	return conn.Ping(pingCtx, opts.ReadPreference)
}

func (m *Mongo) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *Mongo) Setup() error {
	if err := m.Check(); err != nil {
		return err
	}

	return m.loadStreams()
}

func (m *Mongo) Type() string {
	return "MongoDB"
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
			stream, err := produceCollectionSchema(database, colName)
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

func (m *Mongo) Read(stream protocol.Stream, channel chan<- types.Record) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return m.backfill(stream, channel)
	case types.CDC:
		return m.changeStreamSync(stream, channel)
	}

	return nil
}

func (m *Mongo) BulkRead() bool {
	return true
}

// fetch records from mongo and schema types
func produceCollectionSchema(db *mongo.Database, collectionName string) (*types.Stream, error) {
	collection := db.Collection(collectionName)

	return schema, nil
}
