package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	discoverTime        = 5 * time.Minute // maximum time allowed to discover all the streams
	cdcCursorField      = "_data"
	defaultBackoffCount = 3
)

type Mongo struct {
	*base.Driver
	config *Config
	client *mongo.Client
}

// config reference; must be pointer
func (m *Mongo) GetConfigRef() protocol.Config {
	m.config = &Config{}
	return m.config
}

func (m *Mongo) Spec() any {
	return Config{}
}

func (m *Mongo) Setup() error {
	opts := options.Client()
	opts.ApplyURI(m.config.URI())
	opts.SetCompressors([]string{"snappy"}) // using Snappy compression; read here https://en.wikipedia.org/wiki/Snappy_(compression)
	opts.SetMaxPoolSize(1000)

	connectCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	conn, err := mongo.Connect(connectCtx, opts)
	if err != nil {
		return err
	}

	m.client = conn
	// no need to check from discover if it have cdc support or not
	m.CDCSupport = true
	// check for default backoff count
	if m.config.RetryCount < 0 {
		logger.Info("setting backoff retry count to default value %d", defaultBackoffCount)
		m.config.RetryCount = defaultBackoffCount
	} else {
		// add 1 for first run
		m.config.RetryCount += 1
	}
	return nil
}

func (m *Mongo) Check() error {
	pingCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	return m.client.Ping(pingCtx, options.Client().ReadPreference)
}

func (m *Mongo) SetupState(state *types.State) {
	state.Type = m.StateType()
	m.State = state
}

func (m *Mongo) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *Mongo) Type() string {
	return "Mongo"
}

// TODO: utilize discoverSchema boolean
func (m *Mongo) Discover(discoverSchema bool) ([]*types.Stream, error) {
	streams := m.GetStreams()
	if len(streams) != 0 {
		return streams, nil
	}

	logger.Infof("Starting discover for MongoDB database %s", m.config.Database)
	discoverCtx, cancel := context.WithTimeout(context.Background(), discoverTime)
	defer cancel()

	database := m.client.Database(m.config.Database)
	collections, err := database.ListCollections(discoverCtx, bson.M{})
	if err != nil {
		return nil, err
	}

	var streamNames []string
	// Iterate through collections and check if they are views
	for collections.Next(discoverCtx) {
		var collectionInfo bson.M
		if err := collections.Decode(&collectionInfo); err != nil {
			return nil, fmt.Errorf("failed to decode collection: %s", err)
		}

		// Skip if collection is a view
		if collectionType, ok := collectionInfo["type"].(string); ok && collectionType == "view" {
			continue
		}
		streamNames = append(streamNames, collectionInfo["name"].(string))
	}
	// Either wait for covering 100k records from both sides for all streams
	// Or wait till discoverCtx exits
	err = utils.Concurrent(discoverCtx, streamNames, len(streamNames), func(ctx context.Context, streamName string, _ int) error {
		stream, err := m.produceCollectionSchema(discoverCtx, database, streamName)
		if err != nil && discoverCtx.Err() == nil { // if discoverCtx did not make an exit then throw an error
			return fmt.Errorf("failed to process collection[%s]: %s", streamName, err)
		}
		stream.SyncMode = m.config.DefaultMode
		// cache stream
		m.AddStream(stream)
		return err
	})
	if err != nil {
		return nil, err
	}

	return m.GetStreams(), nil
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
	logger.Infof("producing type schema for stream [%s]", streamName)

	// initialize stream
	collection := db.Collection(streamName)
	stream := types.NewStream(streamName, db.Name()).WithSyncMode(types.FULLREFRESH, types.CDC)

	// find primary keys
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
		options.Find().SetLimit(10000).SetSort(bson.D{{Key: "$natural", Value: 1}}),
		options.Find().SetLimit(10000).SetSort(bson.D{{Key: "$natural", Value: -1}}),
	}

	return stream, utils.Concurrent(ctx, findOpts, len(findOpts), func(ctx context.Context, findOpt *options.FindOptions, execNumber int) error {
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

			handleMongoObject(row)
			if err := typeutils.Resolve(stream, row); err != nil {
				return err
			}
		}

		return cursor.Err()
	})
}
