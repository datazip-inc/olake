package driver

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/ssh"
)

const (
	cdcCursorField = "_data"
)

type Mongo struct {
	config     *Config
	client     *mongo.Client
	CDCSupport bool // indicates if the MongoDB instance supports Change Streams
	cdcCursor  sync.Map
	state      *types.State // reference to globally present state
	streams    []types.StreamInterface
	sshDialer  *MongoSSHDialer
}

// MongoSSHDialer implements a custom dialer for SSH tunnel connections.
// The MongoDB Go driver doesn't support SSH tunneling natively, so we
// implement the Dialer interface to route connections through SSH tunnels
// for secure access to databases behind bastion hosts.
type MongoSSHDialer struct {
	sshClient *ssh.Client
}

func (d *MongoSSHDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.sshClient == nil {
		return nil, fmt.Errorf("SSH client is not initialized")
	}
	conn, err := d.sshClient.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return utils.ConnWithCustomDeadlineSupport(conn)
}

// config reference; must be pointer
func (m *Mongo) GetConfigRef() abstract.Config {
	m.config = &Config{}
	return m.config
}

func (m *Mongo) Spec() any {
	return Config{}
}

func (m *Mongo) CDCSupported() bool {
	return m.CDCSupport
}

func (m *Mongo) Setup(ctx context.Context) error {

	if m.config.SSHConfig != nil && m.config.SSHConfig.Host != "" {
		logger.Info("Found SSH Configuration")
		sshClient, err := m.config.SSHConfig.SetupSSHConnection()
		if err != nil {
			return fmt.Errorf("failed to setup SSH connection: %s", err)
		}
		m.sshDialer = &MongoSSHDialer{sshClient: sshClient}
	}

	opts := options.Client()

	opts.ApplyURI(m.config.URI())
	opts.SetCompressors([]string{"snappy"}) // using Snappy compression; read here https://en.wikipedia.org/wiki/Snappy_(compression)
	if m.sshDialer != nil {
		opts.SetDialer(m.sshDialer)
	}
	opts.SetMaxPoolSize(uint64(m.config.MaxThreads))
	connectCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	conn, err := mongo.Connect(connectCtx, opts)
	if err != nil {
		return err
	}

	// Validate the connection by pinging the database
	if err := conn.Ping(connectCtx, nil); err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %s", err)
	}

	m.client = conn
	// no need to check from discover if it have cdc support or not
	m.CDCSupport = true
	// check for default backoff count
	m.config.RetryCount = utils.Ternary(m.config.RetryCount == 0, 1, m.config.RetryCount+1).(int)
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	return m.client.Ping(pingCtx, options.Client().ReadPreference)
}

func (m *Mongo) Close(ctx context.Context) error {
	if m.client != nil {
		if err := m.client.Disconnect(ctx); err != nil {
			logger.Errorf("failed to disconnect from MongoDB: %s", err)
		}
	}

	if m.sshDialer != nil && m.sshDialer.sshClient != nil {
		if err := m.sshDialer.sshClient.Close(); err != nil {
			logger.Errorf("failed to close SSH client: %s", err)
		}
	}

	return nil
}

func (m *Mongo) Type() string {
	return string(constants.MongoDB)
}

func (m *Mongo) SetupState(state *types.State) {
	m.state = state
}

func (m *Mongo) MaxConnections() int {
	return m.config.MaxThreads
}

func (m *Mongo) MaxRetries() int {
	return m.config.RetryCount
}

func (m *Mongo) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for MongoDB database %s", m.config.Database)
	database := m.client.Database(m.config.Database)
	collections, err := database.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	var streamNames []string
	// Iterate through collections and check if they are views
	for collections.Next(ctx) {
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
	return streamNames, collections.Err()
}

func (m *Mongo) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	produceCollectionSchema := func(ctx context.Context, db *mongo.Database, streamName string) (*types.Stream, error) {
		logger.Infof("producing type schema for stream [%s]", streamName)

		// initialize stream
		collection := db.Collection(streamName)
		stream := types.NewStream(streamName, db.Name(), nil)
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

				filterMongoObject(row)
				if err := typeutils.Resolve(stream, row); err != nil {
					return err
				}
			}

			return cursor.Err()
		})
	}
	database := m.client.Database(m.config.Database)
	// Either wait for covering 100k records from both sides for all streams
	// Or wait till discoverCtx exits
	stream, err := produceCollectionSchema(ctx, database, streamName)
	if err != nil && ctx.Err() == nil { // if discoverCtx did not make an exit then throw an error
		return nil, fmt.Errorf("failed to process collection[%s]: %s", streamName, err)
	}
	// Add all discovered fields as potential cursor fields
	stream.Schema.Properties.Range(func(key, value interface{}) bool {
		if fieldName, ok := key.(string); ok {
			stream.WithCursorField(fieldName)
		}
		return true
	})

	stream.WithSyncMode(types.FULLREFRESH, types.INCREMENTAL)
	if m.CDCSupported() {
		stream.WithSyncMode(types.CDC, types.STRICTCDC)
	}

	return stream, err
}

func filterMongoObject(doc bson.M) {
	for key, value := range doc {
		// first make key small case as data being typeresolved with small case keys
		delete(doc, key)
		switch value := value.(type) {
		case primitive.Timestamp:
			doc[key] = value.T
		case primitive.DateTime:
			doc[key] = value.Time()
		case primitive.Null:
			doc[key] = nil
		case primitive.Binary:
			doc[key] = fmt.Sprintf("%x", value.Data)
		case primitive.Decimal128:
			doc[key] = value.String()
		case primitive.ObjectID:
			doc[key] = value.Hex()
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				doc[key] = nil
			} else {
				doc[key] = value
			}
		default:
			doc[key] = value
		}
	}
}
