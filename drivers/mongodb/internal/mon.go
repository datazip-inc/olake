package driver

import (
    "context"
    "fmt"
    "io"
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
)

const (
    cdcCursorField = "_data"
)

type Mongo struct {
    config        *Config
    client        *mongo.Client
    CDCSupport    bool
    cdcCursor     sync.Map
    state         *types.State
    LastOplogTime primitive.Timestamp
}

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
    connectCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
    defer cancel()

    var forwardedHost string
    var forwardedPort int

    if m.config.SSHEnabled {
        sshClient, err := (&utils.SSHConfig{
            Host:       m.config.SSHHost,
            Port:       m.config.SSHPort,
            Username:   m.config.SSHUser,
            PrivateKey: m.config.SSHPrivateKey,
        }).SetupSSHConnection()
        if err != nil {
            return fmt.Errorf("failed to establish SSH connection: %w", err)
        }

        localListener, err := net.Listen("tcp", "localhost:0")
        if err != nil {
            return fmt.Errorf("failed to create local listener: %w", err)
        }
        localAddr := localListener.Addr().(*net.TCPAddr)
        forwardedHost = "localhost"
        forwardedPort = localAddr.Port

        go func() {
            for {
                localConn, err := localListener.Accept()
                if err != nil {
                    continue
                }
                remoteConn, err := sshClient.Dial("tcp", fmt.Sprintf("%s:%d", m.config.Hosts[0], 27017))
                if err != nil {
                    localConn.Close()
                    continue
                }
                go proxy(localConn, remoteConn)
            }
        }()
    } else {
        forwardedHost = m.config.Hosts[0]
        forwardedPort = 27017
    }

    uri := fmt.Sprintf("mongodb://%s:%d", forwardedHost, forwardedPort)
    opts := options.Client().ApplyURI(uri)
    opts.SetCompressors([]string{"snappy"})
    opts.SetMaxPoolSize(uint64(m.config.MaxThreads))

    conn, err := mongo.Connect(connectCtx, opts)
    if err != nil {
        return err
    }

    if err := conn.Ping(connectCtx, nil); err != nil {
        return fmt.Errorf("failed to connect to MongoDB: %s", err)
    }

    m.client = conn
    m.CDCSupport = true
    m.config.RetryCount = utils.Ternary(m.config.RetryCount == 0, 1, m.config.RetryCount+1).(int)

    pingCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
    defer cancel()

    return m.client.Ping(pingCtx, options.Client().ReadPreference)
}

func proxy(local, remote net.Conn) {
    go io.Copy(local, remote)
    go io.Copy(remote, local)
}

func (m *Mongo) Close(ctx context.Context) error {
    return m.client.Disconnect(ctx)
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
    for collections.Next(ctx) {
        var collectionInfo bson.M
        if err := collections.Decode(&collectionInfo); err != nil {
            return nil, fmt.Errorf("failed to decode collection: %s", err)
        }
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
        collection := db.Collection(streamName)
        stream := types.NewStream(streamName, db.Name(), nil)

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
    stream, err := produceCollectionSchema(ctx, database, streamName)
    if err != nil && ctx.Err() == nil {
        return nil, fmt.Errorf("failed to process collection[%s]: %s", streamName, err)
    }

    stream.Schema.Properties.Range(func(key, value interface{}) bool {
        if fieldName, ok := key.(string); ok {
            stream.WithCursorField(fieldName)
        }
        return true
    })
    return stream, err
}

func filterMongoObject(doc bson.M) {
    for key, value := range doc {
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
