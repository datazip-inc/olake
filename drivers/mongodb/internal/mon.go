package driver

import (
	"context"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
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
	return nil
}

func (m *Mongo) Type() string {
	return "MongoDB"
}

func (m *Mongo) Discover() ([]*types.Stream, error) {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	// defer cancel()

	// dbResult, err := m.client.ListDatabases(ctx, nil, &options.ListDatabasesOptions{
	// 	AuthorizedDatabases: types.ToPtr(true),
	// })
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to list databases: %s", err)
	// }

	// streams := []types.Stream{}
	return nil, nil
}

func (m *Mongo) Read(stream protocol.Stream, channel chan<- types.Record) error {
	return nil
}

func (m *Mongo) BulkRead() bool {
	return true
}
