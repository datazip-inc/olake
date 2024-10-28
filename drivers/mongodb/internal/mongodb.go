package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/colake/types"
	"github.com/datazip-inc/colake/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
	config *Config
	client *mongo.Client
}

func (m *MongoDB) Config() *Config {
	if m.config == nil {
		m.config = &Config{}
	}

	return m.config
}

func (m *MongoDB) Setup() error {
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

func (m *MongoDB) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *MongoDB) Discover() ([]types.BaseStream, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbResult, err := m.client.ListDatabases(ctx, nil, &options.ListDatabasesOptions{
		AuthorizedDatabases: utils.Pointer(true),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %s", err)
	}

	streams := []types.BaseStream{}

}
