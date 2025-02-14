package driver

import (
	"context"
	"fmt"
	// "os"
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
	"time"
)

func testClient(t *testing.T) (*mongo.Client, Config) {
	t.Helper()
	config := Config{
		Hosts:      []string{"localhost:27017"},
		Username:   "olake",
		Password:   "olake",
		Database:   "olake",
		AuthDB:     "admin",
		ReplicaSet: "rs0",
		Srv:        false,
	}

	d := &Mongo{
		Driver: base.NewBase(),
		config: &config,
	}
	_ = protocol.ChangeStreamDriver(d)
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config
}

func addTestTableData(
	ctx context.Context,
	t *testing.T,
	c *mongo.Client,
	database string,
	collection string,
	numItems int,
	startAtItem int,
	cols ...string,
) {
	var db = c.Database(database)
	var col = db.Collection(collection)

	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		item := make(map[string]any)

		for _, col := range cols {
			item[col] = map[string]interface{}{
				"str":         fmt.Sprintf("%s val %d", col, idx),
				"array":       []string{"a", "b", "c"},
				"int":         1,
				"float":       1.23,
				"date-normal": time.Date(2023, 11, 7, 16, 57, 0, 0, time.UTC),
			}
		}

		log.WithField("data", item).WithField("col", collection).Debug("inserting data")
		_, err := col.InsertOne(ctx, item)
		require.NoError(t, err)
	}
}

func deleteData(
	ctx context.Context,
	t *testing.T,
	c *mongo.Client,
	database string,
	collection string,
	id any,
) {
	var db = c.Database(database)
	var col = db.Collection(collection)

	_, err := col.DeleteOne(ctx, map[string]any{"_id": id})

	require.NoError(t, err)
}

func updateData(
	ctx context.Context,
	t *testing.T,
	c *mongo.Client,
	database string,
	collection string,
	id any,
	cols ...string,
) {
	var db = c.Database(database)
	var col = db.Collection(collection)

	var item = make(map[string]any)
	for _, col := range cols {
		item[col] = fmt.Sprintf("%s val %d", col, 0)
	}

	_, err := col.UpdateOne(ctx, map[string]any{"_id": id}, map[string]any{"$set": item})

	require.NoError(t, err)
}
