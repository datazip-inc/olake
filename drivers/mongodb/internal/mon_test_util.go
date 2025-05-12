package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Test Client Setup
func testMongoClient(t *testing.T) (*mongo.Client, Config, *Mongo) {
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

	// Properly initialize State
	d.CDCSupport = true
	state := types.NewState(types.GlobalType)
	d.SetupState(state)

	_ = protocol.ChangeStreamDriver(d)
	err := d.Setup()
	require.NoError(t, err)

	return d.client, *d.config, d
}

// MongoDB-specific helpers
func createTestCollection(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	client := conn.(*mongo.Client)
	// Expilicitly creating the collection
	err := client.Database("olake").CreateCollection(ctx, tableName)
	if err != nil {
		// Ignore "collection already exists" error
		if !mongo.IsDuplicateKeyError(err) {
			t.Logf("Note: Collection might already exist: %v", err)
		}
	}
	t.Logf("Collection created")
}

func dropTestCollection(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	client := conn.(*mongo.Client)
	err := client.Database("olake").Collection(tableName).Drop(ctx)
	require.NoError(t, err, "Failed to drop test collection")
}

func cleanTestCollection(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	client := conn.(*mongo.Client)
	_, err := client.Database("olake").Collection(tableName).DeleteMany(ctx, bson.M{})
	require.NoError(t, err, "Failed to clean test collection")
}

func addTestDocuments(ctx context.Context, t *testing.T, conn interface{}, tableName string, numDocs int, startAtDoc int) {
	client := conn.(*mongo.Client)
	coll := client.Database("olake").Collection(tableName)

	for idx := startAtDoc; idx < startAtDoc+numDocs; idx++ {
		doc := bson.M{
			"field1":    fmt.Sprintf("value_%d", idx),
			"field2":    fmt.Sprintf("test_%d", idx),
			"number":    idx * 10,
			"timestamp": time.Now().UnixMilli(),
		}

		_, err := coll.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert test document")
	}

	// Print the collection after data insertion
	cursor, err := coll.Find(ctx, bson.M{})
	require.NoError(t, err, "Failed to retrieve documents from collection")
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var doc bson.M
		err := cursor.Decode(&doc)
		require.NoError(t, err, "Failed to decode document")
		t.Logf("%v", doc)
	}
	require.NoError(t, cursor.Err(), "Cursor encountered an error")
}

func insertDocOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	client := conn.(*mongo.Client)
	coll := client.Database("olake").Collection(tableName)

	newDoc := bson.M{
		"field1":    "new_value",
		"field2":    "new_test",
		"number":    1000,
		"timestamp": time.Now().UnixMilli(),
	}

	_, err := coll.InsertOne(ctx, newDoc)
	require.NoError(t, err, "Failed to insert document in CDC test")
}

func updateDocOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	client := conn.(*mongo.Client)
	coll := client.Database("olake").Collection(tableName)

	// Find a random document to update
	findOptions := options.Find().SetLimit(1)
	cursor, err := coll.Find(ctx, bson.M{}, findOptions)
	require.NoError(t, err, "Failed to find document for update")
	defer cursor.Close(ctx)

	var doc bson.M
	if cursor.Next(ctx) {
		err = cursor.Decode(&doc)
		require.NoError(t, err, "Failed to decode document")

		docID := doc["_id"]

		update := bson.M{
			"$set": bson.M{
				"field1":    "updated_value",
				"timestamp": time.Now().UnixMilli(),
			},
		}

		_, err = coll.UpdateOne(ctx, bson.M{"_id": docID}, update)
		require.NoError(t, err, "Failed to update document in CDC test")
	} else {
		t.Log("No documents found to update")
	}
}

func deleteDocOp(ctx context.Context, t *testing.T, conn interface{}, tableName string) {
	client := conn.(*mongo.Client)
	coll := client.Database("olake").Collection(tableName)

	// Find a random document to delete
	findOptions := options.Find().SetLimit(1)
	cursor, err := coll.Find(ctx, bson.M{}, findOptions)
	require.NoError(t, err, "Failed to find document for deletion")
	defer cursor.Close(ctx)

	var doc bson.M
	if cursor.Next(ctx) {
		err = cursor.Decode(&doc)
		require.NoError(t, err, "Failed to decode document")

		docID := doc["_id"]

		_, err = coll.DeleteOne(ctx, bson.M{"_id": docID})
		require.NoError(t, err, "Failed to delete document in CDC test")
	} else {
		t.Log("No documents found to delete")
	}
}
