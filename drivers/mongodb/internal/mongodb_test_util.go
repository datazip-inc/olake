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
		Hosts: []string{
			"4.187.148.120:27017",
			"4.187.150.133:27017",
			"4.187.151.102:27017",
			"4.224.66.22:27017",
		},
		Username:    "CoXgxxldCfW0U94t73Ai",
		Password:    "T74QLVAHW7OOwvMplrr2wjY7APrGrfIDOdqT4o3vlYfJrYwzVL",
		AuthDB:      "admin",
		Database:    "Duke_DB2",
		DefaultMode: types.CDC,
		MaxThreads:  10,
		RetryCount:  3,
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
func createTestCollection(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string) {
	// MongoDB creates collections automatically when documents are inserted,
	// but we'll explicitly create it with specific options if needed
	err := client.Database(dbName).CreateCollection(ctx, collName)
	if err != nil {
		// Ignore "collection already exists" error
		if !mongo.IsDuplicateKeyError(err) {
			t.Logf("Note: Collection might already exist: %v", err)
		}
	}
}

func dropTestCollection(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string) {
	err := client.Database(dbName).Collection(collName).Drop(ctx)
	require.NoError(t, err, "Failed to drop test collection")
}

func cleanTestCollection(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string) {
	_, err := client.Database(dbName).Collection(collName).DeleteMany(ctx, bson.M{})
	require.NoError(t, err, "Failed to clean test collection")
}

func addTestDocuments(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string, numDocs int, startAtDoc int) {
	coll := client.Database(dbName).Collection(collName)

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
}

func insertDocOp(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string) {
	coll := client.Database(dbName).Collection(collName)

	newDoc := bson.M{
		"field1":    "new_value",
		"field2":    "new_test",
		"number":    1000,
		"timestamp": time.Now().UnixMilli(),
	}

	_, err := coll.InsertOne(ctx, newDoc)
	require.NoError(t, err, "Failed to insert document in CDC test")
}

func updateDocOp(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string) {
	coll := client.Database(dbName).Collection(collName)

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

func deleteDocOp(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string) {
	coll := client.Database(dbName).Collection(collName)

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
