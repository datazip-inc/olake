package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDB connection constants
const (
	MongoDBPort       = 27017
	MongoDBDatabase   = "olake_mongodb_test"
	MongoDBReplicaSet = "rs0"
	MongoDBAdminUser  = "admin"
	MongoDBAdminPass  = "password"
)

var (
	nestedDoc = bson.M{
		"nested_string": "nested_value",
		"nested_int":    42,
	}
)

// ExecuteQuery executes MongoDB operations for testing based on the operation type
func ExecuteQuery(ctx context.Context, t *testing.T, collectionName string, operation string) {
	t.Helper()

	// Connect to MongoDB
	mongoURI := fmt.Sprintf("mongodb://%s:%s@localhost:%d/admin?replicaSet=%s&directConnection=true",
		MongoDBAdminUser, MongoDBAdminPass, MongoDBPort, MongoDBReplicaSet)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err, "Failed to connect to MongoDB replica set at localhost:%d", MongoDBPort)
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("warning: failed to disconnect from MongoDB: %v", err)
		}
	}()

	db := client.Database(MongoDBDatabase)
	collection := db.Collection(collectionName)

	switch operation {
	case "create":
		// Create collection by inserting a dummy document and then deleting it
		dummyDoc := bson.M{"_dummy": "create_collection"}
		_, err := collection.InsertOne(ctx, dummyDoc)
		require.NoError(t, err, "Failed to create collection")
		_, err = collection.DeleteOne(ctx, bson.M{"_dummy": "create_collection"})
		require.NoError(t, err, "Failed to clean up dummy document")

	case "drop":
		err := collection.Drop(ctx)
		require.NoError(t, err, "Failed to drop collection")

	case "clean":
		_, err := collection.DeleteMany(ctx, bson.M{})
		require.NoError(t, err, "Failed to clean collection")

	case "add":
		insertTestData(t, ctx, collection)
		return

	case "insert":
		// Insert the same data as the add operation
		doc := bson.M{
			"id_bigint":         int64(123456789012345),
			"id_int":            int32(100),
			"id_timestamp":      time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"id_decimal":        primitive.NewDecimal128(12345, 67890),
			"id_double":         float64(123.456),
			"id_bool":           true,
			"created_timestamp": primitive.Timestamp{T: uint32(1754905992), I: 1},
			"id_nil":            nil,
			"id_regex":          primitive.Regex{Pattern: "test.*", Options: "i"},
			"id_nested":         nestedDoc,
			"id_minkey":         primitive.MinKey{},
			"id_maxkey":         primitive.MaxKey{},
			"name_varchar":      "varchar_val",
		}
		_, err := collection.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert document")

	case "update":
		filter := bson.M{"id_int": int32(100)}
		update := bson.M{
			"$set": bson.M{
				"id_bigint":         int64(987654321098765),
				"id_int":            int32(200),
				"id_timestamp":      time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC),
				"id_decimal":        primitive.NewDecimal128(54321, 98765),
				"id_double":         float64(202.456),
				"id_bool":           false,
				"created_timestamp": primitive.Timestamp{T: uint32(1754905699), I: 1},
				"id_nil":            nil,
				"id_regex":          primitive.Regex{Pattern: "updated.*", Options: "i"},
				"id_nested":         nestedDoc,
				"id_minkey":         primitive.MinKey{},
				"id_maxkey":         primitive.MaxKey{},
				"name_varchar":      "updated varchar",
			},
		}
		_, err := collection.UpdateOne(ctx, filter, update)
		require.NoError(t, err, "Failed to update document")

	case "delete":
		filter := bson.M{"id_int": 200}
		_, err := collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document")

	default:
		t.Fatalf("Unknown operation: %s", operation)
	}
}

func insertTestData(t *testing.T, ctx context.Context, collection *mongo.Collection) {
	testData := []bson.M{
		{
			"id_bigint":         int64(123456789012345),
			"id_int":            int32(100),
			"id_timestamp":      time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"id_decimal":        primitive.NewDecimal128(12345, 67890),
			"id_double":         float64(123.456),
			"id_bool":           true,
			"created_timestamp": primitive.Timestamp{T: uint32(1754905992), I: 1},
			"id_nil":            nil,
			"id_regex":          primitive.Regex{Pattern: "test.*", Options: "i"},
			"id_nested":         nestedDoc,
			"id_minkey":         primitive.MinKey{},
			"id_maxkey":         primitive.MaxKey{},
			"name_varchar":      "varchar_val",
		},
	}

	for i, doc := range testData {
		_, err := collection.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

var ExpectedMongoData = map[string]interface{}{
	"id_bigint":         int64(123456789012345),
	"id_int":            int32(100),
	"id_timestamp":      arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_decimal":        "12345.67890",
	"id_double":         float64(123.456),
	"id_bool":           true,
	"created_timestamp": int32(1754905992),
	"id_regex":          `{"pattern": "test.*", "options": "i"}`,
	"id_nested":         `{"nested_int":42,"nested_string":"nested_value"}`,
	"id_minkey":         `{}`,
	"id_maxkey":         `{}`,
	"name_varchar":      "varchar_val",
}

var ExpectedUpdatedMongoData = map[string]interface{}{
	"id_bigint":         int64(987654321098765),
	"id_int":            int32(200),
	"id_timestamp":      arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_decimal":        "54321.98765",
	"id_double":         float64(202.456),
	"id_bool":           false,
	"created_timestamp": int32(1754905699),
	"id_regex":          `{"pattern": "updated.*", "options": "i"}`,
	"id_nested":         `{"nested_int":42,"nested_string":"nested_value"}`,
	"id_minkey":         `{}`,
	"id_maxkey":         `{}`,
	"name_varchar":      "updated varchar",
}

var MongoToIcebergSchema = map[string]string{
	"id_bigint":         "bigint",
	"id_int":            "int",
	"id_timestamp":      "timestamp",
	"id_double":         "double",
	"id_bool":           "boolean",
	"id_decimal":        "string",
	"created_timestamp": "string",
	"id_regex":          "string",
	"id_nested":         "string",
	"id_minkey":         "string",
	"id_maxkey":         "string",
	"name_varchar":      "string",
}
