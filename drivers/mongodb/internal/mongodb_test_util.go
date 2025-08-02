package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
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
		// Insert the same data as the add operation (following MySQL/PostgreSQL pattern)
		doc := bson.M{
			"id_bigint":              123456789012345,
			"id_int":                 100,
			"id_int_unsigned":        101,
			"id_integer":             102,
			"id_integer_unsigned":    103,
			"id_mediumint":           5001,
			"id_mediumint_unsigned":  5002,
			"id_smallint":            101,
			"id_smallint_unsigned":   102,
			"id_tinyint":             50,
			"id_tinyint_unsigned":    51,
			"price_decimal":          123.45,
			"price_double":           123.456,
			"price_double_precision": 123.456,
			"price_float":            123.45,
			"price_numeric":          123.45,
			"price_real":             123.456,
			"name_char":              "c",
			"name_varchar":           "varchar_val",
			"name_text":              "text_val",
			"name_tinytext":          "tinytext_val",
			"name_mediumtext":        "mediumtext_val",
			"name_longtext":          "longtext_val",
			"created_date":           time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"created_timestamp":      time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"is_active":              1,
			"long_varchar":           "long_varchar_val",
			"name_bool":              1,
		}
		_, err := collection.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert document")

	case "update":
		filter := bson.M{"id_int": 100}
		update := bson.M{
			"$set": bson.M{
				"id_bigint":              987654321098765,
				"id_int":                 200,
				"id_int_unsigned":        201,
				"id_integer":             202,
				"id_integer_unsigned":    203,
				"id_mediumint":           6001,
				"id_mediumint_unsigned":  6002,
				"id_smallint":            201,
				"id_smallint_unsigned":   202,
				"id_tinyint":             60,
				"id_tinyint_unsigned":    61,
				"price_decimal":          543.21,
				"price_double":           654.321,
				"price_double_precision": 654.321,
				"price_float":            543.21,
				"price_numeric":          543.21,
				"price_real":             654.321,
				"name_char":              "X",
				"name_varchar":           "updated varchar",
				"name_text":              "updated text",
				"name_tinytext":          "upd tiny",
				"name_mediumtext":        "upd medium",
				"name_longtext":          "upd long",
				"created_date":           time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC),
				"created_timestamp":      time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC),
				"is_active":              0,
				"long_varchar":           "updated long...",
				"name_bool":              0,
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
			"id_bigint":              123456789012345,
			"id_int":                 100,
			"id_int_unsigned":        101,
			"id_integer":             102,
			"id_integer_unsigned":    103,
			"id_mediumint":           5001,
			"id_mediumint_unsigned":  5002,
			"id_smallint":            101,
			"id_smallint_unsigned":   102,
			"id_tinyint":             50,
			"id_tinyint_unsigned":    51,
			"price_decimal":          123.45,
			"price_double":           123.456,
			"price_double_precision": 123.456,
			"price_float":            123.45,
			"price_numeric":          123.45,
			"price_real":             123.456,
			"name_char":              "c",
			"name_varchar":           "varchar_val",
			"name_text":              "text_val",
			"name_tinytext":          "tinytext_val",
			"name_mediumtext":        "mediumtext_val",
			"name_longtext":          "longtext_val",
			"created_date":           time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"created_timestamp":      time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			"is_active":              1,
			"long_varchar":           "long_varchar_val",
			"name_bool":              1,
		},
	}

	for i, doc := range testData {
		_, err := collection.InsertOne(ctx, doc)
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
}

var ExpectedMongoData = map[string]interface{}{
	"id_bigint":              int64(123456789012345),
	"id_int":                 int32(100),
	"id_int_unsigned":        int32(101),
	"id_integer":             int32(102),
	"id_integer_unsigned":    int32(103),
	"id_mediumint":           int32(5001),
	"id_mediumint_unsigned":  int32(5002),
	"id_smallint":            int32(101),
	"id_smallint_unsigned":   int32(102),
	"id_tinyint":             int32(50),
	"id_tinyint_unsigned":    int32(51),
	"price_decimal":          float64(123.45),
	"price_double":           float64(123.456),
	"price_double_precision": float64(123.456),
	"price_float":            float64(123.45),
	"price_numeric":          float64(123.45),
	"price_real":             float64(123.456),
	"name_char":              "c",
	"name_varchar":           "varchar_val",
	"name_text":              "text_val",
	"name_tinytext":          "tinytext_val",
	"name_mediumtext":        "mediumtext_val",
	"name_longtext":          "longtext_val",
	"created_date":           arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_timestamp":      arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"is_active":              int32(1),
	"long_varchar":           "long_varchar_val",
	"name_bool":              int32(1),
}

var ExpectedUpdatedMongoData = map[string]interface{}{
	"id_bigint":              int64(987654321098765),
	"id_int":                 int32(200),
	"id_int_unsigned":        int32(201),
	"id_integer":             int32(202), // Updated to match actual update operation
	"id_integer_unsigned":    int32(203), // Updated to match actual update operation
	"id_mediumint":           int32(6001),
	"id_mediumint_unsigned":  int32(6002),
	"id_smallint":            int32(201), // Updated to match actual update operation
	"id_smallint_unsigned":   int32(202), // Updated to match actual update operation
	"id_tinyint":             int32(60),
	"id_tinyint_unsigned":    int32(61),
	"price_decimal":          float64(543.21),
	"price_double":           float64(654.321),
	"price_double_precision": float64(654.321),
	"price_float":            float64(543.21),
	"price_numeric":          float64(543.21),
	"price_real":             float64(654.321),
	"name_char":              "X",
	"name_varchar":           "updated varchar",
	"name_text":              "updated text",
	"name_tinytext":          "upd tiny",
	"name_mediumtext":        "upd medium",
	"name_longtext":          "upd long",
	"created_date":           arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"created_timestamp":      arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"is_active":              int32(0),
	"long_varchar":           "updated long...",
	"name_bool":              int32(0),
}

var MongoToIcebergSchema = map[string]string{
	"id_bigint":              "bigint",
	"id_int":                 "int",
	"id_int_unsigned":        "int",
	"id_integer":             "int",
	"id_integer_unsigned":    "int",
	"id_mediumint":           "int",
	"id_mediumint_unsigned":  "int",
	"id_smallint":            "int",
	"id_smallint_unsigned":   "int",
	"id_tinyint":             "int",
	"id_tinyint_unsigned":    "int",
	"price_decimal":          "double",
	"price_double":           "double",
	"price_double_precision": "double",
	"price_float":            "double",
	"price_numeric":          "double",
	"price_real":             "double",
	"name_char":              "string",
	"name_varchar":           "string",
	"name_text":              "string",
	"name_tinytext":          "string",
	"name_mediumtext":        "string",
	"name_longtext":          "string",
	"created_date":           "timestamp",
	"created_timestamp":      "timestamp",
	"is_active":              "int",
	"long_varchar":           "string",
	"name_bool":              "int",
}
