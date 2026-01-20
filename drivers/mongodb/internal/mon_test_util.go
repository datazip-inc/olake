package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
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

func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var connStr string
	var config Config
	if fileConfig {
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		connStr = fmt.Sprintf(
			"mongodb://%s:%s@%s/?authSource=%s&readPreference=%s",
			config.Username,
			config.Password,
			strings.Join(config.Hosts, ","),
			config.AuthDB,
			config.ReadPreference,
		)
	} else {
		connStr = fmt.Sprintf("mongodb://%s:%s@localhost:%d/admin?replicaSet=%s&directConnection=true",
			MongoDBAdminUser, MongoDBAdminPass, MongoDBPort, MongoDBReplicaSet)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	require.NoError(t, err, "Failed to connect to MongoDB replica set at localhost:%d", MongoDBPort)
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("warning: failed to disconnect from MongoDB: %v", err)
		}
	}()

	integrationTestCollection := streams[0]
	db := client.Database(MongoDBDatabase)
	collection := db.Collection(integrationTestCollection)

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
			"id_double":         float64(123.456),
			"id_bool":           true,
			"id_cursor":         int32(6),
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
		// insert a filtered doc
		filteredDoc := bson.M{
			"id":           999,
			"id_cursor":    -1,
			"id_bigint":    int64(111111111111111),
			"id_int":       int32(0),
			"id_timestamp": time.Date(2022, 6, 15, 10, 0, 0, 0, time.UTC),
			"id_double":    float64(50.123),
		}
		_, err = collection.InsertOne(ctx, filteredDoc)
		require.NoError(t, err, "Failed to insert filtered test data row")

	case "update":
		filter := bson.M{"id": int32(1)}
		update := bson.M{
			"$set": bson.M{
				"id_bigint":         int64(987654321098765),
				"id_int":            int64(200),
				"id_timestamp":      time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC),
				"id_double":         float64(202.456),
				"id_bool":           false,
				"id_cursor":         nil,
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
		filter := bson.M{"id": 1}
		_, err := collection.DeleteOne(ctx, filter)
		require.NoError(t, err, "Failed to delete document")

	case "setup_cdc":
		// truncate the cdc tables
		for _, cdcStream := range streams {
			_, err := client.Database(config.Database).Collection(cdcStream).DeleteMany(ctx, bson.D{})
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		}
		return

	case "bulk_cdc_data_insert":
		backfillStreams := testutils.GetBackfillStreamsFromCDC(streams)
		totalRows := 15000000

		// TODO: insert data in batch
		// insert the data into the cdc tables concurrently
		err := utils.Concurrent(ctx, streams, len(streams), func(ctx context.Context, cdcStream string, executionNumber int) error {
			srcColl := client.Database(config.Database).Collection(backfillStreams[executionNumber])
			destColl := client.Database(config.Database).Collection(cdcStream)

			cursor, err := srcColl.Find(ctx, bson.D{}, options.Find().SetLimit(int64(totalRows)))
			if err != nil {
				return fmt.Errorf("stream: %s, error: %s", cdcStream, err)
			}
			defer cursor.Close(ctx)

			var docs []interface{}
			for cursor.Next(ctx) {
				var doc bson.M
				if err := cursor.Decode(&doc); err != nil {
					return err
				}
				docs = append(docs, doc)
			}
			if err := cursor.Err(); err != nil {
				return err
			}
			if len(docs) == 0 {
				return nil
			}
			_, err = destColl.InsertMany(ctx, docs)
			if err != nil {
				return fmt.Errorf("stream: %s, error: %s", cdcStream, err)
			}
			return nil
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		return
	}
}

func insertTestData(t *testing.T, ctx context.Context, collection *mongo.Collection) {
	t.Helper()
	for i := 1; i <= 5; i++ {
		doc := bson.M{
			"id":                i,
			"id_cursor":         i,
			"id_bigint":         int64(123456789012345),
			"id_int":            int32(100),
			"id_timestamp":      time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
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
		require.NoError(t, err, "Failed to insert test data row %d", i)
	}
	// insert a filtered doc
	filteredDoc := bson.M{
		"id":           999,
		"id_cursor":    -1,
		"id_bigint":    int64(111111111111111),
		"id_int":       int32(0),
		"id_timestamp": time.Date(2021, 6, 15, 10, 0, 0, 0, time.UTC),
		"id_double":    float64(500234.123),
	}
	_, err := collection.InsertOne(ctx, filteredDoc)
	require.NoError(t, err, "Failed to insert filtered test data row")
}

var ExpectedMongoData = map[string]interface{}{
	"id_bigint":         int64(123456789012345),
	"id_int":            int32(100),
	"id_timestamp":      arrow.Timestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_double":         float64(123.456),
	"id_bool":           true,
	"created_timestamp": int32(1754905992),
	"id_regex":          `{"Pattern":"test.*","Options":"i"}`,
	"id_nested":         `{"nested_int":42,"nested_string":"nested_value"}`,
	"id_minkey":         `{}`,
	"id_maxkey":         `{}`,
	"name_varchar":      "varchar_val",
}

var ExpectedUpdatedData = map[string]interface{}{
	"id_bigint":         int64(987654321098765),
	"id_int":            int64(200),
	"id_timestamp":      arrow.Timestamp(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"id_double":         float64(202.456),
	"id_bool":           false,
	"created_timestamp": int32(1754905699),
	"id_regex":          `{"Pattern":"updated.*","Options":"i"}`,
	"id_nested":         `{"nested_int":42,"nested_string":"nested_value"}`,
	"id_minkey":         `{}`,
	"id_maxkey":         `{}`,
	"name_varchar":      "updated varchar",
}

var MongoToDestinationSchema = map[string]string{
	"id_bigint":         "bigint",
	"id_int":            "int",
	"id_timestamp":      "timestamp",
	"id_double":         "double",
	"id_bool":           "boolean",
	"created_timestamp": "int",
	"id_regex":          "string",
	"id_nested":         "string",
	"id_minkey":         "string",
	"id_maxkey":         "string",
	"name_varchar":      "string",
}

var UpdatedMongoToDestinationSchema = map[string]string{
	"id_bigint":         "bigint",
	"id_int":            "bigint",
	"id_timestamp":      "timestamp",
	"id_double":         "double",
	"id_bool":           "boolean",
	"created_timestamp": "int",
	"id_regex":          "string",
	"id_nested":         "string",
	"id_minkey":         "string",
	"id_maxkey":         "string",
	"name_varchar":      "string",
}
