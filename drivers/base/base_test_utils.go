package base

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/writers/parquet"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

// TestHelper defines database-specific helper functions
type TestHelper struct {
	CreateTable func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	DropTable   func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	CleanTable  func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	AddData     func(ctx context.Context, t *testing.T, conn interface{}, tableName string, numItems int, startAtItem int, cols ...string)

	//CDC operations
	InsertOp func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	UpdateOp func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
	DeleteOp func(ctx context.Context, t *testing.T, conn interface{}, tableName string)
}

// MongoTestHelper defines MongoDB-specific helper functions
type MongoTestHelper struct {
	CreateCollection func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string)
	DropCollection   func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string)
	CleanCollection  func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string)
	AddDocuments     func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string, numDocs int, startAtDoc int)

	//CDC operations for MongoDB
	InsertDocOp func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string)
	UpdateDocOp func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string)
	DeleteDocOp func(ctx context.Context, t *testing.T, client *mongo.Client, dbName, collName string)
}

// TestSetup tests the driver setup and connection check
const tableName = "test_table_olake"
const mongoCollName = "test_collection_olake"

func TestSetup(t *testing.T, driver protocol.Driver, client interface{}) {
	t.Helper()
	assert.NotNil(t, client, "Client should not be nil")
	err := driver.Check()
	assert.NoError(t, err, "Connection check failed")
}

// TestMongoSetup tests the MongoDB driver setup and connection check
func TestMongoSetup(t *testing.T, driver protocol.Driver, client *mongo.Client) {
	t.Helper()
	assert.NotNil(t, client, "MongoDB client should not be nil")
	err := driver.Check()
	assert.NoError(t, err, "MongoDB connection check failed")
}

// TestDiscover tests the discovery of tables
func TestDiscover(t *testing.T, driver protocol.Driver, client interface{}, helper TestHelper) {
	t.Helper()
	ctx := context.Background()
	conn := client.(*sqlx.DB) // For Postgres; adjust if MySQL uses *sql.DB

	helper.CreateTable(ctx, t, conn, tableName)
	defer helper.DropTable(ctx, t, conn, tableName)
	helper.CleanTable(ctx, t, conn, tableName)
	helper.AddData(ctx, t, conn, tableName, 5, 6, "col1", "col2")

	streams, err := driver.Discover(true)
	assert.NoError(t, err, "Discover failed")
	assert.NotEmpty(t, streams, "No streams found")
	for _, stream := range streams {
		if stream.Name == tableName {
			return
		}
	}
	assert.Fail(t, "Unable to find test table %s", tableName)
}

// TestMongoDiscover tests the discovery of MongoDB collections
func TestMongoDiscover(t *testing.T, driver protocol.Driver, client *mongo.Client, helper MongoTestHelper, dbName string) {
	t.Helper()
	ctx := context.Background()

	helper.CreateCollection(ctx, t, client, dbName, mongoCollName)
	defer helper.DropCollection(ctx, t, client, dbName, mongoCollName)
	helper.CleanCollection(ctx, t, client, dbName, mongoCollName)
	helper.AddDocuments(ctx, t, client, dbName, mongoCollName, 5, 1)

	streams, err := driver.Discover(true)
	assert.NoError(t, err, "Discover failed")
	assert.NotEmpty(t, streams, "No streams found")

	found := false
	for _, stream := range streams {
		if stream.Name == mongoCollName {
			found = true
			break
		}
	}
	assert.True(t, found, "Unable to find test collection %s", mongoCollName)
}

// TestRead tests full refresh and CDC read operations
func TestRead(t *testing.T, _ protocol.Driver, client interface{}, helper TestHelper, setupClient func(t *testing.T) (interface{}, protocol.Driver)) {
	t.Helper()
	ctx := context.Background()
	conn := client.(*sqlx.DB) // Adjust based on driver

	// Setup table and initial data
	helper.CreateTable(ctx, t, conn, tableName)
	defer helper.DropTable(ctx, t, conn, tableName)
	helper.CleanTable(ctx, t, conn, tableName)
	helper.AddData(ctx, t, conn, tableName, 5, 1, "col1", "col2")

	// Register Parquet writer
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return &parquet.Parquet{}
	}

	pool, err := protocol.NewWriter(ctx, &types.WriterConfig{
		Type: "PARQUET",
		WriterConfig: map[string]any{
			"normalization": true,
			"local_path":    os.TempDir(), // Adjust path as needed
		},
	})
	require.NoError(t, err, "Failed to create writer pool")

	// Get test stream
	getTestStream := func(d protocol.Driver) *types.Stream {
		streams, err := d.Discover(true)
		require.NoError(t, err, "Discover failed")
		require.NotEmpty(t, streams, "No streams found")
		for _, stream := range streams {
			if stream.Name == tableName {
				return stream
			}
		}
		require.Fail(t, "Could not find stream for table %s", tableName)
		return nil
	}

	// Run read test for a given sync mode
	runReadTest := func(t *testing.T, syncMode types.SyncMode, extraTests func(t *testing.T)) {
		_, streamDriver := setupClient(t)
		testStream := getTestStream(streamDriver)
		dummyStream := &types.ConfiguredStream{Stream: testStream}
		dummyStream.Stream.SyncMode = syncMode

		if syncMode == types.CDC {
			readErrCh := make(chan error, 1)
			go func() {
				readErrCh <- streamDriver.Read(pool, dummyStream)
			}()
			time.Sleep(2 * time.Second) // Wait for CDC initialization

			if extraTests != nil {
				extraTests(t)
			}
			time.Sleep(3 * time.Second) // Wait for CDC to process
			// Directly receive from the channel
			err := <-readErrCh
			assert.NoError(t, err, "CDC read operation failed")
		} else {
			err := streamDriver.Read(pool, dummyStream)
			assert.NoError(t, err, "Read operation failed")
		}
	}

	t.Run("full refresh read", func(t *testing.T) {
		runReadTest(t, types.FULLREFRESH, nil)
	})

	t.Run("cdc read", func(t *testing.T) {
		runReadTest(t, types.CDC, func(t *testing.T) {
			t.Run("insert operation", func(t *testing.T) {
				helper.InsertOp(ctx, t, conn, tableName)
			})
			t.Run("update operation", func(t *testing.T) {
				helper.UpdateOp(ctx, t, conn, tableName)
			})
			t.Run("delete operation", func(t *testing.T) {
				helper.DeleteOp(ctx, t, conn, tableName)
			})
		})
	})
}

// TestMongoRead tests full refresh and CDC read operations for MongoDB
func TestMongoRead(t *testing.T, _ protocol.Driver, client *mongo.Client, helper MongoTestHelper, setupClient func(t *testing.T) (*mongo.Client, protocol.Driver), dbName string) {
	t.Helper()
	ctx := context.Background()

	// Setup collection and initial data
	helper.CreateCollection(ctx, t, client, dbName, mongoCollName)
	defer helper.DropCollection(ctx, t, client, dbName, mongoCollName)
	helper.CleanCollection(ctx, t, client, dbName, mongoCollName)
	helper.AddDocuments(ctx, t, client, dbName, mongoCollName, 5, 1)

	// Register Parquet writer
	protocol.RegisteredWriters[types.Parquet] = func() protocol.Writer {
		return &parquet.Parquet{}
	}

	pool, err := protocol.NewWriter(ctx, &types.WriterConfig{
		Type: "PARQUET",
		WriterConfig: map[string]any{
			"normalization": true,
			"local_path":    os.TempDir(),
		},
	})
	require.NoError(t, err, "Failed to create writer pool")

	// Get test stream
	getTestStream := func(d protocol.Driver) *types.Stream {
		streams, err := d.Discover(true)
		require.NoError(t, err, "Discover failed")
		require.NotEmpty(t, streams, "No streams found")
		for _, stream := range streams {
			if stream.Name == mongoCollName {
				return stream
			}
		}
		require.Fail(t, "Could not find stream for collection %s", mongoCollName)
		return nil
	}

	// Run read test for a given sync mode
	runReadTest := func(t *testing.T, syncMode types.SyncMode, extraTests func(t *testing.T)) {
		_, streamDriver := setupClient(t)
		testStream := getTestStream(streamDriver)
		dummyStream := &types.ConfiguredStream{Stream: testStream}
		dummyStream.Stream.SyncMode = syncMode

		if syncMode == types.CDC {
			readErrCh := make(chan error, 1)
			go func() {
				readErrCh <- streamDriver.Read(pool, dummyStream)
			}()
			time.Sleep(2 * time.Second) // Wait for CDC initialization

			if extraTests != nil {
				extraTests(t)
			}
			time.Sleep(3 * time.Second) // Wait for CDC to process
			err := <-readErrCh
			assert.NoError(t, err, "CDC read operation failed")
		} else {
			err := streamDriver.Read(pool, dummyStream)
			assert.NoError(t, err, "Read operation failed")
		}
	}

	t.Run("mongo full refresh read", func(t *testing.T) {
		runReadTest(t, types.FULLREFRESH, nil)
	})

	t.Run("mongo cdc read", func(t *testing.T) {
		runReadTest(t, types.CDC, func(t *testing.T) {
			t.Run("insert document operation", func(t *testing.T) {
				helper.InsertDocOp(ctx, t, client, dbName, mongoCollName)
			})
			t.Run("update document operation", func(t *testing.T) {
				helper.UpdateDocOp(ctx, t, client, dbName, mongoCollName)
			})
			t.Run("delete document operation", func(t *testing.T) {
				helper.DeleteDocOp(ctx, t, client, dbName, mongoCollName)
			})
		})
	})
}
