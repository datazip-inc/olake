package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var connStr string
	if fileConfig {
		var driver Mongo
		utils.UnmarshalFile("./testdata/source.json", &driver.config, false)
		connStr = fmt.Sprintf(
			"mongodb+srv://%s:%s@%s/%s",
			driver.config.Username,
			driver.config.Password,
			driver.config.Hosts[0],
			driver.config.Database,
		)
	} else {
		connStr = "mongodb://localhost:27017"
	}
	db, ok := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	require.NoError(t, ok, "failed to connect to mongodb")

	switch operation {
	case "setup_cdc":
		// truncate the cdc tables
		for _, stream := range streams {
			err := db.Database("mongodb").Collection(fmt.Sprintf("%s_cdc", stream)).Drop(ctx)
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
		}
	case "bulk_cdc_data_insert":
		// insert the data into the cdc tables concurrently
		err := utils.Concurrent(ctx, streams, len(streams), func(ctx context.Context, stream string, executionNumber int) error {
			// TODO: insert 15M rows from backfill stream to CDC stream
			_, err := db.Database("mongodb").Collection(fmt.Sprintf("%s_cdc", stream)).InsertOne(ctx, bson.M{"name": "test"})
			return err
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", operation), err)
	}

}
