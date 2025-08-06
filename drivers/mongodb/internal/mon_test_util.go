package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func ExecuteQueryPerformance(ctx context.Context, t *testing.T, op string, backfillStreams []string) {
	t.Helper()

	var cfg Mongo
	require.NoError(t, utils.UnmarshalFile("./testdata/source.json", &cfg.config, false))
	require.NoError(t, cfg.Setup(ctx))
	db := cfg.client
	defer func() {
		require.NoError(t, cfg.Close(ctx))
	}()

	switch op {
	case "setup_cdc":
		// truncate the cdc tables
		for _, stream := range backfillStreams {
			err := db.Database("mongodb").Collection(fmt.Sprintf("%s_cdc", stream)).Drop(ctx)
			require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", op), err)
		}
	case "trigger_cdc":
		// insert the data into the cdc tables concurrently
		err := utils.Concurrent(ctx, backfillStreams, len(backfillStreams), func(ctx context.Context, stream string, executionNumber int) error {
			// TODO: insert 15M rows from backfill stream to CDC stream
			_, err := db.Database("mongodb").Collection(fmt.Sprintf("%s_cdc", stream)).InsertOne(ctx, bson.M{"name": "test"})
			return err
		})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", op), err)
	}

}
