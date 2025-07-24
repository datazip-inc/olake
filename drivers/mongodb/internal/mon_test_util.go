package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func ExecuteQueryPerformance(ctx context.Context, t *testing.T, op string) {
	t.Helper()

	var cfg Mongo
	require.NoError(t, utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false))
	require.NoError(t, cfg.Setup(ctx))
	db := cfg.client
	defer func() {
		require.NoError(t, cfg.Close(ctx))
	}()

	switch op {
	case "setup_cdc":
		err := db.Database("mongodb").Collection("users_cdc").Drop(ctx)
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", op))
	case "trigger_cdc":
		// TODO: Insert data from backfill collection to cdc collection
		// Check how many rows we need to transfer
		_, err := db.Database("mongodb").Collection("users_cdc").InsertOne(ctx, bson.M{"name": "test"})
		require.NoError(t, err, fmt.Sprintf("failed to execute %s operation", op))
	}

}
