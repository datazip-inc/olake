package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestMongodbPerformance(t *testing.T) {
	config := testutils.PerformanceTestConfig{
		TestConfig:      testutils.GetTestConfig("mongodb"),
		Namespace:       "test",
		BackfillStreams: []string{"users"},
		CDCStreams:      []string{"users_cdc"},
		ConnectDB:       connectDatabase,
		CloseDB:         closeDatabase,
		SetupCDC:        setupDatabaseForCDC,
		TriggerCDC:      triggerMongodbCDC,
		SupportsCDC:     true,
	}

	testutils.RunPerformanceTest(t, config)
}

func connectDatabase(ctx context.Context) (interface{}, error) {
	var cfg Mongo
	if err := utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false); err != nil {
		return nil, err
	}
	if err := cfg.Setup(ctx); err != nil {
		return nil, err
	}
	return cfg.client, nil
}

func closeDatabase(conn interface{}) error {
	if db, ok := conn.(*mongo.Client); ok {
		return db.Disconnect(context.Background())
	}
	return nil
}

func setupDatabaseForCDC(ctx context.Context, conn interface{}) error {
	db, ok := conn.(*mongo.Client)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}
	db.Database("mongodb").Collection("users_cdc").Drop(ctx)
	return nil
}

func triggerMongodbCDC(ctx context.Context, conn interface{}) error {
	db, ok := conn.(*mongo.Client)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}
	db.Database("mongodb").Collection("users_cdc").InsertOne(ctx, bson.M{"name": "test"})
	return nil
}
