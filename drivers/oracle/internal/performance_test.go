package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/jmoiron/sqlx"
)

func TestOraclePerformance(t *testing.T) {
	config := testutils.PerformanceTestConfig{
		TestConfig:      testutils.GetTestConfig("oracle"),
		Namespace:       "ADMIN",
		BackfillStreams: []string{"USERS"},
		CDCStreams:      nil,
		ConnectDB:       connectDatabase,
		CloseDB:         closeDatabase,
		SetupCDC:        nil,
		TriggerCDC:      nil,
		SupportsCDC:     false,
	}

	testutils.RunPerformanceTest(t, config)
}

func connectDatabase(ctx context.Context) (interface{}, error) {
	var cfg Oracle
	if err := utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false); err != nil {
		return nil, err
	}
	cfg.Setup(ctx)
	return cfg.client, nil
}

func closeDatabase(conn interface{}) error {
	if db, ok := conn.(*sqlx.DB); ok {
		return db.Close()
	}
	return nil
}
