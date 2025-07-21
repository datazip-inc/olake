package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/jmoiron/sqlx"
)

func TestMySQLPerformance(t *testing.T) {
	config := testutils.PerformanceTestConfig{
		TestConfig:      testutils.GetTestConfig("mysql"),
		Namespace:       "mysql",
		BackfillStreams: []string{"test"},
		CDCStreams:      []string{"test_cdc"},
		ConnectDB:       connectDatabase,
		CloseDB:         closeDatabase,
		SetupCDC:        setupDatabaseForCDC,
		TriggerCDC:      triggerMySQLCDC,
		SupportsCDC:     true,
	}

	testutils.RunPerformanceTest(t, config)
}

func connectDatabase(ctx context.Context) (interface{}, error) {
	var cfg MySQL
	if err := utils.UnmarshalFile("./testconfig/source.json", &cfg.config, false); err != nil {
		return nil, err
	}
	if err := cfg.Setup(ctx); err != nil {
		return nil, err
	}
	return cfg.client, nil
}

func closeDatabase(conn interface{}) error {
	if db, ok := conn.(*sqlx.DB); ok {
		return db.Close()
	}
	return nil
}

func setupDatabaseForCDC(ctx context.Context, conn interface{}) error {
	db, ok := conn.(*sqlx.DB)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_cdc (id INT PRIMARY KEY, name VARCHAR(255))"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE test_cdc"); err != nil {
		return err
	}
	return nil
}

func triggerMySQLCDC(ctx context.Context, conn interface{}) error {
	db, ok := conn.(*sqlx.DB)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}
	_, err := db.ExecContext(ctx, "INSERT INTO test_cdc SELECT * FROM test")
	return err
}
