package driver

import (
	"context"
	"fmt"
	"testing"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
)

func TestMySQLPerformance(t *testing.T) {
	config := abstract.PerformanceTestConfig{
		TestConfig:      abstract.GetTestConfig("mysql"),
		Namespace:       "mysql",
		BackfillStreams: []string{"test"},
		CDCStreams:      []string{"test_cdc"},
		ConnectDB:       connectDatabase,
		CloseDB:         closeDatabase,
		SetupCDC:        setupDatabaseForCDC,
		TriggerCDC:      triggerMySQLCDC,
		SupportsCDC:     true,
	}

	abstract.RunPerformanceTest(t, config)
}

func connectDatabase(ctx context.Context) (interface{}, error) {
	fmt.Println("🟡 connectDatabase")
	var cfg MySQL
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

func setupDatabaseForCDC(ctx context.Context, conn interface{}) error {
	fmt.Println("🟡 setupDatabaseForCDC")
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
	fmt.Println("🟡 triggerMySQLCDC")
	db, ok := conn.(*sqlx.DB)
	if !ok {
		return fmt.Errorf("invalid connection type")
	}
	_, err := db.ExecContext(ctx, "INSERT INTO test_cdc SELECT * FROM test")
	return err
}
