// testMySQLClient
package driver

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/types"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

func testMySQLClient(t *testing.T) (*sql.DB, Config, *MySQL) {
	t.Helper()

	// Use environment variables if provided, otherwise use defaults
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "localhost"
	}

	port := 3306
	username := os.Getenv("MYSQL_USERNAME")
	if username == "" {
		username = "mysql"
	}

	password := os.Getenv("MYSQL_PASSWORD")
	if password == "" {
		password = "secret1234"
	}

	database := os.Getenv("MYSQL_DATABASE")
	if database == "" {
		database = "mysql"
	}

	config := Config{
		Username:   username,
		Host:       host,
		Port:       port,
		Password:   password,
		Database:   database,
		MaxThreads: 4,
		RetryCount: 3,
	}

	// Create MySQL driver instance
	d := &MySQL{
		Driver: base.NewBase(),
		config: &config,
	}

	// Set up CDC config
	d.config.UpdateMethod = &CDC{
		InitialWaitTime: 5,
	}
	d.CDCSupport = true
	d.cdcConfig = CDC{
		InitialWaitTime: 5,
	}

	// Set up state
	d.SetupState(types.NewState(types.GlobalType))

	// Connect to database
	db, err := sql.Open("mysql", config.URI())
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	// Test connection
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	d.client = db

	return db, *d.config, d
}
