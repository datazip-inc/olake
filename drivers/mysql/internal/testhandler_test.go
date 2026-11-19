package driver

import (
	"context"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// mysqlRootPassword is the root password of drivers/mysql/docker-compose.yml, whose server the integration suites share.
const mysqlRootPassword = "root1234"

var mysqlService = &testhandler.Service{
	Container: "olake_mysql-test",
	Port:      3306,
	Target:    "olake.mysql.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := mysqlCLI(ctx, s, "SELECT 1")
		return err
	},
}

// mysqlSource gives each test its own database on the shared server, which writes a row-format binlog.
type mysqlSource struct{}

func (mysqlSource) Service() *testhandler.Service {
	return mysqlService
}

func (mysqlSource) Create(ctx context.Context, database string) error {
	_, err := mysqlCLI(ctx, mysqlService, "CREATE DATABASE `"+database+"`")
	return err
}

// Drop gives up on a lock that a leaked transaction holds instead of hanging.
func (mysqlSource) Drop(ctx context.Context, database string) error {
	_, err := mysqlCLI(ctx, mysqlService, "SET SESSION lock_wait_timeout = 30; DROP DATABASE IF EXISTS `"+database+"`")
	return err
}

// testConfig reaches this test's own database, which cleanup drops.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Host:     testhandler.Host,
		Port:     mysqlService.Port,
		Database: h.Database(mysqlSource{}),
		Username: "root",
		Password: mysqlRootPassword,
	}
}

// mysqlCLI runs statements on the server as root with the mysql client inside the container.
func mysqlCLI(ctx context.Context, s *testhandler.Service, statements string) (string, error) {
	return s.Exec(ctx, []string{"--env", "MYSQL_PWD=" + mysqlRootPassword},
		"mysql", "--host", "127.0.0.1", "--user", "root", "--skip-column-names", "--execute", statements)
}
