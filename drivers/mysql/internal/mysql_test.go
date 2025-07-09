package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

const (
	currentTestTable      = "mysql_test_table_olake"
	sourceConfigPath      = "/test-olake/drivers/mysql/internal/testdata/source.json"
	streamsPath           = "/test-olake/drivers/mysql/internal/testdata/streams.json"
	destinationConfigPath = "/test-olake/drivers/mysql/internal/testdata/destination.json"
	statePath             = "/test-olake/drivers/mysql/internal/testdata/state.json"
	namespace             = "olake_mysql_test"
)

func testSetup(ctx context.Context, t *testing.T) interface{} {
	db, err := sqlx.ConnectContext(ctx, "mysql",
		"mysql:secret1234@tcp(localhost:3306)/olake_mysql_test?parseTime=true",
	)
	require.NoError(t, err, "failed to connect to  mysql")
	return db
}

func TestMySQLIntegration(t *testing.T) {
	abstract.TestIntegration(t, string(constants.MySQL), sourceConfigPath, streamsPath, destinationConfigPath, statePath, namespace, ExpectedMySQLData, ExpectedUpdatedMySQLData, MySQLSchema, ExecuteQuery, testSetup)
}
