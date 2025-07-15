package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func testSetup(ctx context.Context, t *testing.T) interface{} {
	db, err := sqlx.ConnectContext(ctx, "mysql",
		"mysql:secret1234@tcp(localhost:3306)/olake_mysql_test?parseTime=true",
	)
	require.NoError(t, err, "failed to connect to  mysql")
	return db
}

func TestMySQLIntegration(t *testing.T) {
	testConfig := &abstract.TestInterface{
		Driver:             string(constants.MySQL),
		Namespace:          "olake_mysql_test",
		ExpectedData:       ExpectedMySQLData,
		ExpectedUpdateData: ExpectedUpdatedMySQLData,
		DataTypeSchema:     MySQLSchema,
		ExecuteQuery:       ExecuteQuery,
		TestSetup:          testSetup,
	}
	testConfig.TestIntegration(t)
}
