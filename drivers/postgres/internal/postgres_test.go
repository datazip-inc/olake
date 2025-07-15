package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func testSetup(ctx context.Context, t *testing.T) interface{} {
	db, err := sqlx.ConnectContext(ctx, "postgres",
		"postgres://postgres@localhost:5433/postgres?sslmode=disable",
	)
	require.NoError(t, err, "failed to connect to postgres")
	return db
}

func TestPostgresIntegration(t *testing.T) {
	testConfig := &testutils.TestInterface{
		Driver:             string(constants.Postgres),
		Namespace:          "public",
		ExpectedData:       ExpectedPostgresData,
		ExpectedUpdateData: ExpectedUpdatedPostgresData,
		DataTypeSchema:     PostgresSchema,
		ExecuteQuery:       ExecuteQuery,
		TestSetup:          testSetup,
	}
	testConfig.TestIntegration(t)
}
