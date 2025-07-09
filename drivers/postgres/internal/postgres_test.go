package driver

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

const (
	currentTestTable      = "postgres_test_table_olake"
	sourceConfigPath      = "/test-olake/drivers/postgres/internal/testdata/source.json"
	streamsPath           = "/test-olake/drivers/postgres/internal/testdata/streams.json"
	destinationConfigPath = "/test-olake/drivers/postgres/internal/testdata/destination.json"
	statePath             = "/test-olake/drivers/postgres/internal/testdata/state.json"
	namespace             = "public"
)

func testSetup(ctx context.Context, t *testing.T) interface{} {
	db, err := sqlx.ConnectContext(ctx, "postgres",
		"postgres://postgres@localhost:5433/postgres?sslmode=disable",
	)
	require.NoError(t, err, "failed to connect to postgres")
	return db
}

func TestPostgresIntegration(t *testing.T) {
	abstract.TestIntegration(t, string(constants.Postgres), sourceConfigPath, streamsPath, destinationConfigPath, statePath, namespace, ExpectedPostgresData, ExpectedUpdatedPostgresData, PostgresSchema, ExecuteQuery, testSetup)
}
