package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
	_ "github.com/lib/pq"
)

func TestPostgresIntegration(t *testing.T) {
	testConfig := &testutils.TestInterface{
		Driver:             string(constants.Postgres),
		ExpectedData:       ExpectedPostgresData,
		ExpectedUpdateData: ExpectedUpdatedPostgresData,
		DataTypeSchema:     PostgresSchema,
		ExecuteQuery:       ExecuteQuery,
	}
	testConfig.TestIntegration(t)
}
