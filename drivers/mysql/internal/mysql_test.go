package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMySQLIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		Driver:             string(constants.MySQL),
		ExpectedData:       ExpectedMySQLData,
		ExpectedUpdateData: ExpectedUpdatedMySQLData,
		DataTypeSchema:     MySQLToIcebergSchema,
		ExecuteQuery:       ExecuteQuery,
	}
	testConfig.TestIntegration(t)
}

func TestMySQLPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:     testutils.GetTestConfig("mysql"),
		Namespace:      "performance",
		BackfillStream: "users",
		CDCStream:      "users_cdc",
		ExecuteQuery:   ExecuteQueryPerformance,
		SupportsCDC:    true,
	}

	config.TestPerformance(t)
}
