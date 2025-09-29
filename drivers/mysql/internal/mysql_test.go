package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMySQLIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(string(constants.MySQL)),
		Namespace:                 "olake_mysql_test",
		ExpectedData:              ExpectedMySQLData,
		ExpectedIcebergUpdateData: ExpectedIcebergUpdatedData,
		ExpectedParquetUpdateData: ExpectedParquetUpdatedData,
		DestinationDataTypeSchema: MySQLToDestinationSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "mysql_olake_mysql_test",
	}
	testConfig.TestIntegration(t)
}

func TestMySQLPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(string(constants.MySQL)),
		Namespace:       "benchmark",
		BackfillStreams: []string{"trips", "fhv_trips"},
		CDCStreams:      []string{"trips_cdc", "fhv_trips_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
