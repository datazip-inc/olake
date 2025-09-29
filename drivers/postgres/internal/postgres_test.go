package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
	_ "github.com/lib/pq"
)

func TestPostgresIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(string(constants.Postgres)),
		Namespace:                 "public",
		ExpectedData:              ExpectedPostgresData,
		ExpectedIcebergUpdateData: ExpectedIcebergUpdatedData,
		ExpectedParquetUpdateData: ExpectedParquetUpdateData,
		DestinationDataTypeSchema: PostgresToDestinationSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "postgres_postgres_public",
	}
	testConfig.TestIntegration(t)
}

func TestPostgresPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(string(constants.Postgres)),
		Namespace:       "public",
		BackfillStreams: []string{"trips", "fhv_trips"},
		CDCStreams:      []string{"trips_cdc", "fhv_trips_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
