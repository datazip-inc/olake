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
		Driver:             string(constants.Postgres),
		ExpectedData:       ExpectedPostgresData,
		ExpectedUpdateData: ExpectedUpdatedPostgresData,
		DataTypeSchema:     PostgresToIcebergSchema,
		ExecuteQuery:       ExecuteQuery,
	}
	testConfig.TestIntegration(t)
}

func TestPostgresPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:          testutils.GetTestConfig("postgres"),
		Namespace:           "public",
		BackfillStreams:     []string{"trips", "fhv_trips"},
		CDCStreams:          []string{"trips_cdc", "fhv_trips_cdc"},
		ExecuteQuery:        ExecuteQueryPerformance,
		SupportsCDC:         true,
		UsesPreChunkedState: false,
	}

	config.TestPerformance(t)
}
