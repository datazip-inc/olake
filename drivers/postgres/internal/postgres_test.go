package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
	_ "github.com/lib/pq"
)

func TestPostgresIntegration(t *testing.T) {
	t.Parallel()

	t.Run("Normalized", func(t *testing.T) {
		testConfig := &testutils.IntegrationTest{
			TestConfig:                       testutils.GetTestConfig(string(constants.Postgres)),
			Namespace:                        "public",
			ExpectedData:                     ExpectedPostgresData,
			ExpectedUpdatedData:              ExpectedUpdatedData,
			DestinationDataTypeSchema:        PostgresToDestinationSchema,
			UpdatedDestinationDataTypeSchema: UpdatedPostgresToDestinationSchema,
			ExecuteQuery:                     ExecuteQuery,
			DestinationDB:                    "postgres_postgres_public",
			CursorField:                      "col_bigserial",
			PartitionRegex:                   "/{col_bigserial,identity}",
		}
		testConfig.TestIntegration(t)
	})

	t.Run("Denormalized", func(t *testing.T) {
		normalization := false
		testConfig := &testutils.IntegrationTest{
			TestConfig:                       testutils.GetTestConfig(string(constants.Postgres)),
			Namespace:                        "public",
			ExpectedData:                     ExpectedPostgresData,
			ExpectedUpdatedData:              ExpectedUpdatedData,
			DestinationDataTypeSchema:        PostgresToDestinationSchema,
			UpdatedDestinationDataTypeSchema: UpdatedPostgresToDestinationSchema,
			ExecuteQuery:                     ExecuteQuery,
			DestinationDB:                    "postgres_denorm_test",
			CursorField:                      "col_bigserial",
			PartitionRegex:                   "/{col_bigserial,identity}",
			Normalization:                    &normalization,
		}
		testConfig.TestIntegration(t)
	})
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
