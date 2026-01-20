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
		TestConfig:                       testutils.GetTestConfig(string(constants.Postgres)),
		Namespace:                        "public",
		ExpectedData:                     ExpectedPostgresData,
		ExpectedUpdatedData:              ExpectedUpdatedData,
		DestinationDataTypeSchema:        PostgresToDestinationSchema,
		UpdatedDestinationDataTypeSchema: UpdatedPostgresToDestinationSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "postgres_postgres_public",
		CursorField:                      "col_cursor:col_int",
		PartitionRegex:                   "/{col_bigserial,identity}",
		FilterInput: `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "col_double_precision",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "col_timestamp",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`,
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
