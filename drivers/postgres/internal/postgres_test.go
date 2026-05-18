package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
	_ "github.com/lib/pq"
)

// newPostgresBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestPostgresIntegration and TestPostgres2PC.
func newPostgresBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(string(constants.Postgres)),
		Namespace:                 "public",
		ExpectedData:              ExpectedPostgresData,
		DestinationDataTypeSchema: PostgresToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedPostgresDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "postgres_postgres_public",
		CursorField:               "col_cursor:col_int",
		PartitionRegex:            "/{col_bigserial,identity}",
		ColumnToExclude:           "excludedcolumn",
		FilterConfig: `{
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
}

func TestPostgresIntegration(t *testing.T) {
	t.Parallel()
	cfg := newPostgresBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedPostgresToDestinationSchema
	cfg.TestIntegration(t)
}

func TestPostgres2PC(t *testing.T) {
	t.Parallel()
	newPostgresBaseConfig().Test2PCIntegration(t)
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
