package postgres

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	_ "github.com/lib/pq"
)

// postgresBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the postgres suites.
func postgresBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                &testutils.TestConfig{Driver: string(constants.Postgres)},
		ExpectedData:              ExpectedPostgresData,
		DestinationDataTypeSchema: PostgresToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedPostgresDefaultCDCColumnsSchema,
	}
	cfg.TestConfig.Namespace = "public"
	cfg.TestConfig.ExecuteQuery = ExecuteQuery
	cfg.TestConfig.DestinationDB = "postgres_postgres_public"
	cfg.TestConfig.CursorField = "col_cursor:col_int"
	cfg.TestConfig.PartitionRegex = "/{col_bigserial,identity}"
	cfg.TestConfig.ColumnToExclude = "excludedcolumn"
	cfg.TestConfig.FilterConfig = `{
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
                }`
	return cfg
}

func TestPostgresDiscover(t *testing.T) {
	postgresBaseConfig().TestDiscover(t)
}

func TestPostgresSync(t *testing.T) {
	t.Parallel()
	cfg := postgresBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedPostgresToDestinationSchema
	cfg.TestSync(t)
}

func TestPostgres2PC(t *testing.T) {
	t.Parallel()
	postgresBaseConfig().Test2PCIntegration(t)
}

func TestPostgresPerformance(t *testing.T) {
	config := &performance.Test{
		TestConfig:      &testutils.TestConfig{Driver: string(constants.Postgres)},
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}
	config.TestConfig.Namespace = "public"
	config.TestConfig.ExecuteQuery = ExecuteQuery

	config.TestPerformance(t)
}
