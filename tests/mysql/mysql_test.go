package mysql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
)

// mysqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the mysql suites.
func mysqlBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                &testutils.TestConfig{Driver: string(constants.MySQL)},
		ExpectedData:              ExpectedMySQLData,
		DestinationDataTypeSchema: MySQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMySQLDefaultCDCColumnsSchema,
	}
	cfg.TestConfig.Namespace = "olake_mysql_test"
	cfg.TestConfig.ExecuteQuery = ExecuteQuery
	cfg.TestConfig.DestinationDB = "mysql_olake_mysql_test"
	cfg.TestConfig.CursorField = "id_cursor:id_smallint"
	cfg.TestConfig.PartitionRegex = "/{id,identity}"
	cfg.TestConfig.ColumnToExclude = "excludedColumn"
	cfg.TestConfig.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "price_double",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "created_timestamp",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`
	return cfg
}

func TestMySQLDiscover(t *testing.T) {
	mysqlBaseConfig().TestDiscover(t)
}

func TestMySQLSync(t *testing.T) {
	t.Parallel()
	cfg := mysqlBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
	cfg.TestSync(t)
}

func TestMySQL2PC(t *testing.T) {
	t.Parallel()
	mysqlBaseConfig().Test2PCIntegration(t)
}

func TestMySQLPerformance(t *testing.T) {
	config := &performance.Test{
		TestConfig:      &testutils.TestConfig{Driver: string(constants.MySQL)},
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}
	config.TestConfig.Namespace = "benchmark"
	config.TestConfig.ExecuteQuery = ExecuteQuery

	config.TestPerformance(t)
}
