package mysql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// mysqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the mysql suites.
func mysqlBaseConfig(t *testing.T) *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(t, string(constants.MySQL)),
		Namespace:                 "olake_mysql_test",
		ExpectedData:              ExpectedMySQLData,
		DestinationDataTypeSchema: MySQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMySQLDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "mysql_olake_mysql_test",
		CursorField:               "id_cursor:id_smallint",
		PartitionRegex:            "/{id,identity}",
		ColumnToExclude:           "excludedColumn",
		FilterConfig: `{
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
                }`,
	}
}

func TestMySQLDiscover(t *testing.T) {
	mysqlBaseConfig(t).TestDiscover(t)
}

func TestMySQLSync(t *testing.T) {
	t.Parallel()
	cfg := mysqlBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
	cfg.TestSync(t)
}

func TestMySQL2PC(t *testing.T) {
	t.Parallel()
	mysqlBaseConfig(t).Test2PCIntegration(t)
}

func TestMySQLPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(t, string(constants.MySQL)),
		Namespace:       "benchmark",
		BackfillStreams: []string{"trips", "fhv_trips"},
		CDCStreams:      []string{"trips_cdc", "fhv_trips_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
