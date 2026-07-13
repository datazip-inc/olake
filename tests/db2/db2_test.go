package db2

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// db2BaseConfig returns an IntegrationTest pre-populated with all fields shared
// between the db2 integration tests.
func db2BaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig("db2").WithImagePlatform("linux/amd64"),
		Namespace:                 "DB2INST1",
		ExpectedData:              ExpectedDB2Data,
		DestinationDataTypeSchema: DB2ToDestinationSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "db2_testdb_db2inst1",
		CursorField:               "COL_CURSOR:COL_TIMESTAMP",
		PartitionRegex:            "/{id, identity}",
		ColumnToExclude:           "EXCLUDEDCOLUMN",
		FilterConfig: `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "COL_DOUBLE",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "COL_TIMESTAMP",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`,
	}
}

func TestDiscover(t *testing.T) {
	db2BaseConfig().TestDiscover(t)
}

func TestSync(t *testing.T) {
	cfg := db2BaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedDB2Data
	cfg.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
	cfg.TestSync(t)
}

func Test2PC(t *testing.T) {
	db2BaseConfig().Test2PCIntegration(t)
}
