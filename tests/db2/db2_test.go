package db2

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// db2BaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestDB2Integration and TestDB22PC.
func db2BaseConfig(t *testing.T) *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(t, string(constants.DB2)).WithImagePlatform("linux/amd64"),
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

func TestDB2Integration(t *testing.T) {
	t.Parallel()
	cfg := db2BaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedDB2Data
	cfg.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
	cfg.TestIntegration(t)
}

func TestDB22PC(t *testing.T) {
	t.Parallel()
	db2BaseConfig(t).Test2PCIntegration(t)
}
