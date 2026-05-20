package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

// oracleBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestOracleIntegration and TestOracle2PC.
func oracleBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(string(constants.Oracle)),
		Namespace:                 "MYUSER",
		ExpectedData:              ExpectedOracleData,
		DestinationDataTypeSchema: OracleToDestinationSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "oracle_myuser",
		CursorField:               "COL_CURSOR:COL_SMALLINT",
		PartitionRegex:            "/{id, identity}",
		ColumnToExclude:           "EXCLUDEDCOLUMN",
		FilterConfig: `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "COL_DOUBLE_PRECISION",
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

func TestOracleIntegration(t *testing.T) {
	t.Parallel()
	cfg := oracleBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedOracleData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedOracleToDestinationSchema
	cfg.TestIntegration(t)
}

func TestOracle2PC(t *testing.T) {
	t.Parallel()
	oracleBaseConfig().Test2PCIntegration(t)
}
