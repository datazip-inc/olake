package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestOracleIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(string(constants.Oracle)),
		Namespace:                        "MYUSER",
		ExpectedData:                     ExpectedOracleData,
		ExpectedUpdatedData:              ExpectedUpdatedOracleData,
		DestinationDataTypeSchema:        OracleToDestinationSchema,
		UpdatedDestinationDataTypeSchema: UpdatedOracleToDestinationSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "oracle_myuser",
		CursorField:                      "COL_CURSOR:COL_SMALLINT",
		PartitionRegex:                   "/{id, identity}",
		FilterInput: `{
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
	testConfig.TestIntegration(t)
}
