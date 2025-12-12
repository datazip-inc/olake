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
	}
	testConfig.TestIntegration(t)
}
