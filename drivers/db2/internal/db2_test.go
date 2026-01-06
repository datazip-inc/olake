package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestDB2Integration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(string(constants.DB2)),
		Namespace:                        "DB2INST1",
		ExpectedData:                     ExpectedDB2Data,
		ExpectedUpdatedData:              ExpectedUpdatedDB2Data,
		DestinationDataTypeSchema:        DB2ToDestinationSchema,
		UpdatedDestinationDataTypeSchema: UpdatedDB2ToDestinationSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "db2_testdb_db2inst1",
		CursorField:                      "COL_CURSOR:COL_SMALLINT",
		PartitionRegex:                   "/{id, identity}",
	}
	testConfig.TestIntegration(t)
}
