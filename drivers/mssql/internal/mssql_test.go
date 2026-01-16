package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMSSQLIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(string(constants.MSSQL)),
		Namespace:                        "dbo",
		ExpectedData:                     ExpectedMSSQLData,
		ExpectedUpdatedData:              ExpectedUpdatedMSSQLData,
		DestinationDataTypeSchema:        MSSQLToDestinationSchema,
		UpdatedDestinationDataTypeSchema: MSSQLToDestinationSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "mssql_olake_mssql_test_dbo",
		CursorField:                      "id_cursor:col_int",
		PartitionRegex:                   "/{id,identity}",
	}
	testConfig.TestIntegration(t)
}
