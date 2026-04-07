package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMSSQLIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(string(constants.MSSQL), ""),
		Namespace:                        "dbo",
		ExpectedData:                     ExpectedMSSQLData,
		ExpectedUpdatedData:              ExpectedUpdatedMSSQLData,
		DestinationDataTypeSchema:        MSSQLToDestinationSchema,
		UpdatedDestinationDataTypeSchema: MSSQLToDestinationSchema,
		DefaultCDCColumnsSchema:          ExpectedMSSQLDefaultCDCColumnsSchema,
		ExecuteQuery:                     ExecuteQuery,
		ColumnToExclude:                  "excludedColumn",
		DestinationDB:                    "mssql_olake_mssql_test_dbo",
		CursorField:                      "id_cursor:col_int",
		PartitionRegex:                   "/{id,identity}",
		FilterConfig: `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "col_decimal",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "created_at",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`,
	}
	testConfig.TestIntegration(t)
}
