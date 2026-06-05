package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

// mssqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestMSSQLIntegration and TestMSSQL2PC.
func mssqlBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(string(constants.MSSQL)),
		Namespace:                 "dbo",
		ExpectedData:              ExpectedMSSQLData,
		DestinationDataTypeSchema: MSSQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMSSQLDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQuery,
		ColumnToExclude:           "excludedColumn",
		DestinationDB:             "mssql_olake_mssql_test_dbo",
		CursorField:               "id_cursor:col_int",
		PartitionRegex:            "/{id,identity}",
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
}

func TestMSSQLIntegration(t *testing.T) {
	t.Parallel()
	cfg := mssqlBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
	cfg.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
	cfg.TestIntegration(t)
}

func TestMSSQL2PC(t *testing.T) {
	t.Parallel()
	mssqlBaseConfig().Test2PCIntegration(t)
}
