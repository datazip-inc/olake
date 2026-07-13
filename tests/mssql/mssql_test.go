package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// mssqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between the mssql integration tests.
func mssqlBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig("mssql"),
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

func TestDiscover(t *testing.T) {
	mssqlBaseConfig().TestDiscover(t)
}

func TestSync(t *testing.T) {
	cfg := mssqlBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
	cfg.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
	cfg.TestSync(t)
}

func Test2PC(t *testing.T) {
	mssqlBaseConfig().Test2PCIntegration(t)
}
