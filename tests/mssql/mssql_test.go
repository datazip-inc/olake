package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// mssqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the mssql suites.
func mssqlBaseConfig(t *testing.T) *testutils.IntegrationTest {
	testConf := testutils.GetTestConfig(t, string(constants.MSSQL))
	return &testutils.IntegrationTest{
		TestConfig:                testConf,
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

func TestMSSQLDiscover(t *testing.T) {
	mssqlBaseConfig(t).TestDiscover(t)
}

func TestMSSQLSync(t *testing.T) {
	t.Parallel()
	cfg := mssqlBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
	cfg.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
	cfg.TestSync(t)
}

func TestMSSQL2PC(t *testing.T) {
	t.Parallel()
	mssqlBaseConfig(t).Test2PCIntegration(t)
}
