package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
)

// mssqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the mssql suites.
func mssqlBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                &testutils.TestConfig{Driver: string(constants.MSSQL)},
		ExpectedData:              ExpectedMSSQLData,
		DestinationDataTypeSchema: MSSQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMSSQLDefaultCDCColumnsSchema,
	}
	cfg.TestConfig.Namespace = "dbo"
	cfg.TestConfig.ExecuteQuery = ExecuteQuery
	cfg.TestConfig.ColumnToExclude = "excludedColumn"
	cfg.TestConfig.DestinationDB = "mssql_olake_mssql_test_dbo"
	cfg.TestConfig.CursorField = "id_cursor:col_int"
	cfg.TestConfig.PartitionRegex = "/{id,identity}"
	cfg.TestConfig.FilterConfig = `{
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
                }`
	return cfg
}

func TestMSSQLDiscover(t *testing.T) {
	mssqlBaseConfig().TestDiscover(t)
}

func TestMSSQLSync(t *testing.T) {
	t.Parallel()
	cfg := mssqlBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
	cfg.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
	cfg.TestSync(t)
}

func TestMSSQL2PC(t *testing.T) {
	t.Parallel()
	mssqlBaseConfig().Test2PCIntegration(t)
}
