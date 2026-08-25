package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// mssqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the mssql suites.
func mssqlBaseConfig(t *testing.T) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.MSSQL, "dbo", "mssql_olake_mssql_test_dbo", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")
	cfg.ColumnToExclude = "excludedColumn"
	cfg.CursorField = "id_cursor:col_int"
	cfg.PartitionRegex = "/{id,identity}"
	cfg.FilterConfig = `{
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

	return &integration.Test{
		TestConfig:                cfg,
		ExpectedData:              ExpectedMSSQLData,
		DestinationDataTypeSchema: MSSQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMSSQLDefaultCDCColumnsSchema,
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

// TestMSSQLCompatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
// func TestMSSQLCompatibility(t *testing.T) {
// 	t.Parallel()
// 	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
// 		base := mssqlBaseConfig(t)
// 		base.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
// 		base.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
// 		cfg := &compatibility.Test{IntegrationTest: base}
// 		return cfg
// 	})
// }
