package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// mssqlTestConfig builds the config every mssql suite shares: the source, the namespace and the stream settings.
func mssqlTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.MSSQL, "dbo", ExecuteQuery, opts...)
	require.NoError(t, err, "failed to build the test config")
	cfg.ColumnToExclude = "excludedColumn"
	cfg.SkipSchemaEvolution = true
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

	return cfg
}

// mssqlBaseConfig is mssqlTestConfig with the integration suites' expected data.
func mssqlBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                mssqlTestConfig(t, opts...),
		ExpectedData:              ExpectedMSSQLData,
		DestinationDataTypeSchema: MSSQLToDestinationSchema,
		TypeMapping:               MSSQLTypeMapping,
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
func TestMSSQLCompatibility(t *testing.T) {
	t.Parallel()

	testHandler := &compatibility.TestHandler{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return mssqlTestConfig(t, testutils.WithDriverVersion(version))
		},
		DestinationSchema: MSSQLToDestinationSchema,
		CDCColumnsSchema:  ExpectedMSSQLDefaultCDCColumnsSchema,
	}

	testHandler.RunBackwardCompatibility(t)
}
