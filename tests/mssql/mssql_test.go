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

// mssqlSyncConfig adds to mssqlBaseConfig what the sync suites verify after the update sync.
func mssqlSyncConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	cfg := mssqlBaseConfig(t, opts...)
	cfg.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
	cfg.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
	return cfg
}

func TestMSSQLSync(t *testing.T) {
	t.Parallel()
	mssqlSyncConfig(t).TestSync(t)
}

// TestMSSQLDecimalSync partitions and filters on DECIMAL(18,2) and MONEY columns. The filter cases use the
// decimal cursor; the 19 digit MONEY filter value is a string.
func TestMSSQLDecimalSync(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		configure func(cfg *integration.TestHandler)
	}{
		{
			name: "partition",
			configure: func(cfg *integration.TestHandler) {
				cfg.PartitionRegex = "/{col_decimal,identity}"
			},
		},
		{
			// keeps only the filtered rows and the updated row the update phase asserts; the bound sits a
			// ten-thousandth below the filtered value
			name: "filter_inclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "col_cursor_decimal:id_cursor"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "col_decimal",
                            "operator": ">",
                            "value": 239835.8899
                        },
                        {
                            "column": "col_decimal",
                            "operator": "=",
                            "value": 543.25
                        }
                    ]
                }`
				cfg.ExpectedData = ExpectedFilteredMSSQLData()
			},
		},
		{
			// drops the filtered rows; the updated row drops out too if its 19 digit MONEY filter value rounds
			// through a double
			name: "filter_exclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "col_cursor_decimal:id_cursor"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "col_decimal",
                            "operator": "=",
                            "value": "123.50"
                        },
                        {
                            "column": "col_money",
                            "operator": "=",
                            "value": "922337203685477.5807"
                        }
                    ]
                }`
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := mssqlSyncConfig(t)
			tc.configure(cfg)
			cfg.TestCommonSync(t)
			if !testutils.KeepTestData() {
				cfg.ExecuteQuery(t.Context(), t, cfg.TestConfig, "drop")
			}
		})
	}
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
