package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// oracleTestConfig builds the config every oracle suite shares: the source, the namespace and the stream settings.
func oracleTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.Oracle, "MYUSER", ExecuteQuery, opts...)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "COL_CURSOR:COL_SMALLINT"
	cfg.PartitionRegex = "/{id, identity}"
	cfg.ColumnToExclude = "EXCLUDEDCOLUMN"
	cfg.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "COL_DOUBLE_PRECISION",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "COL_TIMESTAMP",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`

	return cfg
}

// oracleBaseConfig is oracleTestConfig with the integration suites' expected data.
func oracleBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                oracleTestConfig(t, opts...),
		ExpectedData:              ExpectedOracleData,
		DestinationDataTypeSchema: OracleToDestinationSchema,
		TypeMapping:               OracleTypeMapping,
	}
}

func TestOracleDiscover(t *testing.T) {
	oracleBaseConfig(t).TestDiscover(t)
}

// oracleSyncConfig adds to oracleBaseConfig what the sync suites verify after the update sync.
func oracleSyncConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	cfg := oracleBaseConfig(t, opts...)
	cfg.ExpectedUpdatedData = ExpectedUpdatedOracleData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedOracleToDestinationSchema
	return cfg
}

func TestOracleSync(t *testing.T) {
	t.Parallel()
	oracleSyncConfig(t).TestSync(t)
}

// TestOracleDecimalSync partitions and filters on a NUMBER(18,4) column. The filter cases use the decimal
// cursor so incremental reads the filtered insert row too; the 18 digit filter value is a string.
func TestOracleDecimalSync(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		configure func(cfg *integration.TestHandler)
	}{
		{
			name: "partition",
			configure: func(cfg *integration.TestHandler) {
				cfg.PartitionRegex = "/{col_decimal, identity}"
			},
		},
		{
			// keeps only the filtered rows and the updated row the update phase asserts; each bound sits a
			// ten-thousandth from a kept value
			name: "filter_inclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "COL_CURSOR_DECIMAL:COL_SMALLINT"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "COL_DECIMAL",
                            "operator": "<",
                            "value": 50.1231
                        },
                        {
                            "column": "COL_DECIMAL",
                            "operator": ">",
                            "value": 543.2099
                        }
                    ]
                }`
				cfg.ExpectedData = ExpectedFilteredOracleData
			},
		},
		{
			// drops the filtered rows; the 18 digit updated row drops out too if its filter value rounds
			// through a double
			name: "filter_exclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "COL_CURSOR_DECIMAL:COL_SMALLINT"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "COL_DECIMAL",
                            "operator": "=",
                            "value": "123.4500"
                        },
                        {
                            "column": "COL_DECIMAL",
                            "operator": "=",
                            "value": "12345678901234.5678"
                        }
                    ]
                }`
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := oracleSyncConfig(t)
			tc.configure(cfg)
			cfg.TestCommonSync(t)
			if !testutils.KeepTestData() {
				cfg.ExecuteQuery(t.Context(), t, cfg.TestConfig, "drop")
			}
		})
	}
}

func TestOracle2PC(t *testing.T) {
	t.Parallel()
	oracleBaseConfig(t).Test2PCIntegration(t)
}

// TestOracleCompatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
func TestOracleCompatibility(t *testing.T) {
	t.Parallel()
	testHandler := &compatibility.TestHandler{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return oracleTestConfig(t, testutils.WithDriverVersion(version))
		},
		DestinationSchema: OracleToDestinationSchema,
	}
	testHandler.RunBackwardCompatibility(t)
}
