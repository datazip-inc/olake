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

func TestOracleSync(t *testing.T) {
	t.Parallel()
	cfg := oracleBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedOracleData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedOracleToDestinationSchema
	cfg.TestSync(t)
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
