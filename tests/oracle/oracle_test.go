package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// oracleBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the oracle suites.
func oracleBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.Oracle, "MYUSER", "oracle_myuser", ExecuteQuery, opts...)
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

	return &integration.Test{
		TestConfig:                cfg,
		ExpectedData:              ExpectedOracleData,
		DestinationDataTypeSchema: OracleToDestinationSchema,
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
	fixture := &compatibility.Test{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return oracleBaseConfig(t, testutils.WithDriverVersion(version)).TestConfig
		},
		DeclaredSchema: OracleToDestinationSchema,
	}
	fixture.RunBackwardCompatibility(t)
}
