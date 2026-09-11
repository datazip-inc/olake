package db2

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// db2TestConfig builds the config every db2 suite shares: the source, the namespace and the stream settings.
func db2TestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.DB2, "DB2INST1", ExecuteQuery,
		append([]testutils.TestConfigOption{testutils.WithImagePlatform("linux/amd64")}, opts...)...)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "COL_CURSOR:COL_TIMESTAMP"
	cfg.PartitionRegex = "/{id, identity}"
	cfg.ColumnToExclude = "EXCLUDEDCOLUMN"
	cfg.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "COL_DOUBLE",
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

// db2BaseConfig is db2TestConfig with the integration suites' expected data.
func db2BaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                db2TestConfig(t, opts...),
		ExpectedData:              ExpectedDB2Data,
		DestinationDataTypeSchema: DB2ToDestinationSchema,
		TypeMapping:               DB2TypeMapping,
	}
}

func TestDB2Discover(t *testing.T) {
	db2BaseConfig(t).TestDiscover(t)
}

func TestDB2Sync(t *testing.T) {
	t.Parallel()
	cfg := db2BaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedDB2Data
	cfg.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
	cfg.TestSync(t)
}

func TestDB22PC(t *testing.T) {
	t.Parallel()
	db2BaseConfig(t).Test2PCIntegration(t)
}

// TestDB2Compatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
func TestDB2Compatibility(t *testing.T) {
	t.Parallel()
	testHandler := &compatibility.TestHandler{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return db2TestConfig(t, testutils.WithDriverVersion(version))
		},
		DestinationSchema: DB2ToDestinationSchema,
		ColumnTypes:       seedColumnTypes(),
	}
	testHandler.RunBackwardCompatibility(t)
}
