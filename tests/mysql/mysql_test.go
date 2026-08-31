package mysql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// mysqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the mysql suites.
func mysqlBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.MySQL, "olake_mysql_test", "mysql_olake_mysql_test", ExecuteQuery, opts...)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "id_cursor:id_smallint"
	cfg.PartitionRegex = "/{id,identity}"
	cfg.ColumnToExclude = "excludedColumn"
	cfg.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "price_double",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "created_timestamp",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`

	return &integration.Test{
		TestConfig:                cfg,
		ExpectedData:              ExpectedMySQLData,
		DestinationDataTypeSchema: MySQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMySQLDefaultCDCColumnsSchema,
	}
}

func TestMySQLDiscover(t *testing.T) {
	mysqlBaseConfig(t).TestDiscover(t)
}

func TestMySQLSync(t *testing.T) {
	t.Parallel()
	cfg := mysqlBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
	cfg.TestSync(t)
}

func TestMySQL2PC(t *testing.T) {
	t.Parallel()
	mysqlBaseConfig(t).Test2PCIntegration(t)
}

func TestMySQLPerformance(t *testing.T) {
	cfg, err := testutils.NewTestConfig(t, constants.MySQL, "benchmark", "", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")

	perf := &performance.Test{
		TestConfig:      cfg,
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}

	perf.TestPerformance(t)
}

// TestMySQLCompatibility pins the backward-compatibility contract for the driver that owns three of the
// six version gates -- the binlog timestamp location (v2), the timezone offset (v3) and the
// UNSIGNED widening (v4), see constants/state_version.go.
func TestMySQLCompatibility(t *testing.T) {
	t.Parallel()
	fixture := &compatibility.Test{
		DeclaredSchema:   MySQLToDestinationSchema,
		CDCColumnsSchema: ExpectedMySQLDefaultCDCColumnsSchema,
		ColumnTypes:      seedColumnTypes(),
	}
	fixture.NewConfig = func(t *testing.T, version string) *testutils.TestConfig {
		return mysqlBaseConfig(t, testutils.WithDriverVersion(version)).TestConfig
	}
	fixture.RunBackwardCompatibility(t)
}
