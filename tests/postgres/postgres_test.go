package postgres

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
	_ "github.com/lib/pq"
)

// postgresBaseConfig returns an IntegrationTest pre-populated with all fields shared
// by the postgres suites.
func postgresBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.Postgres, "public", "postgres_postgres_public", ExecuteQuery, opts...)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "col_cursor:col_int"
	cfg.PartitionRegex = "/{col_bigserial,identity}"
	cfg.ColumnToExclude = "excludedcolumn"
	cfg.FilterConfig = `{
                    "logical_operator": "And",
                    "conditions": [
                        {
                            "column": "col_double_precision",
                            "operator": "<",
                            "value": 239834.89
                        },
                        {
                            "column": "col_timestamp",
                            "operator": ">=",
                            "value": "2022-07-01T15:30:00.000+00:00"
                        }
                    ]
                }`

	return &integration.Test{
		TestConfig:                cfg,
		ExpectedData:              ExpectedPostgresData,
		DestinationDataTypeSchema: PostgresToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedPostgresDefaultCDCColumnsSchema,
	}
}

func TestPostgresDiscover(t *testing.T) {
	postgresBaseConfig(t).TestDiscover(t)
}

func TestPostgresSync(t *testing.T) {
	t.Parallel()
	cfg := postgresBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedPostgresToDestinationSchema
	cfg.TestSync(t)
}

func TestPostgres2PC(t *testing.T) {
	t.Parallel()
	postgresBaseConfig(t).Test2PCIntegration(t)
}

func TestPostgresPerformance(t *testing.T) {
	cfg, err := testutils.NewTestConfig(t, constants.Postgres, "public", "", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")

	perf := &performance.Test{
		TestConfig:      cfg,
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}

	perf.TestPerformance(t)
}

// TestPostgresCompatibility pins the backward-compatibility contract: the same scenarios run twice in
// parallel -- once entirely on a released baseline image, once handing off to this build after the
// initial load -- and the two destinations must match. The baseline defaults to the newest
// release; OLAKE_COMPATIBILITY_BASELINE picks another tag, image or commit. See tests/testutils/compatibility.go.
func TestPostgresCompatibility(t *testing.T) {
	t.Parallel()
	// No column rules: postgres compares clean on every reachable baseline (COMPAT_RESULTS_v2.md).
	fixture := &compatibility.Test{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return postgresBaseConfig(t, testutils.WithDriverVersion(version)).TestConfig
		},
		DeclaredSchema:   PostgresToDestinationSchema,
		CDCColumnsSchema: ExpectedPostgresDefaultCDCColumnsSchema,
	}
	fixture.RunBackwardCompatibility(t)
}
