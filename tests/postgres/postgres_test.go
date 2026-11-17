package postgres

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
	_ "github.com/lib/pq"
)

// postgresTestConfig builds the config every postgres suite shares: the source, the namespace and the stream settings.
func postgresTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.Postgres, "public", ExecuteQuery, opts...)
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

	return cfg
}

// postgresBaseConfig is postgresTestConfig with the integration suites' expected data.
func postgresBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                postgresTestConfig(t, opts...),
		ExpectedData:              ExpectedPostgresData,
		DestinationDataTypeSchema: PostgresToDestinationSchema,
		TypeMapping:               PostgresTypeMapping,
		DefaultCDCColumnsSchema:   ExpectedPostgresDefaultCDCColumnsSchema,
	}
}

func createReplicationSlot(t *testing.T, cfg *testutils.TestConfig) {
	t.Helper()
	ctx := t.Context()
	ExecuteQuery(ctx, t, cfg, "create-slot")
	t.Cleanup(func() {
		ExecuteQuery(context.WithoutCancel(ctx), t, cfg, "drop-slot")
	})
}

func TestPostgresDiscover(t *testing.T) {
	cfg := postgresBaseConfig(t)
	createReplicationSlot(t, cfg.TestConfig)
	cfg.TestDiscover(t)
}

func TestPostgresUnchangedToastRecovery(t *testing.T) {
	t.Parallel()
	cfg := postgresBaseConfig(t)
	cfg.UpdatedDestinationDataTypeSchema = UpdatedPostgresToDestinationSchema
	createReplicationSlot(t, cfg.TestConfig)
	cfg.TestUnchangedToastRecovery(t)
}

// postgresSyncConfig adds to postgresBaseConfig what the sync suites verify after the update sync, and
// the replication slot the suite's source config points at.
func postgresSyncConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	cfg := postgresBaseConfig(t, opts...)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.ExpectedCDCUpdatedData = ExpectedCDCUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedPostgresToDestinationSchema
	createReplicationSlot(t, cfg.TestConfig)
	return cfg
}

func TestPostgresSync(t *testing.T) {
	t.Parallel()
	postgresSyncConfig(t).TestSync(t)
}

// TestPostgresDecimalSync partitions and filters on a NUMERIC(18,4) column that evolve-schema widens to NUMERIC(20,4).
// The filter cases use the decimal cursor so incremental reads the filtered insert row too; the 18 digit filter value is a string.
func TestPostgresDecimalSync(t *testing.T) {
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
			// keeps only the filtered rows and the updated row the update phase asserts; each bound sits a
			// ten-thousandth from a kept value
			name: "filter_inclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "col_cursor_decimal:col_int"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "col_decimal",
                            "operator": "<",
                            "value": 50.1231
                        },
                        {
                            "column": "col_decimal",
                            "operator": ">",
                            "value": 543.2099
                        }
                    ]
                }`
				cfg.ExpectedData = ExpectedFilteredData
			},
		},
		{
			// drops the filtered rows; the 18 digit updated row drops out too if its filter value rounds
			// through a double
			name: "filter_exclusive",
			configure: func(cfg *integration.TestHandler) {
				cfg.CursorField = "col_cursor_decimal:col_int"
				cfg.FilterConfig = `{
                    "logical_operator": "Or",
                    "conditions": [
                        {
                            "column": "col_decimal",
                            "operator": "=",
                            "value": "123.4500"
                        },
                        {
                            "column": "col_decimal",
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
			cfg := postgresSyncConfig(t)
			tc.configure(cfg)
			cfg.TestCommonSync(t)
			if !testutils.KeepTestData() {
				cfg.ExecuteQuery(t.Context(), t, cfg.TestConfig, "drop")
			}
		})
	}
}

func TestPostgres2PC(t *testing.T) {
	t.Parallel()
	cfg := postgresBaseConfig(t)
	createReplicationSlot(t, cfg.TestConfig)
	cfg.Test2PCIntegration(t)
}

func TestPostgresIcebergPosIncremental(t *testing.T) {
	t.Parallel()
	cfg := postgresBaseConfig(t)
	createReplicationSlot(t, cfg.TestConfig)
	cfg.TestIcebergPosIncremental(t)
}

func TestPostgresPerformance(t *testing.T) {
	cfg, err := testutils.NewTestConfig(t, constants.Postgres, "public", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")

	perf := &performance.TestHandler{
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
	testHandler := &compatibility.TestHandler{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			cfg := postgresTestConfig(t, testutils.WithDriverVersion(version))
			createReplicationSlot(t, cfg)
			return cfg
		},
		DestinationSchema: PostgresToDestinationSchema,
		CDCColumnsSchema:  ExpectedPostgresDefaultCDCColumnsSchema,
	}
	testHandler.RunBackwardCompatibility(t)
}
