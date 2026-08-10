package mysql

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// mysqlBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestMySQLIntegration and TestMySQL2PC.
func mysqlBaseConfig(t *testing.T) *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(t, string(constants.MySQL)),
		Namespace:                 "olake_mysql_test",
		ExpectedData:              ExpectedMySQLData,
		DestinationDataTypeSchema: MySQLToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMySQLDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "mysql_olake_mysql_test",
		CursorField:               "id_cursor:id_smallint",
		PartitionRegex:            "/{id,identity}",
		ColumnToExclude:           "excludedColumn",
		FilterConfig: `{
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
                }`,
	}
}

func TestMySQLIntegration(t *testing.T) {
	t.Parallel()
	cfg := mysqlBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
	cfg.TestIntegration(t)
}

func TestMySQL2PC(t *testing.T) {
	t.Parallel()
	mysqlBaseConfig(t).Test2PCIntegration(t)
}

// TestMySQLCompat pins the backward-compatibility contract for the driver that owns three of the
// six version gates -- the binlog timestamp location (v2), the timezone offset (v3) and the
// UNSIGNED widening (v4), see constants/state_version.go. Note that a passing run is the
// contract HOLDING: the candidate reading a state file at version N reproduces version N's types,
// so it agrees with the baseline. A diff here means a gate stopped firing.
//
// Baseline defaults to the newest release; OLAKE_COMPAT_BASELINE picks another tag, image or
// commit. v0.4.0 is the newest release still on state version 3, so it is the one that exercises
// the UNSIGNED gate.
func TestMySQLCompat(t *testing.T) {
	t.Parallel()
	testutils.RunBackwardCompat(t, func(t *testing.T) *testutils.IntegrationTest {
		cfg := mysqlBaseConfig(t)
		cfg.ExpectedUpdatedData = ExpectedUpdatedData
		cfg.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
		// Every known mysql finding, as data (COMPAT_RESULTS_v2.md). The ExcludeBelow columns are
		// the ones #940 ("fix CDC charset corruption for utf16/ucs2/latin1 columns", v0.7.2) added
		// as its own regression test: an older baseline hands their raw bytes to the Iceberg
		// writer as invalid UTF-8, the gRPC marshal fails, and the driver retries on a doubling
		// backoff that looks like a hang -- a hard fail, so they stay out of the seed data
		// entirely. The AssertValueFrom columns synced fine all along but changed value form at
		// the named release, so below it they are compared by type only: SET columns emitted the
		// numeric bitmask on the binlog path before #940 (M1), ENUMs serialised differently before
		// v0.3.9 (M2), and DECIMAL/NUMERIC round-tripped through float32 before v0.3.7 (M3).
		cfg.CompatColumnRules = []testutils.CompatColumnRule{
			{Column: "name_ucs2", ExcludeBelow: "v0.7.2"},
			{Column: "name_utf16le", ExcludeBelow: "v0.7.2"},
			{Column: "grade", ExcludeBelow: "v0.7.2"},
			{Column: "permissions", AssertValueFrom: "v0.7.2"},
			{Column: "tags", AssertValueFrom: "v0.7.2"},
			{Column: "priority", AssertValueFrom: "v0.3.9"},
			{Column: "status", AssertValueFrom: "v0.3.9"},
			{Column: "price_decimal", AssertValueFrom: "v0.3.7"},
			{Column: "amount_decimal_9_2", AssertValueFrom: "v0.3.7"},
			{Column: "price_numeric", AssertValueFrom: "v0.3.7"},
		}
		// The closure reads SeedExcludedColumns at call time; RunBackwardCompat fills it in after
		// resolving the rules above against the baseline.
		cfg.SupportsSeedExclusion = true
		cfg.ExecuteQuery = func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
			ExecuteQueryExcluding(ctx, t, streams, operation, fileConfig, cfg.SeedExcludedColumns)
		}
		// The filter stays on. It used to be cleared here because v0.4.0 synced the id=999 row
		// that HEAD filtered away -- an 8-vs-7 row count that masked everything behind it. That
		// was the input format, not the binary: filter_config arrived in v0.6.0, so v0.4.0 never
		// saw the key. RunBackwardCompat now writes the baseline's own input generation, which
		// hands a pre-v0.6.0 baseline the legacy `filter` string both binaries honour identically.
		return cfg
	})
}

func TestMySQLPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(t, string(constants.MySQL)),
		Namespace:       "benchmark",
		BackfillStreams: []string{"trips", "fhv_trips"},
		CDCStreams:      []string{"trips_cdc", "fhv_trips_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
