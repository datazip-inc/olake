package mysql

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestMySQLCompatibility pins the backward-compatibility contract for the driver that owns three of the
// six version gates -- the binlog timestamp location (v2), the timezone offset (v3) and the
// UNSIGNED widening (v4), see constants/state_version.go. Note that a passing run is the
// contract HOLDING: the candidate reading a state file at version N reproduces version N's types,
// so it agrees with the baseline. A diff here means a gate stopped firing.
//
// Baseline defaults to the newest release; OLAKE_COMPATIBILITY_BASELINE picks another tag, image or
// commit. v0.4.0 is the newest release still on state version 3, so it is the one that exercises
// the UNSIGNED gate.
func TestMySQLCompatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: mysqlBaseConfig()}
		cfg.IntegrationTest.ExpectedUpdatedData = ExpectedUpdatedData
		cfg.IntegrationTest.UpdatedDestinationDataTypeSchema = EvolvedMySQLToDestinationSchema
		// Every known mysql finding, as data (COMPAT_RESULTS_v2.md). The ExcludeBelow columns are
		// the ones #940 ("fix CDC charset corruption for utf16/ucs2/latin1 columns", v0.7.2) added
		// as its own regression test: an older baseline hands their raw bytes to the Iceberg
		// writer as invalid UTF-8, the gRPC marshal fails, and the driver retries on a doubling
		// backoff that looks like a hang -- a hard fail, so they stay out of the seed data
		// entirely. The AssertValueFrom columns synced fine all along but changed value form at
		// the named release, so below it they are compared by type only: SET columns emitted the
		// numeric bitmask on the binlog path before #940 (M1), ENUMs serialized differently before
		// v0.3.9 (M2), and DECIMAL/NUMERIC round-tripped through float32 before v0.3.7 (M3).
		// The closure reads SeedExcludedColumns at call time; RunBackwardCompatibility fills it in after
		// resolving the rules above against the baseline.
		cfg.SupportsSeedExclusion = true
		cfg.IntegrationTest.TestConfig.ExecuteQuery = func(ctx context.Context, t *testing.T, conf *testutils.TestConfig, operation string) {
			ExecuteQueryExcluding(ctx, t, conf, operation, cfg.SeedExcludedColumns)
		}
		// The filter stays on. It used to be cleared here because v0.4.0 synced the id=999 row
		// that HEAD filtered away -- an 8-vs-7 row count that masked everything behind it. That
		// was the input format, not the binary: filter_config arrived in v0.6.0, so v0.4.0 never
		// saw the key. RunBackwardCompatibility now writes the baseline's own input generation, which
		// hands a pre-v0.6.0 baseline the legacy `filter` string both binaries honor identically.
		return cfg
	})
}
