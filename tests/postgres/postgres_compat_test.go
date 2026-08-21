package postgres

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestPostgresCompatibility pins the backward-compatibility contract: the same scenarios run twice in
// parallel -- once entirely on a released baseline image, once handing off to this build after the
// initial load -- and the two destinations must match. The baseline defaults to the newest
// release; OLAKE_COMPATIBILITY_BASELINE picks another tag, image or commit. See tests/testutils/compatibility.go.
func TestPostgresCompatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: postgresBaseConfig()}
		cfg.IntegrationTest.ExpectedUpdatedData = ExpectedUpdatedData
		cfg.IntegrationTest.UpdatedDestinationDataTypeSchema = UpdatedPostgresToDestinationSchema
		// No column rules: postgres compares clean on every reachable baseline (COMPAT_RESULTS_v2.md).
		// The OLAKE_COMPATIBILITY_EXCLUDE_COLUMNS sweep hook lives in RunBackwardCompatibility now.
		return cfg
	})
}
