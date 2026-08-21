package oracle

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestOracleCompatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
func TestOracleCompatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: oracleBaseConfig()}
		cfg.IntegrationTest.ExpectedUpdatedData = ExpectedUpdatedOracleData
		cfg.IntegrationTest.UpdatedDestinationDataTypeSchema = UpdatedOracleToDestinationSchema
		// No floor declared: oracle images exist for every sweep baseline. If a first sweep finds
		// an unrunnable band, declare it here with its reason.
		return cfg
	})
}
