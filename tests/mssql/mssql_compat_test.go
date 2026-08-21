package mssql

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestMSSQLCompatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
func TestMSSQLCompatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: mssqlBaseConfig()}
		cfg.IntegrationTest.ExpectedUpdatedData = ExpectedUpdatedMSSQLData
		cfg.IntegrationTest.UpdatedDestinationDataTypeSchema = MSSQLToDestinationSchema
		return cfg
	})
}
