package db2

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestDB2Compatibility pins the backward-compatibility contract: the same scenarios run on a released
// baseline image and on this build after the initial load, and the destinations must match.
// See tests/testutils/compatibility.go.
func TestDB2Compatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: db2BaseConfig()}
		cfg.IntegrationTest.ExpectedUpdatedData = ExpectedUpdatedDB2Data
		cfg.IntegrationTest.UpdatedDestinationDataTypeSchema = UpdatedDB2ToDestinationSchema
		// Type tags for compatibility_rules.json's db2 rules; floor and descriptions live there too.
		cfg.ColumnTypes = map[string][]string{"col_decfloat": {"decfloat"}}
		return cfg
	})
}
