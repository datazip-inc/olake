package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/compatibility"
)

// TestMongodbCompatibility pins the backward-compatibility contract for the driver owning the v5 gate
// (BSON DateTime decoded as UTC time.Time at any depth, constants/state_version.go). v0.6.1 is
// the newest release still on state version 4, so it is the one that exercises it.
//
// _id and _olake_id are volatile here, unlike every other driver: the seed inserts documents
// without an _id, so the server generates a fresh ObjectID per run and _olake_id, which hashes the
// primary key, follows it. Both are still compared by TYPE -- only their values are exempt.
func TestMongodbCompatibility(t *testing.T) {
	t.Parallel()
	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
		cfg := &compatibility.Test{IntegrationTest: mongodbBaseConfig()}
		cfg.IntegrationTest.ExpectedUpdatedData = ExpectedUpdatedData
		cfg.IntegrationTest.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
		cfg.ExtraVolatileColumns = []string{"_id", "_olake_id"}
		// Type tags for compatibility_rules.json's mongodb rules (G1: id_regex value change at #657).
		cfg.ColumnTypes = map[string][]string{"id_regex": {"regex"}}
		return cfg
	})
}
