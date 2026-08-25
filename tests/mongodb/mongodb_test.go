package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
func mongodbBaseConfig(t *testing.T) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.MongoDB, "olake_mongodb_test", "mongodb_olake_mongodb_test", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")
	cfg.CursorField = "id_cursor:id_int"
	cfg.PartitionRegex = "/{_id,identity}"
	cfg.ColumnToExclude = "excludedColumn"
	cfg.FilterConfig = `{
			"logical_operator": "And",
			"conditions": [
				{
					"column": "id_double",
					"operator": "<",
					"value": 239834.89
				},
				{
					"column": "id_timestamp",
					"operator": ">=",
					"value": "2022-07-01T15:30:00.000+00:00"
				}
			]
		}`

	return &integration.Test{
		TestConfig:                cfg,
		ExpectedData:              ExpectedMongoData,
		DestinationDataTypeSchema: MongoToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMongoDBDefaultCDCColumnsSchema,
	}
}

func TestMongodbDiscover(t *testing.T) {
	mongodbBaseConfig(t).TestDiscover(t)
}

func TestMongodbSync(t *testing.T) {
	t.Parallel()
	cfg := mongodbBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
	cfg.TestSync(t)
}

func TestMongodb2PC(t *testing.T) {
	t.Parallel()
	mongodbBaseConfig(t).Test2PCIntegration(t)
}

// func TestMongodbPerformance(t *testing.T) {
// 	cfg, err := testutils.NewTestConfig(constants.MongoDB, "twitter_data", "", ExecuteQuery, "")
// 	require.NoError(t, err, "failed to build the test config")

// 	perf := &performance.Test{
// 		TestConfig:      cfg,
// 		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
// 		CDCStreams:      performanceCDCStreams,
// 	}

// 	perf.TestPerformance(t)
// }

// TestMongodbCompatibility pins the backward-compatibility contract for the driver owning the v5 gate
// (BSON DateTime decoded as UTC time.Time at any depth, constants/state_version.go). v0.6.1 is
// the newest release still on state version 4, so it is the one that exercises it.
//
// _id and _olake_id are volatile here, unlike every other driver: the seed inserts documents
// without an _id, so the server generates a fresh ObjectID per run and _olake_id, which hashes the
// primary key, follows it. Both are still compared by TYPE -- only their values are exempt.
// func TestMongodbCompatibility(t *testing.T) {
// 	t.Parallel()
// 	compatibility.RunBackwardCompatibility(t, func() *compatibility.Test {
// 		base := mongodbBaseConfig(t)
// 		base.ExpectedUpdatedData = ExpectedUpdatedData
// 		base.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
// 		cfg := &compatibility.Test{IntegrationTest: base}
// 		cfg.ExtraVolatileColumns = []string{"_id", "_olake_id"}
// 		// Type tags for compatibility_rules.json's mongodb rules (G1: id_regex value change at #657).
// 		cfg.ColumnTypes = map[string][]string{"id_regex": {"regex"}}
// 		return cfg
// 	})
// }
