package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestMongodbIntegration and TestMongodb2PC.
func mongodbBaseConfig(t *testing.T) *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(t, string(constants.MongoDB)),
		Namespace:                 "olake_mongodb_test",
		ExpectedData:              ExpectedMongoData,
		DestinationDataTypeSchema: MongoToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMongoDBDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQuery,
		DestinationDB:             "mongodb_olake_mongodb_test",
		CursorField:               "id_cursor:id_int",
		PartitionRegex:            "/{_id,identity}",
		ColumnToExclude:           "excludedColumn",
		FilterConfig: `{
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
		}`,
	}
}

func TestMongodbIntegration(t *testing.T) {
	t.Parallel()
	cfg := mongodbBaseConfig(t)
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
	cfg.TestIntegration(t)
}

func TestMongodb2PC(t *testing.T) {
	t.Parallel()
	mongodbBaseConfig(t).Test2PCIntegration(t)
}

// TestMongodbCompat pins the backward-compatibility contract for the driver owning the v5 gate
// (BSON DateTime decoded as UTC time.Time at any depth, constants/state_version.go). v0.6.1 is
// the newest release still on state version 4, so it is the one that exercises it.
//
// _id and _olake_id are volatile here, unlike every other driver: the seed inserts documents
// without an _id, so the server generates a fresh ObjectID per run and _olake_id, which hashes the
// primary key, follows it. Both are still compared by TYPE -- only their values are exempt.
func TestMongodbCompat(t *testing.T) {
	t.Parallel()
	testutils.RunBackwardCompat(t, func(t *testing.T) *testutils.IntegrationTest {
		cfg := mongodbBaseConfig(t)
		cfg.ExpectedUpdatedData = ExpectedUpdatedData
		cfg.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
		cfg.ExtraVolatileColumns = []string{"_id", "_olake_id"}
		// G1 (COMPAT_RESULTS_v2.md): BSON regex serialised with Go field names, not lowercase
		// keys, until #657 -- an ungated value change, so older baselines are type-only on it.
		cfg.CompatColumnRules = []testutils.CompatColumnRule{
			{Column: "id_regex", AssertValueFrom: "v0.3.14"},
		}
		return cfg
	})
}

func TestMongodbPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(t, string(constants.MongoDB)),
		Namespace:       "twitter_data",
		BackfillStreams: []string{"tweets"},
		CDCStreams:      []string{"tweets_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
