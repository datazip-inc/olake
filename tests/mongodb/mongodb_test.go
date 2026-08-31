package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
func mongodbBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.MongoDB, "olake_mongodb_test", "mongodb_olake_mongodb_test", ExecuteQuery, opts...)
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

func TestMongodbPerformance(t *testing.T) {
	cfg, err := testutils.NewTestConfig(t, constants.MongoDB, "twitter_data", "", ExecuteQuery)
	require.NoError(t, err, "failed to build the test config")

	perf := &performance.Test{
		TestConfig:      cfg,
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}

	perf.TestPerformance(t)
}

// TestMongodbCompatibility pins the backward-compatibility contract for the driver owning the v5 gate
// (BSON DateTime decoded as UTC time.Time at any depth, constants/state_version.go). v0.6.1 is
// the newest release still on state version 4, so it is the one that exercises it.
func TestMongodbCompatibility(t *testing.T) {
	t.Parallel()
	fixture := &compatibility.Test{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return mongodbBaseConfig(t, testutils.WithDriverVersion(version)).TestConfig
		},
		DeclaredSchema:   MongoToDestinationSchema,
		ColumnTypes:      seedColumnTypes(),
		CDCColumnsSchema: ExpectedMongoDBDefaultCDCColumnsSchema,
	}
	fixture.RunBackwardCompatibility(t)
}
