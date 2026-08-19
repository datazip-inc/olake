package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
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
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(t, string(constants.MongoDB)),
		Namespace:       "twitter_data",
		BackfillStreams: testutils.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
