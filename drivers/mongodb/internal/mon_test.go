package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between TestMongodbIntegration and TestMongodb2PC.
func mongodbBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(string(constants.MongoDB)),
		Namespace:                 "olake_mongodb_test",
		ExpectedData:              ExpectedMongoData,
		DestinationDataTypeSchema: MongoToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMongoDbDefaultCDCColumnsSchema,
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
	cfg := mongodbBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
	cfg.TestIntegration(t)
}

func TestMongodb2PC(t *testing.T) {
	t.Parallel()
	mongodbBaseConfig().Test2PCIntegration(t)
}

func TestMongodbPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(string(constants.MongoDB)),
		Namespace:       "twitter_data",
		BackfillStreams: []string{"tweets"},
		CDCStreams:      []string{"tweets_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
