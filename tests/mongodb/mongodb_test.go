package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between the mongodb integration tests.
func mongodbBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig("mongodb"),
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

func TestDiscover(t *testing.T) {
	mongodbBaseConfig().TestDiscover(t)
}

func TestSync(t *testing.T) {
	cfg := mongodbBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
	cfg.TestSync(t)
}

func Test2PC(t *testing.T) {
	mongodbBaseConfig().Test2PCIntegration(t)
}

func TestPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig("mongodb"),
		Namespace:       "twitter_data",
		BackfillStreams: []string{"tweets"},
		CDCStreams:      []string{"tweets_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}
