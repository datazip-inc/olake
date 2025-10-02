package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMongodbIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:         testutils.GetTestConfig(string(constants.MongoDB)),
		Namespace:          "olake_mongodb_test",
		ExpectedData:       ExpectedMongoData,
		ExpectedUpdateData: ExpectedUpdatedMongoData,
		DataTypeSchema:     MongoToIcebergSchema,
		ExecuteQuery:       ExecuteQuery,
		IcebergDB:          "mongodb_olake_mongodb_test",
		CursorField:        "_id",
		PartitionRegex:     "/{_id,identity}",
	}
	testConfig.TestIntegration(t)
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
