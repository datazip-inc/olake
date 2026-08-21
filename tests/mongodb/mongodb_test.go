package mongodb

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/performance"
)

// mongodbBaseConfig returns an IntegrationTest pre-populated with all fields shared
func mongodbBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                &testutils.TestConfig{Driver: string(constants.MongoDB)},
		ExpectedData:              ExpectedMongoData,
		DestinationDataTypeSchema: MongoToDestinationSchema,
		DefaultCDCColumnsSchema:   ExpectedMongoDBDefaultCDCColumnsSchema,
	}
	cfg.TestConfig.Namespace = "olake_mongodb_test"
	cfg.TestConfig.ExecuteQuery = ExecuteQuery
	cfg.TestConfig.DestinationDB = "mongodb_olake_mongodb_test"
	cfg.TestConfig.CursorField = "id_cursor:id_int"
	cfg.TestConfig.PartitionRegex = "/{_id,identity}"
	cfg.TestConfig.ColumnToExclude = "excludedColumn"
	cfg.TestConfig.FilterConfig = `{
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
	return cfg
}

func TestMongodbDiscover(t *testing.T) {
	mongodbBaseConfig().TestDiscover(t)
}

func TestMongodbSync(t *testing.T) {
	t.Parallel()
	cfg := mongodbBaseConfig()
	cfg.ExpectedUpdatedData = ExpectedUpdatedData
	cfg.UpdatedDestinationDataTypeSchema = UpdatedMongoToDestinationSchema
	cfg.TestSync(t)
}

func TestMongodb2PC(t *testing.T) {
	t.Parallel()
	mongodbBaseConfig().Test2PCIntegration(t)
}

func TestMongodbPerformance(t *testing.T) {
	config := &performance.Test{
		TestConfig:      &testutils.TestConfig{Driver: string(constants.MongoDB)},
		BackfillStreams: performance.GetBackfillStreamsFromCDC(performanceCDCStreams),
		CDCStreams:      performanceCDCStreams,
	}
	config.TestConfig.Namespace = "twitter_data"
	config.TestConfig.ExecuteQuery = ExecuteQuery

	config.TestPerformance(t)
}
