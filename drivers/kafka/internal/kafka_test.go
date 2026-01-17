package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestKafkaIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(string(constants.Kafka)),
		Namespace:                        "topics",
		ExpectedData:                     ExpectedKafkaData,
		ExpectedUpdatedData:              ExpectedUpdatedData,
		DestinationDataTypeSchema:        KafkaToDestinationSchema,
		UpdatedDestinationDataTypeSchema: KafkaToDestinationSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "kafka_olake_kafka",
		CursorField:                      "id_cursor:id_smallint",
		PartitionRegex:                   "/{id,identity}",
	}
	testConfig.TestIntegration(t)
}
