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
		ExpectedUpdatedData:              ExpectedKafkaUpdatedData,
		DestinationDataTypeSchema:        KafkaToDestinationSchema,
		UpdatedDestinationDataTypeSchema: EvolvedKafkaToDestinationSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
		ExecuteQuery:                     ExecuteQuery,
		DestinationDB:                    "kafka_topics",
		CursorField:                      "int_value:bigint",
		PartitionRegex:                   "/{int_value,identity}",
		ExtraExpectedData: map[string]map[string]interface{}{
			"Avro-insert":        ExpectedKafkaAvroData,
			"Avro-evolve-schema": ExpectedKafkaAvroUpdatedData,
		},
		FilterConfig: `{
			"logical_operator": "And",
			"conditions": [
				{
					"column": "int_value",
					"operator": ">=",
					"value": 100
				},
				{
					"column": "float_value",
					"operator": "<",
					"value": 100.00
				}
			]
		}`,
	}
	testConfig.TestIntegration(t)
}
