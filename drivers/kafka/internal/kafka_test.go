package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestKafkaIntegration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *testutils.IntegrationTest
	}{
		{
			name: "JSON-Format",
			cfg: &testutils.IntegrationTest{
				TestConfig:                       testutils.GetTestConfig(string(constants.Kafka), "json"),
				Namespace:                        "topics",
				ExpectedData:                     ExpectedKafkaJSONData,
				ExpectedUpdatedData:              ExpectedKafkaUpdatedJSONData,
				DestinationDataTypeSchema:        KafkaToDestinationJSONSchema,
				UpdatedDestinationDataTypeSchema: EvolvedKafkaToDestinationJSONSchema,
				DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
				ExecuteQuery:                     ExecuteQueryForJson,
				DestinationDB:                    "kafka_topics",
				PartitionRegex:                   "/{int_value,identity}",
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
			},
		},
		{
			name: "AVRO-Format",
			cfg: &testutils.IntegrationTest{
				TestConfig:                       testutils.GetTestConfig(string(constants.Kafka), "avro"),
				Namespace:                        "topics",
				ExpectedData:                     ExpectedKafkaAvroData,
				ExpectedUpdatedData:              ExpectedKafkaUpdatedAvroData,
				DestinationDataTypeSchema:        KafkaToDestinationAvroSchema,
				UpdatedDestinationDataTypeSchema: EvolvedKafkaToDestinationAvroSchema,
				DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
				ExecuteQuery:                     ExecuteQueryForAvro,
				DestinationDB:                    "kafka_topics",
				PartitionRegex:                   "/{int_value,identity}",
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
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.cfg.TestIntegration(t)
		})
	}
}
