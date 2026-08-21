package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
)

type kafkaFormat struct {
	name string
	cfg  *integration.Test
}

func kafkaFormats() []kafkaFormat {
	return []kafkaFormat{
		{name: "JSON-Format", cfg: kafkaJSONBaseConfig()},
		{name: "AVRO-Format", cfg: kafkaAvroBaseConfig()},
	}
}

func kafkaJSONBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                       &testutils.TestConfig{Driver: string(constants.Kafka), DataFormat: "json"},
		ExpectedData:                     ExpectedKafkaJSONData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedJSONData,
		DestinationDataTypeSchema:        KafkaToDestinationJSONSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationJSONSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
	}
	cfg.TestConfig.Namespace = "topics"
	cfg.TestConfig.ExecuteQuery = ExecuteQueryJSON
	cfg.TestConfig.DestinationDB = "kafka_topics"
	cfg.TestConfig.PartitionRegex = "/{int_value,identity}"
	cfg.TestConfig.ColumnToExclude = "col_excluded"
	cfg.TestConfig.FilterConfig = `{
			"logical_operator": "And",
			"conditions": [
				{
					"column": "string_value",
					"operator": "!=",
					"value": ""
				},
				{
					"column": "float_value",
					"operator": "<",
					"value": 100.00
				}
			]
		}`
	return cfg
}

func kafkaAvroBaseConfig() *integration.Test {
	cfg := &integration.Test{
		TestConfig:                       &testutils.TestConfig{Driver: string(constants.Kafka), DataFormat: "avro"},
		ExpectedData:                     ExpectedKafkaAvroData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedAvroData,
		DestinationDataTypeSchema:        KafkaToDestinationAvroSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationAvroSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
	}
	cfg.TestConfig.Namespace = "topics"
	cfg.TestConfig.ExecuteQuery = ExecuteQueryAvro
	cfg.TestConfig.DestinationDB = "kafka_topics"
	cfg.TestConfig.PartitionRegex = "/{int64_value,identity}"
	cfg.TestConfig.ColumnToExclude = "col_excluded"
	cfg.TestConfig.FilterConfig = `{
			"logical_operator": "And",
			"conditions": [
				{
					"column": "string_value",
					"operator": "!=",
					"value": ""
				},
				{
					"column": "float64_value",
					"operator": "<",
					"value": 100.00
				}
			]
		}`
	return cfg
}

func TestKafkaDiscover(t *testing.T) {
	for _, format := range kafkaFormats() {
		t.Run(format.name, func(t *testing.T) {
			format.cfg.TestDiscover(t)
		})
	}
}

func TestKafkaSync(t *testing.T) {
	t.Parallel()
	for _, format := range kafkaFormats() {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()
			format.cfg.TestSync(t)
		})
	}
}

func TestKafka2PC(t *testing.T) {
	t.Parallel()
	kafkaJSONBaseConfig().Test2PCIntegration(t)
}

func TestKafkaRebalance(t *testing.T) {
	t.Parallel()
	kafkaJSONBaseConfig().TestRebalance(t)
}
