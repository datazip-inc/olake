package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
)

// kafkaJsonBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between the kafka JSON-format integration tests.
func kafkaJsonBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig("kafka", "json"),
		Namespace:                 "topics",
		ExpectedData:              ExpectedKafkaJSONData,
		DestinationDataTypeSchema: KafkaToDestinationJSONSchema,
		DefaultCDCColumnsSchema:   ExpectedKafkaDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQueryJSON,
		DestinationDB:             "kafka_topics",
		PartitionRegex:            "/{int_value,identity}",
		ColumnToExclude:           "col_excluded",
		FilterConfig: `{
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
		}`,
	}
}

// kafkaAvroBaseConfig returns an IntegrationTest pre-populated with all fields shared
// between the kafka AVRO-format integration tests.
func kafkaAvroBaseConfig() *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig("kafka", "avro"),
		Namespace:                 "topics",
		ExpectedData:              ExpectedKafkaAvroData,
		DestinationDataTypeSchema: KafkaToDestinationAvroSchema,
		DefaultCDCColumnsSchema:   ExpectedKafkaDefaultCDCColumnsSchema,
		ExecuteQuery:              ExecuteQueryAvro,
		DestinationDB:             "kafka_topics",
		PartitionRegex:            "/{int64_value,identity}",
		ColumnToExclude:           "col_excluded",
		FilterConfig: `{
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
		}`,
	}
}

func TestDiscover(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		kafkaJsonBaseConfig().TestDiscover(t)
	})
	t.Run("avro", func(t *testing.T) {
		kafkaAvroBaseConfig().TestDiscover(t)
	})
}

func TestSync(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		cfg := kafkaJsonBaseConfig()
		cfg.ExpectedUpdatedData = ExpectedKafkaUpdatedJSONData
		cfg.UpdatedDestinationDataTypeSchema = UpdatedKafkaToDestinationJSONSchema
		cfg.TestSync(t)
	})
	t.Run("avro", func(t *testing.T) {
		cfg := kafkaAvroBaseConfig()
		cfg.ExpectedUpdatedData = ExpectedKafkaUpdatedAvroData
		cfg.UpdatedDestinationDataTypeSchema = UpdatedKafkaToDestinationAvroSchema
		cfg.TestSync(t)
	})
}

func Test2PC(t *testing.T) {
	kafkaJsonBaseConfig().Test2PCIntegration(t)
}

func TestRebalance(t *testing.T) {
	kafkaJsonBaseConfig().TestRebalance(t)
}
