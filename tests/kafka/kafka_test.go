package kafka

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

type kafkaFormat struct {
	name string
	cfg  *testutils.IntegrationTest
}

func kafkaFormats(t *testing.T) []kafkaFormat {
	return []kafkaFormat{
		{name: "JSON-Format", cfg: kafkaJSONBaseConfig(t)},
		{name: "AVRO-Format", cfg: kafkaAvroBaseConfig(t)},
	}
}

func kafkaJSONBaseConfig(t *testing.T) *testutils.IntegrationTest {
	testConf := testutils.GetTestConfig(t, string(constants.Kafka), "json")
	return &testutils.IntegrationTest{
		TestConfig:                       testConf,
		Namespace:                        "topics",
		ExpectedData:                     ExpectedKafkaJSONData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedJSONData,
		DestinationDataTypeSchema:        KafkaToDestinationJSONSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationJSONSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
		ExecuteQuery: func(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
			ExecuteQueryJSON(ctx, t, streams, operation, fileConfig, testConf)
		},
		DestinationDB:   "kafka_topics",
		PartitionRegex:  "/{int_value,identity}",
		ColumnToExclude: "col_excluded",
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

func kafkaAvroBaseConfig(t *testing.T) *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(t, string(constants.Kafka), "avro"),
		Namespace:                        "topics",
		ExpectedData:                     ExpectedKafkaAvroData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedAvroData,
		DestinationDataTypeSchema:        KafkaToDestinationAvroSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationAvroSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
		ExecuteQuery:                     ExecuteQueryAvro,
		DestinationDB:                    "kafka_topics",
		PartitionRegex:                   "/{int64_value,identity}",
		ColumnToExclude:                  "col_excluded",
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

func TestKafkaDiscover(t *testing.T) {
	for _, format := range kafkaFormats(t) {
		t.Run(format.name, func(t *testing.T) {
			format.cfg.TestDiscover(t)
		})
	}
}

func TestKafkaSync(t *testing.T) {
	t.Parallel()
	for _, format := range kafkaFormats(t) {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()
			format.cfg.TestSync(t)
		})
	}
}

func TestKafka2PC(t *testing.T) {
	t.Parallel()
	kafkaJSONBaseConfig(t).Test2PCIntegration(t)
}

func TestKafkaRebalance(t *testing.T) {
	t.Parallel()
	kafkaJSONBaseConfig(t).TestRebalance(t)
}
