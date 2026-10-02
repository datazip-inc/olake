package kafka

import (
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
	return &testutils.IntegrationTest{
		TestConfig:                       testutils.GetTestConfig(t, string(constants.Kafka), "json"),
		Namespace:                        "topics",
		ExpectedData:                     ExpectedKafkaJSONData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedJSONData,
		DestinationDataTypeSchema:        KafkaToDestinationJSONSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationJSONSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
		ExecuteQuery:                     ExecuteQueryJSON,
		DestinationDB:                    "kafka_topics",
		PartitionRegex:                   "/{int_value,identity}",
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

func TestKafkaUpsert(t *testing.T) {
	t.Parallel()

	t.Run("kafka_key_json", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"_kafka_key"}
		cfg.IsolateSuite(t, "upsert_kafka_key_json")
		cfg.TestSync(t)
	})

	t.Run("kafka_key_avro", func(t *testing.T) {
		cfg := kafkaAvroBaseConfig(t)
		cfg.DedupKeys = []string{"_kafka_key"}
		cfg.IsolateSuite(t, "upsert_kafka_key_avro")
		cfg.TestSync(t)
	})

	t.Run("record_key_field", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"key"}
		cfg.SkipTombstone = true
		cfg.IsolateSuite(t, "upsert_record_key_field")
		cfg.TestSync(t)
	})

	t.Run("single_body_column", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"customer_id"}
		cfg.UpsertAddOp = "upsert_column_add"
		cfg.UpsertUpdateOp = "upsert_column_update"
		cfg.SkipTombstone = true
		cfg.IsolateSuite(t, "upsert_single_body_column")
		cfg.TestSync(t)
	})

	t.Run("composite_body_columns", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"customer_id", "order_id"}
		cfg.UpsertAddOp = "upsert_column_add"
		cfg.UpsertUpdateOp = "upsert_column_update"
		cfg.SkipTombstone = true
		cfg.IsolateSuite(t, "upsert_composite_body_columns")
		cfg.TestSync(t)
	})

	// Filtered update must not POS-delete the in-filter row.
	t.Run("update_dropped_by_filter", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"_kafka_key"}
		cfg.RunUpsertIceberg(t, "upsert_update_dropped_by_filter", "upsert_add", []testutils.Upsert{
			{
				Name:      "insert",
				Operation: "",
				UseState:  false,
				OpSymbol:  "c",
				Expected:  cfg.ExpectedData,
			},
			{
				Name:      "update-out-of-filter",
				Operation: "upsert_update_out_of_filter",
				UseState:  true,
				OpSymbol:  "c",
				Expected:  cfg.ExpectedData,
			},
		})
	})

	// Same Kafka key, Iceberg partition column int_value changes.
	t.Run("update_changes_partition", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"_kafka_key"}
		expected := map[string]interface{}{}
		for k, v := range cfg.ExpectedUpdatedData {
			expected[k] = v
		}
		expected["int_value"] = int64(200)
		cfg.RunUpsertIceberg(t, "upsert_update_changes_partition", "upsert_add", []testutils.Upsert{
			{
				Name:      "insert",
				Operation: "",
				UseState:  false,
				OpSymbol:  "c",
				Expected:  cfg.ExpectedData,
			},
			{
				Name:      "update-repartition",
				Operation: "upsert_update_repartition",
				UseState:  true,
				OpSymbol:  "u",
				Expected:  expected,
			},
		})
	})

	t.Run("same_key_same_partition", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"_kafka_key"}
		cfg.RunUpsertIceberg(t, "upsert_same_key_same_partition", "upsert_same_key_batch", []testutils.Upsert{
			{
				Name:      "insert",
				Operation: "upsert_update",
				UseState:  false,
				OpSymbol:  "u",
				Expected:  cfg.ExpectedUpdatedData,
			},
		})
	})

	t.Run("same_key_cross_partition", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"_kafka_key"}
		cfg.RunUpsertIceberg(t, "upsert_same_key_cross_partition", "upsert_add", []testutils.Upsert{
			{
				Name:      "insert",
				Operation: "",
				UseState:  false,
				OpSymbol:  "c",
				Expected:  cfg.ExpectedData,
			},
			{
				Name:      "update-other-partition",
				Operation: "upsert_update_partition_1",
				UseState:  true,
				OpSymbol:  "u",
				Expected:  cfg.ExpectedUpdatedData,
			},
		})
	})

	t.Run("dedup_field_absent", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"customer_id"}
		cfg.RunUpsertIceberg(t, "upsert_dedup_field_absent", "upsert_add", []testutils.Upsert{
			{
				Name:      "insert",
				Operation: "",
				UseState:  false,
				OpSymbol:  "c",
				Expected:  cfg.ExpectedData,
			},
		})
	})

	t.Run("dedup_value_empty", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"customer_id"}
		cfg.RunUpsertIceberg(t, "upsert_dedup_value_empty", "upsert_empty", []testutils.Upsert{
			{
				Name:      "insert",
				Operation: "",
				UseState:  false,
				OpSymbol:  "c",
				Expected:  cfg.ExpectedData,
			},
		})
	})

	t.Run("dedup_value_null", func(t *testing.T) {
		cfg := kafkaJSONBaseConfig(t)
		cfg.DedupKeys = []string{"customer_id"}
		cfg.RunUpsertIcebergExpectFail(t, "upsert_dedup_value_null", "upsert_null")
	})
}
