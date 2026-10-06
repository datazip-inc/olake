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

	testCases := []struct {
		name          string
		isolateSuite  string
		json          bool
		dedup         []string
		add           string
		update        string
		skipTombstone bool
	}{
		{"kafka_key_json", "upsert_kafka_key_json", true, []string{"_kafka_key"}, "", "", false},
		{"kafka_key_avro", "upsert_kafka_key_avro", false, []string{"_kafka_key"}, "", "", false},
		{"record_key_field", "upsert_record_key_field", true, []string{"key"}, "", "", true},
		{"single_body_column", "upsert_single_body_column", true, []string{"customer_id"}, "upsert_column_add", "upsert_column_update", true},
		{"composite_body_columns", "upsert_composite_body_columns", true, []string{"customer_id", "order_id"}, "upsert_column_add", "upsert_column_update", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := kafkaJSONBaseConfig(t)
			if !tc.json {
				cfg = kafkaAvroBaseConfig(t)
			}
			cfg.DedupKeys = tc.dedup
			cfg.UpsertAddOp = tc.add
			cfg.UpsertUpdateOp = tc.update
			cfg.SkipTombstone = tc.skipTombstone
			cfg.IsolateSuite(t, tc.isolateSuite)
			cfg.TestSync(t)
		})
	}

	icebergTestCases := []struct {
		name         string
		isolateSuite string
		seed         string
		dedup        []string
		expectFail   bool
		syncs        []testutils.Upsert
	}{
		{
			name:         "update_dropped_by_filter",
			isolateSuite: "upsert_update_dropped_by_filter",
			seed:         "upsert_add",
			dedup:        []string{"_kafka_key"},
			syncs: []testutils.Upsert{
				{
					Name:     "insert",
					UseState: false,
					OpSymbol: "c",
					Expected: ExpectedKafkaJSONData,
				},
				{
					Name:      "update-out-of-filter",
					Operation: "upsert_update_out_of_filter",
					UseState:  true,
					OpSymbol:  "c",
					Expected:  ExpectedKafkaJSONData,
				},
			},
		},
		{
			name:         "update_changes_partition",
			isolateSuite: "upsert_update_changes_partition",
			seed:         "upsert_add",
			dedup:        []string{"_kafka_key"},
			syncs: []testutils.Upsert{
				{
					Name:     "insert",
					UseState: false,
					OpSymbol: "c",
					Expected: ExpectedKafkaJSONData,
				},
				{
					Name:      "update-repartition",
					Operation: "upsert_update_repartition",
					UseState:  true,
					OpSymbol:  "u",
					Expected:  ExpectedKafkaRepartitionJSONData,
				},
			},
		},
		{
			name:         "same_key_same_partition",
			isolateSuite: "upsert_same_key_same_partition",
			seed:         "upsert_same_key_batch",
			dedup:        []string{"_kafka_key"},
			syncs: []testutils.Upsert{
				{
					Name:     "insert",
					UseState: false,
					OpSymbol: "u",
					Expected: ExpectedKafkaUpdatedJSONData,
				},
			},
		},
		{
			name:         "same_key_cross_partition",
			isolateSuite: "upsert_same_key_cross_partition",
			seed:         "upsert_add",
			dedup:        []string{"_kafka_key"},
			syncs: []testutils.Upsert{
				{
					Name:     "insert",
					UseState: false,
					OpSymbol: "c",
					Expected: ExpectedKafkaJSONData,
				},
				{
					Name:      "update-other-partition",
					Operation: "upsert_update_partition_1",
					UseState:  true,
					OpSymbol:  "u",
					Expected:  ExpectedKafkaUpdatedJSONData,
				},
			},
		},
		{
			name:         "dedup_field_absent",
			isolateSuite: "upsert_dedup_field_absent",
			seed:         "upsert_add",
			dedup:        []string{"customer_id"},
			syncs: []testutils.Upsert{
				{
					Name:     "insert",
					UseState: false,
					OpSymbol: "c",
					Expected: ExpectedKafkaJSONData,
				},
				{
					Name:      "insert-again",
					Operation: "upsert_add",
					UseState:  true,
					OpSymbol:  "c",
					Expected:  ExpectedKafkaJSONData,
				},
			},
		},
		{
			name:         "dedup_value_empty",
			isolateSuite: "upsert_dedup_value_empty",
			seed:         "upsert_empty",
			dedup:        []string{"customer_id"},
			syncs: []testutils.Upsert{
				{
					Name:     "insert",
					UseState: false,
					OpSymbol: "c",
					Expected: ExpectedKafkaJSONData,
				},
			},
		},
		{
			name:         "dedup_value_null",
			isolateSuite: "upsert_dedup_value_null",
			seed:         "upsert_null",
			dedup:        []string{"customer_id"},
			expectFail:   true,
		},
	}
	for _, tc := range icebergTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := kafkaJSONBaseConfig(t)
			cfg.DedupKeys = tc.dedup
			if tc.expectFail {
				cfg.RunUpsertIcebergExpectFail(t, tc.isolateSuite, tc.seed)
				return
			}
			cfg.RunUpsertIceberg(t, tc.isolateSuite, tc.seed, tc.syncs)
		})
	}
}
