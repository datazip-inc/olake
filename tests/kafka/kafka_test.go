package kafka

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/compatibility"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
	"github.com/datazip-inc/olake/tests/testutils/require"
)

type kafkaFormat struct {
	name string
	// build runs inside the subtest, not beside it: every name a suite owns is derived from
	// t.Name(), so both formats built against the parent would answer to the same one.
	build func(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test
}

var kafkaFormats = []kafkaFormat{
	{name: "JSON-Format", build: kafkaJSONBaseConfig},
	{name: "AVRO-Format", build: kafkaAvroBaseConfig},
}

func kafkaJSONBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.Kafka, "topics", "kafka_topics", ExecuteQueryJSON,
		append([]testutils.TestConfigOption{testutils.WithDataFormat("json")}, opts...)...)
	require.NoError(t, err, "failed to build the test config")
	cfg.PartitionRegex = "/{int_value,identity}"
	cfg.ColumnToExclude = "col_excluded"
	cfg.FilterConfig = `{
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

	return &integration.Test{
		TestConfig:                       cfg,
		ExpectedData:                     ExpectedKafkaJSONData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedJSONData,
		DestinationDataTypeSchema:        KafkaToDestinationJSONSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationJSONSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
	}
}

func kafkaAvroBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.Test {
	cfg, err := testutils.NewTestConfig(t, constants.Kafka, "topics", "kafka_topics", ExecuteQueryAvro,
		append([]testutils.TestConfigOption{testutils.WithDataFormat("avro")}, opts...)...)
	require.NoError(t, err, "failed to build the test config")
	cfg.PartitionRegex = "/{int64_value,identity}"
	cfg.ColumnToExclude = "col_excluded"
	cfg.FilterConfig = `{
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

	return &integration.Test{
		TestConfig:                       cfg,
		ExpectedData:                     ExpectedKafkaAvroData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedAvroData,
		DestinationDataTypeSchema:        KafkaToDestinationAvroSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationAvroSchema,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
	}
}

func TestKafkaDiscover(t *testing.T) {
	for _, format := range kafkaFormats {
		t.Run(format.name, func(t *testing.T) {
			format.build(t).TestDiscover(t)
		})
	}
}

func TestKafkaSync(t *testing.T) {
	t.Parallel()
	for _, format := range kafkaFormats {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()
			format.build(t).TestSync(t)
		})
	}
}

func TestKafka2PC(t *testing.T) {
	t.Parallel()
	kafkaJSONBaseConfig(t).Test2PCIntegration(t)
}

func TestKafkaRebalance(t *testing.T) {
	t.Parallel()
	runRebalanceSuite(t, kafkaJSONBaseConfig(t))
}

// TestKafkaCompatibility pins the backward-compatibility contract on the JSON format, the same single
// format Test2PCIntegration uses: the suite varies only the binary, and avro would add a
// schema-registry axis to the comparison. See tests/testutils/compatibility.go.
func TestKafkaCompatibility(t *testing.T) {
	t.Parallel()
	fixture := &compatibility.Test{
		NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
			return kafkaJSONBaseConfig(t, testutils.WithDriverVersion(version)).TestConfig
		},
		DeclaredSchema:   KafkaToDestinationJSONSchema,
		CDCColumnsSchema: ExpectedKafkaDefaultCDCColumnsSchema,
	}
	fixture.RunBackwardCompatibility(t)
}
