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
	build func(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler
	// destinationSchema is the format's destination column types, which the compatibility suite
	// resolves its type-keyed rules against.
	destinationSchema map[string]string
}

var kafkaFormats = []kafkaFormat{
	{name: "JSON-Format", build: kafkaJSONBaseConfig, destinationSchema: KafkaToDestinationJSONSchema},
	{name: "AVRO-Format", build: kafkaAvroBaseConfig, destinationSchema: KafkaToDestinationAvroSchema},
}

// kafkaJSONTestConfig builds the config every kafka JSON suite shares: the source, the namespace and the stream settings.
func kafkaJSONTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.Kafka, "topics", ExecuteQueryJSON,
		append([]testutils.TestConfigOption{testutils.WithDataFormat("json")}, opts...)...)
	require.NoError(t, err, "failed to build the test config")
	cfg.PartitionRegex = "/{int_value,identity}"
	cfg.ColumnToExclude = "col_excluded"
	cfg.SkipSchemaEvolution = true
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

	return cfg
}

// kafkaJSONBaseConfig is kafkaJSONTestConfig with the integration suites' expected data.
func kafkaJSONBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                       kafkaJSONTestConfig(t, opts...),
		ExpectedData:                     ExpectedKafkaJSONData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedJSONData,
		DestinationDataTypeSchema:        KafkaToDestinationJSONSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationJSONSchema,
		TypeMapping:                      KafkaTypeMapping,
		DefaultCDCColumnsSchema:          ExpectedKafkaDefaultCDCColumnsSchema,
	}
}

// kafkaAvroTestConfig builds the config every kafka Avro suite shares: the source, the namespace and the stream settings.
func kafkaAvroTestConfig(t *testing.T, opts ...testutils.TestConfigOption) *testutils.TestConfig {
	cfg, err := testutils.NewTestConfig(t, constants.Kafka, "topics", ExecuteQueryAvro,
		append([]testutils.TestConfigOption{testutils.WithDataFormat("avro")}, opts...)...)
	require.NoError(t, err, "failed to build the test config")
	cfg.PartitionRegex = "/{int64_value,identity}"
	cfg.ColumnToExclude = "col_excluded"
	cfg.SkipSchemaEvolution = true
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

	return cfg
}

// kafkaAvroBaseConfig is kafkaAvroTestConfig with the integration suites' expected data.
func kafkaAvroBaseConfig(t *testing.T, opts ...testutils.TestConfigOption) *integration.TestHandler {
	return &integration.TestHandler{
		TestConfig:                       kafkaAvroTestConfig(t, opts...),
		ExpectedData:                     ExpectedKafkaAvroData,
		ExpectedUpdatedData:              ExpectedKafkaUpdatedAvroData,
		DestinationDataTypeSchema:        KafkaToDestinationAvroSchema,
		UpdatedDestinationDataTypeSchema: UpdatedKafkaToDestinationAvroSchema,
		TypeMapping:                      KafkaTypeMapping,
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
	testRebalance(t, kafkaJSONBaseConfig(t))
}

// TestKafkaCompatibility runs every source format. Each format owns its broker, its testdata
// directory and its topic name, so the two sweeps share one destination namespace without colliding.
func TestKafkaCompatibility(t *testing.T) {
	t.Parallel()
	for _, format := range kafkaFormats {
		t.Run(format.name, func(t *testing.T) {
			t.Parallel()
			testHandler := &compatibility.TestHandler{
				NewConfig: func(t *testing.T, version string) *testutils.TestConfig {
					return format.build(t, testutils.WithDriverVersion(version)).TestConfig
				},
				DestinationSchema: format.destinationSchema,
				CDCColumnsSchema:  ExpectedKafkaDefaultCDCColumnsSchema,
			}
			testHandler.RunBackwardCompatibility(t)
		})
	}
}
