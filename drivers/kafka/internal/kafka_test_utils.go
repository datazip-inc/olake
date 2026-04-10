package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/utils"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

const (
	partitionCount        = 5
	avroSchemaRegistryURL = "http://127.0.0.1:8081"

	// Base Avro schema
	avroSchema = `{
		"type":"record",
		"name":"test",
		"fields":[
			{"name":"int32_value","type":"int"},
			{"name":"int64_value","type":"long"},
			{"name":"float32_value","type":"float"},
			{"name":"float64_value","type":"double"},
			{"name":"boolean","type":"boolean"},
			{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"}},
			{"name":"string_value","type":"string"},
			{"name":"int_value","type":"int"},
			{"name":"float_value","type":"float"},
			{"name":"col_excluded","type":"int"}
		]
	}`

	// Updated Avro schema
	updatedAvroSchema = `{
		"type":"record",
		"name":"test",
		"fields":[
			{"name":"int32_value","type":"int"},
			{"name":"int64_value","type":"long"},
			{"name":"float32_value","type":"float"},
			{"name":"float64_value","type":"double"},
			{"name":"boolean","type":"boolean"},
			{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"}},
			{"name":"string_value","type":"string"},
			{"name":"int_value","type":"long"},
			{"name":"float_value","type":"double"},
			{"name":"col_excluded","type":"int"},
			{"name":"col_included","type":"int","default": 102}
		]
	}`
)

var (
	// JSON
	jsonKey          = []byte("json-key")
	jsonValue        = []byte(`{"int_value": 100,"float_value": 99.99,"boolean": true,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string", "col_excluded": 101}`)
	jsonUpdatedValue = []byte(`{"int_value": 100,"float_value": 99.99,"boolean": true,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string", "col_excluded": 101, "col_included": 102}`)
	jsonFilterValue  = []byte(`{"string_value": "","float_value": 99.99,"col_excluded": 101}`)

	// Avro
	avroKey   = []byte("avro-key")
	avroValue = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"boolean":         true,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "test_string",
		"int_value":       int32(100),
		"float_value":     float32(64.6464),
		"col_excluded":    int32(101),
	}

	avroFilterValue = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"boolean":         true,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "",
		"int_value":       int32(100),
		"float_value":     float32(64.6464),
		"col_excluded":    int32(101),
	}

	avroUpdatedValue = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"boolean":         true,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "test_string",
		"int_value":       int64(100),
		"float_value":     float64(64.6464),
		"col_excluded":    int32(101),
		"col_included":    int32(102),
	}
)

// ExecuteQueryJSON executes Kafka queries for testing based on the operation type
func ExecuteQueryJSON(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var kafkaJSONBroker string
	if fileConfig {
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		kafkaJSONBroker = config.BootstrapServers
	} else {
		kafkaJSONBroker = "127.0.0.1:29092"
	}

	// kafka writer
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaJSONBroker),
		Topic:                  streams[0],
		Balancer:               &kafka.RoundRobin{},
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()

	switch operation {
	case "create":
		createKafkaTopic(ctx, t, kafkaJSONBroker, streams[0])

	case "clean":
		deleteKafkaTopic(ctx, t, kafkaJSONBroker, streams[0])
		createKafkaTopic(ctx, t, kafkaJSONBroker, streams[0])

	case "drop":
		deleteKafkaTopic(ctx, t, kafkaJSONBroker, streams[0])

	case "add":
		// 5 messages inserted with different partitions
		for partition := range partitionCount {
			writeMessagesWithRetry(ctx, t, writer, kafka.Message{Key: jsonKey, Value: jsonValue, Partition: partition})
		}
		writeMessagesWithRetry(ctx, t, writer, kafka.Message{Key: jsonKey, Value: jsonFilterValue})
		t.Logf("Added 6 messages to topic '%s' (one per partition and one for filters)", streams[0])

	case "update":
		writeMessagesWithRetry(ctx, t, writer, kafka.Message{Key: jsonKey, Value: jsonUpdatedValue})
		t.Logf("Added 1 updated message to topic '%s'", streams[0])

	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

// ExecuteQueryAvro executes Kafka queries for testing based on the operation type
func ExecuteQueryAvro(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var kafkaAvroBroker string
	if fileConfig {
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)
		kafkaAvroBroker = config.BootstrapServers
	} else {
		kafkaAvroBroker = "127.0.0.1:29192"
	}
	// kafka writer
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAvroBroker),
		Topic:                  streams[0],
		Balancer:               &kafka.RoundRobin{},
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()
	switch operation {
	case "create":
		createKafkaTopic(ctx, t, kafkaAvroBroker, streams[0])

	case "clean":
		deleteKafkaTopic(ctx, t, kafkaAvroBroker, streams[0])
		createKafkaTopic(ctx, t, kafkaAvroBroker, streams[0])

	case "drop":
		deleteKafkaTopic(ctx, t, kafkaAvroBroker, streams[0])

	case "add":
		// avro codec
		codec, err := goavro.NewCodec(avroSchema)
		require.NoError(t, err)
		schemaID := registerSchemaWithRetry(t, avroSchemaRegistryURL, streams[0], avroSchema)

		// avro messages written
		encodeAndWriteAvro(ctx, t, writer, codec, schemaID, avroKey, avroValue)
		encodeAndWriteAvro(ctx, t, writer, codec, schemaID, avroKey, avroFilterValue)
		t.Logf("Added 2 messages to topic '%s' (one valid for sync and one filtered out)", streams[0])

	case "update":
		codec, err := goavro.NewCodec(updatedAvroSchema)
		require.NoError(t, err)
		schemaID := registerSchemaWithRetry(t, avroSchemaRegistryURL, streams[0], updatedAvroSchema)

		// avro message written with new schema
		encodeAndWriteAvro(ctx, t, writer, codec, schemaID, avroKey, avroUpdatedValue)
		t.Logf("Added 1 updated message to topic '%s'", streams[0])

	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

// deleteTopic deletes the topic and waits briefly so the broker can settle (matches prior test harness behavior).
func deleteKafkaTopic(ctx context.Context, t *testing.T, broker, topic string) {
	t.Helper()

	// conect to kafka cluster
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	require.NoError(t, err, "failed to dial kafka broker")
	defer conn.Close()

	// delete topic
	err = conn.DeleteTopics(topic)
	require.NoError(t, err, "failed to delete topic '%s'", topic)
	time.Sleep(5 * time.Second)
}

// createTopic creates the test topic with a fixed partition count and replication factor 1.
func createKafkaTopic(ctx context.Context, t *testing.T, broker, topic string) {
	t.Helper()

	// conect to kafka cluster
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	require.NoError(t, err, "failed to dial kafka broker")
	defer conn.Close()

	// create topic
	err = conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: partitionCount, ReplicationFactor: 1})
	if err != nil && err != kafka.TopicAlreadyExists {
		require.NoError(t, err, "failed to create topic '%s' explicitly", topic)
	}
}

// Writes a Kafka message with retries until success or context timeout.
func writeMessagesWithRetry(ctx context.Context, t *testing.T, writer *kafka.Writer, msg kafka.Message) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		// write message
		err := writer.WriteMessages(ctx, msg)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			require.NoError(t, err, "timed out writing kafka message after retries (topic=%q partition=%d): %v", writer.Topic, msg.Partition, err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Registers a schema with retries and returns its schema ID.
func registerSchemaWithRetry(t *testing.T, url, topic, schema string) uint32 {
	t.Helper()

	body, err := json.Marshal(map[string]string{"schema": schema})
	require.NoError(t, err)

	client := &http.Client{Timeout: 10 * time.Second}
	var schemaID uint32

	// retry for schema registration
	err = utils.RetryOnBackoff(context.Background(), 5, 2*time.Second, func(_ context.Context) error {
		// get schema response
		response, err := client.Post(
			fmt.Sprintf("%s/subjects/%s-value/versions", url, topic),
			"application/vnd.schemaregistry.v1+json",
			bytes.NewReader(body),
		)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status: %d", response.StatusCode)
		}

		var schema struct {
			ID uint32 `json:"id"`
		}
		if err := json.NewDecoder(response.Body).Decode(&schema); err != nil {
			return err
		}

		schemaID = schema.ID
		return nil
	})

	require.NoError(t, err, "failed to register schema")
	return schemaID
}

// encodeAndWriteAvro encodes the Avro value and writes it to the Kafka topic
func encodeAndWriteAvro(ctx context.Context, t *testing.T, writer *kafka.Writer, codec *goavro.Codec, schemaID uint32, key []byte, value map[string]interface{}) {
	t.Helper()
	binaryData, err := codec.BinaryFromNative(nil, value)
	require.NoError(t, err, "encode Avro value to binary (topic=%q, schema_id=%d)", writer.Topic, schemaID)

	// Confluent wire format: 1-byte magic (0x00) + 4-byte big-endian schema ID + Avro binary payload.
	msg := make([]byte, 5+len(binaryData))
	msg[0] = 0x00
	binary.BigEndian.PutUint32(msg[1:5], schemaID)
	copy(msg[5:], binaryData)

	// write message
	writeMessagesWithRetry(ctx, t, writer, kafka.Message{Key: key, Value: msg})
}

// JSON data format resources
var ExpectedKafkaJSONData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
}

var KafkaToDestinationJSONSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
}

var ExpectedKafkaUpdatedJSONData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"col_included":    int64(102),
}

var UpdatedKafkaToDestinationJSONSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"col_included":    "bigint",
}

// AVRO data format resources
var ExpectedKafkaAvroData = map[string]interface{}{
	"int32_value":     int32(132),
	"int64_value":     int64(6400000000),
	"float32_value":   float32(32.5),
	"float64_value":   float64(64.6464),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"int_value":       int32(100),
	"float_value":     float32(64.6464),
}

var ExpectedKafkaUpdatedAvroData = map[string]interface{}{
	"int32_value":     int32(132),
	"int64_value":     int64(6400000000),
	"float32_value":   float32(32.5),
	"float64_value":   float64(64.6464),
	"boolean":         true,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"int_value":       int64(100),       // promoted from int → long
	"float_value":     float64(64.6464), // promoted from float → double
	"col_included":    int32(102),       // new field
}

var KafkaToDestinationAvroSchema = map[string]string{
	"int32_value":     "int",
	"int64_value":     "bigint",
	"float32_value":   "float",
	"float64_value":   "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"int_value":       "int",
	"float_value":     "float",
	"string_value":    "string",
}

var UpdatedKafkaToDestinationAvroSchema = map[string]string{
	"int32_value":     "int",
	"int64_value":     "bigint",
	"float32_value":   "float",
	"float64_value":   "double",
	"boolean":         "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"int_value":       "bigint",
	"float_value":     "double",
	"col_included":    "int",
}

var ExpectedKafkaDefaultCDCColumnsSchema = map[string]string{
	"_kafka_key":       "string",
	"_kafka_offset":    "bigint",
	"_kafka_partition": "int",
	"_kafka_timestamp": "timestamp",
	"_op_type":         "string",
	"_cdc_timestamp":   "timestamp",
	"_olake_id":        "string",
	"_olake_timestamp": "timestamp",
}
