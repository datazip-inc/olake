package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/utils"
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

var (
	// Message key and value for JSON
	Jsonkey            = []byte("json-key")
	Avrokey            = []byte("avro-key")
	value          = []byte(`{"int_value": 100,"float_value": 99.99,"boolean_true": true,"boolean_false": false,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string"}`)
	evolved_value  = []byte(`{"int_value": 100,"float_value": 99.99,"boolean_true": true,"boolean_false": false,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string", "id_int": 101}`)
	filtervalue1   = []byte(`{"int_value": 99,"float_value": 99.99}`)
	filtervalue2   = []byte(`{"int_value": 100,"float_value": 100.00}`)
	partitionCount = 5

	// Base Avro schema
	Avroschema = `{
		"type":"record",
		"name":"test",
		"fields":[
			{"name":"int32_value","type":"int","default":0},
			{"name":"int64_value","type":"long","default":0},
			{"name":"int_value","type":"long","default":0},
			{"name":"float32_value","type":"float","default":0},
			{"name":"float64_value","type":"double","default":0},
			{"name":"float_value","type":"double","default":0},
			{"name":"boolean_true","type":"boolean","default":false},
			{"name":"boolean_false","type":"boolean","default":false},
			{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"},"default":0},
			{"name":"string_value","type":"string","default":""}
		]
	}`

	// Evolved Avro schema
	UpdatedAvroschema = `{
		"type":"record",
		"name":"test",
		"fields":[
			{"name":"int32_value","type":"long","default":0},
			{"name":"int64_value","type":"long","default":0},
			{"name":"int_value","type":"long","default":0},
			{"name":"float32_value","type":"float","default":0},
			{"name":"float64_value","type":"double","default":0},
			{"name":"float_value","type":"double","default":0},
			{"name":"boolean_true","type":"boolean","default":false},
			{"name":"boolean_false","type":"boolean","default":false},
			{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"},"default":0},
			{"name":"string_value","type":"string","default":""},
			{"name":"id_int","type":"long","default":0}
		]
	}`

	dataToProduce = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"int_value":       int64(101),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"float_value":     float64(66.6666),
		"boolean_true":    true,
		"boolean_false":   false,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "test_string",
	}
	dataToProduceFilter = map[string]interface{}{
		"int_value":   99,
		"float_value": 99.99,
	}
	evolvedDataToProduce = map[string]interface{}{
		"int32_value":     int32(132),
		"int64_value":     int64(6400000000),
		"int_value":       int64(101),
		"float32_value":   float32(32.5),
		"float64_value":   float64(64.6464),
		"float_value":     float64(66.6666),
		"boolean_true":    true,
		"boolean_false":   false,
		"timestamp_value": int64(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
		"string_value":    "test_string",
		"id_int":          int64(101),
	}
)

// ExecuteQueryForJson executes Kafka queries for testing based on the operation type
func ExecuteQueryForJson(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var broker string
	if fileConfig {
		var config Config
		require.NoError(t, utils.UnmarshalFile("./testdata/Json/source.json", &config, false), "failed to unmarshal kafka test source config")
		broker = config.BootstrapServers
	} else {
		broker = "127.0.0.1:29092"
	}

	broker = strings.ReplaceAll(broker, "host.docker.internal", "127.0.0.1")
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  streams[0],
		Balancer:               &kafka.RoundRobin{},
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()

	switch operation {
	case "create":
		createKafkaTopic(ctx, t, broker, streams[0])
	case "clean":
		deleteKafkaTopic(ctx, t, broker, streams[0])
		createKafkaTopic(ctx, t, broker, streams[0])
	case "drop":
		deleteKafkaTopic(ctx, t, broker, streams[0])
	case "add":
		for partition := 0; partition < partitionCount; partition++ {
			writeMessagesWithRetry(ctx, t, writer, kafka.Message{
				Key:   Jsonkey,
				Value: value,
			})
		}
		writeMessagesWithRetry(ctx, t, writer, kafka.Message{
			Key:   Jsonkey,
			Value: filtervalue1,
		})
		writeMessagesWithRetry(ctx, t, writer, kafka.Message{
			Key:   Jsonkey,
			Value: filtervalue2,
		})
		t.Logf("Added 7 messages to topic '%s' (one per partition and two for filters)", streams[0])
	case "evolve-schema":
		for partition := 0; partition < partitionCount; partition++ {
			writeMessagesWithRetry(ctx, t, writer, kafka.Message{
				Key:       Jsonkey,
				Value:     evolved_value,
				Partition: partition,
			})
		}
		t.Logf("Added 5 messages to topic '%s' (each per partition)", streams[0])
	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

// ExecuteQueryForAvro executes Kafka queries for testing based on the operation type
func ExecuteQueryForAvro(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var broker string
	var config Config
	require.NoError(t, utils.UnmarshalFile("./testdata/Avro/source.json", &config, false), "failed to unmarshal kafka test source config")
	if fileConfig {
		broker = config.BootstrapServers
	} else {
		broker = "127.0.0.1:29092"
	}

	registryURL := config.SchemaRegistry.Endpoint
	registryURL = strings.ReplaceAll(registryURL, "host.docker.internal", "127.0.0.1")

	broker = strings.ReplaceAll(broker, "host.docker.internal", "127.0.0.1")

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  streams[0],
		Balancer:               &kafka.RoundRobin{},
		AllowAutoTopicCreation: false,
	}
	defer writer.Close()
	switch operation {
	case "create":
		createKafkaTopic(ctx, t, broker, streams[0])
	case "clean":
		deleteKafkaTopic(ctx, t, broker, streams[0])
		createKafkaTopic(ctx, t, broker, streams[0])
	case "drop":
		deleteKafkaTopic(ctx, t, broker, streams[0])
	case "add":
		schema := Avroschema
		codec, err := goavro.NewCodec(schema)
		require.NoError(t, err)

		schemaID := registerSchemaWithRetry(t, registryURL, streams[0], schema)

		binaryData, err := codec.BinaryFromNative(nil, dataToProduce)
		require.NoError(t, err)
		binaryDataFilter, err := codec.BinaryFromNative(nil, dataToProduceFilter)
		require.NoError(t, err)

		confluentBaseMsg := encodeConfluentBinary(schemaID, binaryData)
		confluentFilterMsg := encodeConfluentBinary(schemaID, binaryDataFilter)
		err = writer.WriteMessages(ctx,
			kafka.Message{Key: Avrokey, Value: confluentBaseMsg},
			kafka.Message{Key: Avrokey, Value: confluentFilterMsg},
		)
		require.NoError(t, err)
	case "evolve-schema":
		schema := UpdatedAvroschema
		codec, err := goavro.NewCodec(schema)
		require.NoError(t, err)
		schemaID := registerSchemaWithRetry(t, registryURL, streams[0], schema)
		binaryData, err := codec.BinaryFromNative(nil, evolvedDataToProduce)
		require.NoError(t, err)
		confluentMsg := encodeConfluentBinary(schemaID, binaryData)
		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   Avrokey,
			Value: confluentMsg,
		})
		require.NoError(t, err)
	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

// deleteTopic deletes the topic and waits briefly so the broker can settle (matches prior test harness behavior).
func deleteKafkaTopic(ctx context.Context, t *testing.T, broker, topic string) {
	t.Helper()
	conn := dialKafkaAdminConn(ctx, t, broker)
	defer conn.Close()
	err := conn.DeleteTopics(topic)
	require.NoError(t, err, "failed to delete topic '%s'", topic)
	time.Sleep(5 * time.Second)
}

// createTopic creates the test topic with a fixed partition count and replication factor 1.
func createKafkaTopic(ctx context.Context, t *testing.T, broker, topic string) {
	t.Helper()
	conn := dialKafkaAdminConn(ctx, t, broker)
	defer conn.Close()

	err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitionCount,
		ReplicationFactor: 1,
	})
	if err != nil && err != kafka.TopicAlreadyExists {
		require.NoError(t, err, "failed to create topic '%s' explicitly", topic)
	}
}

func dialKafkaAdminConn(ctx context.Context, t *testing.T, broker string) *kafka.Conn {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	require.NoError(t, err, "failed to dial kafka broker")
	_, err = conn.ReadPartitions()
	require.NoError(t, err, "failed to read kafka partitions metadata")
	return conn
}

func writeMessagesWithRetry(ctx context.Context, t *testing.T, writer *kafka.Writer, msg kafka.Message) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	var lastErr error
	var attempts int
	nextLog := time.Now()
	for {
		lastErr = writer.WriteMessages(ctx, msg)
		if lastErr == nil {
			return
		}
		attempts++
		if err := ctx.Err(); err != nil {
			require.NoError(t, lastErr, "failed to write seed kafka message after %d attempts (topic=%q partition=%d)", attempts, writer.Topic, msg.Partition)
			return
		}
		// Without this, a bad broker/topic state spins silently until the test timeout (e.g. 3m).
		if time.Now().After(nextLog) {
			t.Logf("kafka seed write retry: attempt=%d topic=%q partition=%d err=%v", attempts, writer.Topic, msg.Partition, lastErr)
			nextLog = time.Now().Add(20 * time.Second)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func retryWithBackoff(attempts int, baseDelay time.Duration, fn func() error) error {
	delay := baseDelay
	var err error

	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(delay)
		delay *= 2
	}
	return fmt.Errorf("after %d attempts, last error: %w", attempts, err)
}

func registerSchemaWithRetry(t *testing.T, url, topic, schema string) uint32 {
	body, err := json.Marshal(map[string]string{"schema": schema})
	require.NoError(t, err)

	var schemaID uint32

	client := &http.Client{Timeout: 10 * time.Second}

	err = retryWithBackoff(5, 2*time.Second, func() error {
		resp, err := client.Post(
			fmt.Sprintf("%s/subjects/%s-value/versions", url, topic),
			"application/vnd.schemaregistry.v1+json",
			bytes.NewReader(body),
		)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("registry not ready (status: %d)", resp.StatusCode)
		}

		var res struct {
			ID uint32 `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return err
		}

		schemaID = res.ID
		return nil
	})

	require.NoError(t, err)
	return schemaID
}

func encodeConfluentBinary(id uint32, data []byte) []byte {
	out := make([]byte, 5+len(data))
	out[0] = 0x00
	binary.BigEndian.PutUint32(out[1:5], id)
	copy(out[5:], data)
	return out
}

var ExpectedKafkaJSONData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean_true":    true,
	"boolean_false":   false,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
}

var ExpectedKafkaUpdatedJSONData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean_true":    true,
	"boolean_false":   false,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"id_int":          int64(101),
}

var KafkaToDestinationJSONSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean_true":    "boolean",
	"boolean_false":   "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
}

var EvolvedKafkaToDestinationJSONSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean_true":    "boolean",
	"boolean_false":   "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"id_int":          "bigint",
}

var KafkaToDestinationAvroSchema = map[string]string{
	"int32_value":     "int",
	"int64_value":     "bigint",
	"int_value":       "bigint",
	"float32_value":   "float",
	"float64_value":   "double",
	"float_value":     "double",
	"boolean_true":    "boolean",
	"boolean_false":   "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
}

var EvolvedKafkaToDestinationAvroSchema = map[string]string{
	"int32_value":     "bigint",
	"int64_value":     "bigint",
	"int_value":       "bigint",
	"float32_value":   "float",
	"float64_value":   "double",
	"float_value":     "double",
	"boolean_true":    "boolean",
	"boolean_false":   "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"id_int":          "bigint",
}

var ExpectedKafkaUpdatedAvroData = map[string]interface{}{
	"int32_value":     int64(132), // promoted from int → long
	"int64_value":     int64(6400000000),
	"int_value":       int64(101),
	"float32_value":   float32(32.5),
	"float64_value":   float64(64.6464),
	"float_value":     float64(66.6666),
	"boolean_true":    true,
	"boolean_false":   false,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"id_int":          int64(101), // new field
}

var ExpectedKafkaAvroData = map[string]interface{}{
	"int32_value":     int32(132),
	"int64_value":     int64(6400000000),
	"int_value":       int64(101),
	"float32_value":   float32(32.5),
	"float64_value":   float64(64.6464),
	"float_value":     float64(66.6666),
	"boolean_true":    true,
	"boolean_false":   false,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
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
