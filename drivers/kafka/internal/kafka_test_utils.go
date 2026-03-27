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

// ExecuteQuery executes Kafka queries for testing based on the operation type
func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	var brokers []string
	if fileConfig {
		var config Config
		require.NoError(t, utils.UnmarshalFile("./testdata/source.json", &config, false), "failed to unmarshal kafka test source config")
		brokers = strings.Split(config.BootstrapServers, ",")
	} else {
		brokers = []string{"127.0.0.1:29092"}
	}

	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(strings.ReplaceAll(b, "host.docker.internal", "127.0.0.1"))
	}

	activeBroker := waitForAnyKafkaBroker(ctx, t, brokers)

	// Message key and value
	key := []byte("test-key")
	value := []byte(`{"int_value": 100,"float_value": 99.99,"boolean_true": true,"boolean_false": false,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string"}`)
	evolved_value := []byte(`{"int_value": 100,"float_value": 99.99,"boolean_true": true,"boolean_false": false,"timestamp_value": "2026-03-22T14:30:00Z","string_value": "test_string", "id_int": 101}`)
	filtervalue1 := []byte(`{"int_value": 99,"float_value": 99.99}`)
	filtervalue2 := []byte(`{"int_value": 100,"float_value": 100.00}`)
	switch operation {
	case "create", "clean", "drop":
		// 1. Dial a reachable broker for admin operations
		conn, err := kafka.DialContext(ctx, "tcp", activeBroker)
		require.NoError(t, err, "failed to dial kafka for topic creation")
		defer conn.Close()
		// 3. If it's a "clean" or "drop" operation, delete first
		if operation == "clean" || operation == "drop" {
			_ = conn.DeleteTopics(streams[0])
			time.Sleep(5 * time.Second)
			if operation == "drop" {
				return
			}
		}
		partitionNumber := 5
		err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             streams[0],
			NumPartitions:     partitionNumber,
			ReplicationFactor: 1,
		})
		// 3. Ignore if already exists
		if err != nil && err != kafka.TopicAlreadyExists {
			require.NoError(t, err, "failed to create topic '%s' explicitly", streams[0])
		}
		t.Logf("Topic '%s' is ready for writes (%d partitions)", streams[0], partitionNumber)
	case "add", "insert":
		// NEW: Initialize the writer only when needed
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false, // Much safer!
			MaxAttempts:            10,
		}
		defer writer.Close()
		for p := 0; p < 5; p++ {
			addDataToPartition(ctx, t, writer, streams[0], p, key, value)
		}
		addDataToPartition(ctx, t, writer, streams[0], 0, key, filtervalue1)
		addDataToPartition(ctx, t, writer, streams[0], 1, key, filtervalue2)
		t.Logf("Added 5 messages to topic '%s' (one per partition)", streams[0])
	case "evolve-schema":
		// NEW: Initialize the writer only when needed
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false,
			MaxAttempts:            10,
		}
		defer writer.Close()
		for p := 0; p < 5; p++ {
			addDataToPartition(ctx, t, writer, streams[0], p, key, evolved_value)
		}
		t.Logf("Added 5 messages to topic '%s' (one per partition)", streams[0])
	case "Avro-insert", "Avro-evolve-schema":
		var config Config
		utils.UnmarshalFile("./testdata/source.json", &config, false)

		registryURL := config.SchemaRegistry.Endpoint
		registryURL = strings.ReplaceAll(registryURL, "host.docker.internal", "127.0.0.1")

		writer := &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.LeastBytes{},
			MaxAttempts:  10,
			RequiredAcks: kafka.RequireAll,
		}
		defer writer.Close()

		// Base schema
		schemaV1 := `{
			"type":"record",
			"name":"test",
			"fields":[
				{"name":"int32_value","type":"int"},
				{"name":"int64_value","type":"long"},
				{"name":"int_value","type":"long"},
				{"name":"float32_value","type":"float"},
				{"name":"float64_value","type":"double"},
				{"name":"float_value","type":"double"},
				{"name":"boolean_true","type":"boolean"},
				{"name":"boolean_false","type":"boolean"},
				{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"}},
				{"name":"string_value","type":"string"}
			]
		}`

		// Evolved schema
		schemaV2 := `{
			"type":"record",
			"name":"test",
			"fields":[
				{"name":"int32_value","type":"long"},
				{"name":"int64_value","type":"long"},
				{"name":"int_value","type":"long"},
				{"name":"float32_value","type":"float"},
				{"name":"float64_value","type":"double"},
				{"name":"float_value","type":"double"},
				{"name":"boolean_true","type":"boolean"},
				{"name":"boolean_false","type":"boolean"},
				{"name":"timestamp_value","type":{"type":"long","logicalType":"timestamp-micros"}},
				{"name":"string_value","type":"string"},
				{"name":"id_int","type":"long","default":0}
			]
		}`

		schema := schemaV1
		if operation == "Avro-evolve-schema" {
			schema = schemaV2
		}

		codec, err := goavro.NewCodec(schema)
		require.NoError(t, err)

		schemaID := registerSchemaWithRetry(t, registryURL, streams[0], schema)

		dataToProduce := map[string]interface{}{
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

		if operation == "Avro-evolve-schema" {
			dataToProduce["id_int"] = int64(101)
		}

		binaryData, err := codec.BinaryFromNative(nil, dataToProduce)
		require.NoError(t, err)

		confluentMsg := encodeConfluentBinary(schemaID, binaryData)

		err = writer.WriteMessages(ctx, kafka.Message{
			Topic: streams[0],
			Key:   []byte("avro-key"),
			Value: confluentMsg,
		})
		require.NoError(t, err)
	default:
		t.Fatalf("unsupported operation: %s", operation)
	}
}

func addDataToPartition(ctx context.Context, t *testing.T, writer *kafka.Writer, topic string, partition int, key, value []byte) {
	t.Helper()
	originalBalancer := writer.Balancer
	writer.Balancer = nil
	writer.Topic = topic
	defer func() { writer.Balancer = originalBalancer }()
	writeMessagesWithRetry(ctx, t, writer, kafka.Message{
		Key:       key,
		Value:     value,
		Partition: partition,
	})
	t.Logf("Added message to topic '%s', partition %d", topic, partition)
}

func waitForAnyKafkaBroker(ctx context.Context, t *testing.T, brokers []string) string {
	t.Helper()

	var lastErr error

	for {
		for _, b := range brokers {
			conn, err := kafka.DialContext(ctx, "tcp", b)
			if err != nil {
				lastErr = err
				continue
			}

			// 🔥 Real readiness check: metadata must be available
			_, err = conn.ReadPartitions()
			if err != nil {
				lastErr = err
				t.Logf("Waiting for Kafka broker %s to initialize metadata... (%v)", b, err)
				_ = conn.Close() // IMPORTANT: avoid connection leaks
				continue
			}

			_ = conn.Close()

			t.Logf("Kafka broker %s is fully ready (metadata available)", b)

			// Optional: small buffer for stability in slower environments
			time.Sleep(1 * time.Second)

			return b
		}

		// Respect test context timeout
		if err := ctx.Err(); err != nil {
			t.Fatalf("kafka brokers not ready (tried: %v): last error: %v", brokers, lastErr)
		}

		// Slightly relaxed retry interval
		time.Sleep(1 * time.Second)
	}
}

func writeMessagesWithRetry(ctx context.Context, t *testing.T, writer *kafka.Writer, msg kafka.Message) {
	t.Helper()

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
		// Without this, a bad broker/topic state spins silently until the test timeout (e.g. 30m).
		if time.Now().After(nextLog) {
			t.Logf("kafka seed write retry: attempt=%d topic=%q partition=%d err=%v", attempts, writer.Topic, msg.Partition, lastErr)
			nextLog = time.Now().Add(5 * time.Second)
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

var ExpectedKafkaData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean_true":    true,
	"boolean_false":   false,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
}

var ExpectedKafkaUpdatedData = map[string]interface{}{
	"int_value":       int64(100),
	"float_value":     float64(99.99),
	"boolean_true":    true,
	"boolean_false":   false,
	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),
	"string_value":    "test_string",
	"id_int":          int64(101),
}

var KafkaToDestinationSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean_true":    "boolean",
	"boolean_false":   "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
}

var EvolvedKafkaToDestinationSchema = map[string]string{
	"int_value":       "bigint",
	"float_value":     "double",
	"boolean_true":    "boolean",
	"boolean_false":   "boolean",
	"timestamp_value": "timestamp",
	"string_value":    "string",
	"id_int":          "bigint",
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

var ExpectedKafkaAvroDataSchema = map[string]string{
	"int32_value": "int",
	"int64_value": "bigint",
	"int_value": "bigint",
	"float32_value": "float",
	"float64_value": "double",
	"float_value": "double",
	"boolean_true": "boolean",
	"boolean_false": "boolean",
	"timestamp_value": "timestamp",
	"string_value": "string",
}
	
var ExpectedKafkaAvroUpdatedDataSchema = map[string]string{
	"int32_value": "bigint",
	"int64_value": "bigint",
	"int_value": "bigint",
	"float32_value": "float",
	"float64_value": "double",
	"float_value": "double",
	"boolean_true": "boolean",
	"boolean_false": "boolean",
	"timestamp_value": "timestamp",
	"string_value": "string",
	"id_int": "bigint",
}

var ExpectedKafkaAvroUpdatedData = map[string]interface{}{
	"int32_value": int64(132), // promoted from int → long
	"int64_value": int64(6400000000),
	"int_value": int64(101),

	"float32_value": float32(32.5),
	"float64_value": float64(64.6464),
	"float_value":     float64(66.6666),

	"boolean_true":  true,
	"boolean_false": false,

	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),

	"string_value": "test_string",

	"id_int": int64(101), // new field
}

var ExpectedKafkaAvroData = map[string]interface{}{
	"int32_value": int32(132),
	"int64_value": int64(6400000000),
	"int_value": int64(101),

	"float32_value": float32(32.5),
	"float64_value": float64(64.6464),
	"float_value":     float64(66.6666),

	"boolean_true":  true,
	"boolean_false": false,

	"timestamp_value": arrow.Timestamp(time.Date(2026, 3, 22, 14, 30, 0, 0, time.UTC).UnixNano() / int64(time.Microsecond)),

	"string_value": "test_string",
}
