package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datazip-inc/olake/utils"
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
	switch operation {
	case "create", "clean", "drop":
		// 1. Dial a reachable broker for admin operations
		conn, err := kafka.DialContext(ctx, "tcp", activeBroker)
		require.NoError(t, err, "failed to dial kafka for topic creation")
		defer conn.Close()

		// 2. Loop over provided streams
		for i := 0; i < len(streams); i++ {

			// 3. If it's a "clean" or "drop" operation, delete first
			if operation == "clean" || operation == "drop" {
				_ = conn.DeleteTopics(streams[i])
				time.Sleep(5 * time.Second)
				if operation == "drop" {
					continue
				}
			}
			partitionNumber := utils.Ternary(i == 0, 1, 5).(int)
			err = conn.CreateTopics(kafka.TopicConfig{
				Topic:             streams[i],
				NumPartitions:     partitionNumber,
				ReplicationFactor: 1,
			})
			// 3. Ignore if already exists
			if err != nil && err != kafka.TopicAlreadyExists {
				require.NoError(t, err, "failed to create topic '%s' explicitly", streams[i])
			}
			t.Logf("Topic '%s' is ready for writes (%d partitions)", streams[i], partitionNumber)
		}
		return
	case "add", "insert":
		// NEW: Initialize the writer only when needed
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false, // Much safer!
			MaxAttempts:            10,
		}
		defer writer.Close()

		for _, s := range streams {
			if strings.HasSuffix(s, "_2") {
				// Topic _2: Round-robin 3 messages across 3 partitions (0, 3, 4)
				for _, p := range []int{0, 3, 4} {
					addDataToPartition(ctx, t, writer, s, p, key, value)
				}
				t.Logf("Added 3 messages to topic '%s' across partitions 0, 3, 4", s)
			} else if strings.HasSuffix(s, "_3") {
				// Topic _3: Fill all 5 partitions (0-4)
				for p := 0; p < 5; p++ {
					addDataToPartition(ctx, t, writer, s, p, key, value)
				}
				t.Logf("Added 5 messages to topic '%s' (one per partition)", s)
			} else {
				// Topic _1 OR base topic: 5 messages in its single partition (0)
				for i := 0; i < 5; i++ {
					addDataToPartition(ctx, t, writer, s, 0, key, value)
				}
				t.Logf("Added 5 messages to topic '%s' in partition 0", s)
			}
		}
		return
	case "evolve-schema":
		// NEW: Initialize the writer only when needed
		writer := &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false,
			MaxAttempts:            10,
		}
		defer writer.Close()

		for _, s := range streams {
			if strings.HasSuffix(s, "_2") {
				// Topic _2: Round-robin 3 messages across 3 partitions (0, 3, 4)
				for _, p := range []int{0, 3, 4} {
					addDataToPartition(ctx, t, writer, s, p, key, evolved_value)
				}
				t.Logf("Added 3 messages to topic '%s' across partitions 0, 3, 4", s)
			} else if strings.HasSuffix(s, "_3") {
				// Topic _3: Fill all 5 partitions (0-4)
				for p := 0; p < 5; p++ {
					addDataToPartition(ctx, t, writer, s, p, key, evolved_value)
				}
				t.Logf("Added 5 messages to topic '%s' (one per partition)", s)
			} else {
				// Topic _1 OR base topic: 5 messages in its single partition (0)
				for i := 0; i < 5; i++ {
					addDataToPartition(ctx, t, writer, s, 0, key, evolved_value)
				}
				t.Logf("Added 5 messages to topic '%s' in partition 0", s)
			}
		}
		return
	case "Avro-insert":
		
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
