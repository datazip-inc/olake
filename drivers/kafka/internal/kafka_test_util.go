package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

const (
	brokerAddress = "localhost:9095"
	topic         = "test_topic"
)

type TestMessage struct {
	ID        int     `json:"id"`
	Name      string  `json:"name"`
	Price     float64 `json:"price"`
	Timestamp string  `json:"timestamp"`
}

// ExecuteQuery executes Kafka operations for testing
func ExecuteQuery(ctx context.Context, t *testing.T, streams []string, operation string, fileConfig bool) {
	t.Helper()

	// For integration tests, we assume the topic is the first stream
	// But in Kafka driver, streams are topics.
	// The test framework passes streams[0] as the table/stream name.
	currentTopic := streams[0]
	if currentTopic == "" {
		currentTopic = topic
	}

	switch operation {
	case "create":
		createTopic(ctx, t, currentTopic)

	case "drop":
		deleteTopic(ctx, t, currentTopic)

	case "clean":
		// In Kafka, "cleaning" could mean deleting the topic and recreating it
		// For simplicity, we'll recreate the topic.
		deleteTopic(ctx, t, currentTopic)
		createTopic(ctx, t, currentTopic)

	case "add":
		produceMessages(ctx, t, currentTopic, false)

	case "insert":
		// Produce more messages
		produceMessages(ctx, t, currentTopic, true)

	case "update":
		// Produce messages with same ID but different content
		produceUpdateMessages(ctx, t, currentTopic)

	case "delete":
		// skip explicit delete for now or implement a tombstone if applicable.
		t.Log("Delete operation not fully implemented for Kafka test helper yet")

	case "evolve-schema":
		// Kafka is schema-less, so this might be a no-op
		// or producing messages with new fields.
		t.Log("Evolve schema operation - producing message with new field")
		produceEvolvedMessage(ctx, t, currentTopic)

	default:
		t.Fatalf("Unsupported operation: %s", operation)
	}
}

func createTopic(ctx context.Context, t *testing.T, topicName string) {
	client := &kafka.Client{
		Addr: kafka.TCP(brokerAddress),
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             topicName,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	})
	if err != nil {
		t.Logf("CreateTopics transport error: %v", err)
		return
	}

	for _, err := range resp.Errors {
		if err != nil {
			t.Logf("CreateTopics error: %v", err)
		}
	}
}

func deleteTopic(ctx context.Context, t *testing.T, topicName string) {
	client := &kafka.Client{
		Addr: kafka.TCP(brokerAddress),
	}

	resp, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{
		Topics: []string{topicName},
	})
	if err != nil {
		t.Logf("DeleteTopics transport error: %v", err)
		return
	}

	for _, err := range resp.Errors {
		if err != nil {
			t.Logf("DeleteTopics error: %v", err)
		}
	}
}

func produceMessages(ctx context.Context, t *testing.T, topicName string, isCDC bool) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	messages := []kafka.Message{}
	startID := 1
	if isCDC {
		startID = 6 // Continue from where "add" left off
	}

	for i := startID; i < startID+5; i++ {
		msg := TestMessage{
			ID:        i,
			Name:      fmt.Sprintf("name_%d", i),
			Price:     float64(i) * 10.5,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}
		val, err := json.Marshal(msg)
		require.NoError(t, err)

		messages = append(messages, kafka.Message{
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: val,
		})
	}

	err := w.WriteMessages(ctx, messages...)
	require.NoError(t, err, "Failed to write messages to Kafka")
}

func produceUpdateMessages(ctx context.Context, t *testing.T, topicName string) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	msg := TestMessage{
		ID:        6,
		Name:      "updated_name_6",
		Price:     999.99,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	val, err := json.Marshal(msg)
	require.NoError(t, err)

	err = w.WriteMessages(ctx, kafka.Message{
		Key:   []byte("6"),
		Value: val,
	})
	require.NoError(t, err, "Failed to write update message to Kafka")
}

func produceEvolvedMessage(ctx context.Context, t *testing.T, topicName string) {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	msg := map[string]interface{}{
		"id":        7,
		"name":      "evolved_name",
		"price":     77.7,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"new_field": "extra_data",
	}
	val, err := json.Marshal(msg)
	require.NoError(t, err)

	err = w.WriteMessages(ctx, kafka.Message{
		Key:   []byte("7"),
		Value: val,
	})
	require.NoError(t, err, "Failed to write evolved message to Kafka")
}

var ExpectedKafkaData = map[string]interface{}{
	"id":    int64(6),
	"name":  "name_6",
	"price": float64(63.0),
}

var ExpectedUpdatedData = map[string]interface{}{
	"id":    int64(6),
	"name":  "updated_name_6",
	"price": float64(999.99),
}

var KafkaToDestinationSchema = map[string]string{
	"id":        "long",
	"name":      "string",
	"price":     "double",
	"timestamp": "string",
}
