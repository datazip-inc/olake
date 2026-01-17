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
	brokerAddress = "localhost:9094"
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
		// Create a topic (used by Discover test)
		createTopic(ctx, t, currentTopic)

	case "clean":
		// Delete and recreate the topic for a fresh test state
		deleteTopic(ctx, t, currentTopic)
		time.Sleep(2 * time.Second)
		createTopic(ctx, t, currentTopic)
		time.Sleep(3 * time.Second) // Wait for topic to be fully available

	case "add":
		// Produce initial messages
		produceMessages(ctx, t, currentTopic, false)

	case "insert":
		// Produce additional messages for incremental sync test
		produceMessages(ctx, t, currentTopic, true)

	case "drop":
		// delete the topic
		deleteTopic(ctx, t, currentTopic)

	default:
		t.Logf("Operation %s not implemented for Kafka (append-only)", operation)
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

var ExpectedKafkaData = map[string]interface{}{
	"id":    int64(6),
	"name":  "name_6",
	"price": float64(63.0),
}

// ExpectedUpdatedData not used for Kafka (append-only), but required by test framework
var ExpectedUpdatedData = map[string]interface{}{
	"id":    int64(6),
	"name":  "name_6",
	"price": float64(63.0),
}

var KafkaToDestinationSchema = map[string]string{
	"id":        "long",
	"name":      "string",
	"price":     "double",
	"timestamp": "string",
}
