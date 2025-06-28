package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
)

// StreamIncremental processes messages in real-time for incremental sync.
func (k *Kafka) StreamIncremental(ctx context.Context, stream types.StreamInterface, processFn abstract.IncrementalMsgFn) error {
	topic := stream.Name()

	// Fetch topic metadata to get partitions
	metadataReq := &kafka.MetadataRequest{
		Topics: []string{topic},
	}
	metadataResp, err := k.adminClient.Metadata(ctx, metadataReq)
	if err != nil {
		return fmt.Errorf("failed to fetch topic metadata: %v", err)
	}

	var topicDetail kafka.Topic
	for _, t := range metadataResp.Topics {
		if t.Name == topic {
			topicDetail = t
			break
		}
	}
	if topicDetail.Error != nil {
		return fmt.Errorf("topic %s not found in metadata: %v", topic, topicDetail.Error)
	}

	// Set up consumer group
	groupID := k.config.ConsumerGroup
	if groupID == "" {
		groupID = fmt.Sprintf("olake-consumer-incremental-%d", time.Now().UnixNano())
		logger.Infof("No consumer group specified; using generated group ID: %s", groupID)
	}

	// Create a reader with consumer group
	readerConfig := kafka.ReaderConfig{
		Brokers:  strings.Split(k.config.BootstrapServers, ","),
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		Dialer:   k.dialer,
	}

	switch k.config.AutoOffsetReset {
	case "earliest":
		readerConfig.StartOffset = kafka.FirstOffset
		logger.Infof("EARLIEST %s: %v", topic, readerConfig.StartOffset)
	case "latest":
		readerConfig.StartOffset = kafka.LastOffset
		logger.Infof("LATEST %s: %v", topic, readerConfig.StartOffset)
	case "none":
		// Let the consumer group manage the offset
	default:
		return fmt.Errorf("invalid auto_offset_reset value: %s", k.config.AutoOffsetReset)
	}

	reader := kafka.NewReader(readerConfig)
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Warnf("Closing reader for topic %s: %v", topic, err)
		}
	}()

	// Stream messages continuously
	for {
		select {
		case <-ctx.Done():
			logger.Infof("Streaming stopped for topic %s due to context cancellation", topic)
			return nil
		default:
			fetchCtx, cancel := context.WithTimeout(ctx, time.Duration(k.config.WaitTime))
			msg, err := reader.ReadMessage(fetchCtx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					break // No messages
				}
				logger.Warnf("No more available messages to fetch for topic %s, issue: %v; attempting to continue", topic, err)
				return nil
			}

			data := func() map[string]interface{} {
				var result map[string]interface{}
				if err := json.Unmarshal(msg.Value, &result); err != nil {
					logger.Errorf("Failed to unmarshal message value: %v", err)
					return nil
				}
				result["partition"] = msg.Partition
				result["offset"] = msg.Offset
				return result
			}()

			if err := processFn(data); err != nil {
				logger.Errorf("Failed to process message at offset %d: %v", msg.Offset, err)
				return err
			}

			// Commit the offset to Kafka
			if err := reader.CommitMessages(ctx, msg); err != nil {
				logger.Errorf("Failed to commit offset %d: %v", msg.Offset, err)
				return err
			}
			logger.Infof("Processed and committed message at offset %d for topic %s, partition %d", msg.Offset, topic, msg.Partition)
		}
	}
}
