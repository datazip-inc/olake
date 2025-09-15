package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
)

func (k *Kafka) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	topic := stream.Name()

	consumerGroupID := k.consumerGroupID
	// get consumer group id from state
	if k.state != nil {
		if cursor := k.state.GetCursor(stream.Self(), "consumer_group_id"); cursor != nil {
			if gID, ok := cursor.(string); ok && gID != "" {
				consumerGroupID = gID
			}
		}
	}
	logger.Infof("[KAFKA] using consumer group id: %s for topic: %s", consumerGroupID, topic)

	// Fetch topic metadata to get partitions
	metadataReq := &kafka.MetadataRequest{
		Topics: []string{topic},
	}
	metadataResp, err := k.adminClient.Metadata(ctx, metadataReq)
	if err != nil {
		return fmt.Errorf("[KAFKA] failed to fetch topic metadata: %v", err)
	}

	var topicDetail kafka.Topic
	for _, t := range metadataResp.Topics {
		if t.Name == topic {
			topicDetail = t
			break
		}
	}
	if topicDetail.Error != nil {
		return fmt.Errorf("[KAFKA] topic %s not found in metadata: %v", topic, topicDetail.Error)
	}

	// Prepare partition data for concurrent processing
	partitionData := make([]struct {
		Partition   int
		StartOffset int64
	}, 0, len(topicDetail.Partitions))

	// Building partition data first
	for _, partition := range topicDetail.Partitions {
		partitionID := partition.ID
		startOffset := utils.Ternary(k.config.AutoOffsetReset == "latest", kafka.LastOffset, kafka.FirstOffset).(int64)
		partitionData = append(partitionData, struct {
			Partition   int
			StartOffset int64
		}{
			Partition:   partitionID,
			StartOffset: startOffset,
		})
	}

	// Processing partitions concurrently
	err = utils.Concurrent(ctx, partitionData, k.config.MaxThreads, func(ctx context.Context, data struct {
		Partition   int
		StartOffset int64
	}, _ int) error {
		partition := data.Partition
		// Partition specific-reader
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  strings.Split(k.config.BootstrapServers, ","),
			Topic:    topic,
			GroupID:  consumerGroupID,
			MinBytes: 1,
			MaxBytes: 10e6,
			Dialer:   k.dialer,
		})
		defer func() {
			if err := reader.Close(); err != nil {
				logger.Warnf("[KAFKA] failed to close reader for topic %s, partition %d: %v", topic, partition, err)
			}
		}()
		logger.Infof("[KAFKA] starting incremental sync for topic %s, partition %d", topic, partition)
		// per-partition wait time after last processed message
		partitionIdleWait := time.Duration(k.config.WaitTime) * time.Second
		for {
			select {
			case <-ctx.Done():
				logger.Infof("[KAFKA] incremental sync stopped for topic %s, partition %d due to context cancellation", topic, partition)
				return ctx.Err()
			default:
				deadline := time.Now().Add(partitionIdleWait)
				fetchCtx, cancel := context.WithDeadline(ctx, deadline)
				msg, err := reader.FetchMessage(fetchCtx)
				cancel()

				// Handle message read errors
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						// no data within wait time
						logger.Infof("[KAFKA] no messages in topic %s, partition %d within remaining time", topic, partition)
						return nil
					}
					if errors.Is(err, kafka.ErrGenerationEnded) {
						// current generation ended, rebalance happened
						logger.Infof("[KAFKA] generation ended for partition %d", partition)
						return nil
					}
					if errors.Is(err, context.Canceled) {
						// context canceled i.e. shutdown
						logger.Infof("[KAFKA] context canceled for partition %d", partition)
						return nil
					}
					// any failure during reading
					return fmt.Errorf("[KAFKA] error reading message in incremental sync: %v", err)
				}
				data := func() map[string]any {
					var result map[string]any
					if msg.Value == nil {
						logger.Warnf("[KAFKA] received nil message value at offset %d for topic %s, partition %d", msg.Offset, topic, partition)
						return nil
					}
					if err := json.Unmarshal(msg.Value, &result); err != nil {
						logger.Errorf("[KAFKA] failed to unmarshal message value: %v", err)
						return nil
					}
					result["partition"] = msg.Partition
					result["offset"] = msg.Offset
					result["key"] = string(msg.Key)
					result["kafka_timestamp"] = msg.Time.UnixMilli()
					return result
				}()
				// skip processing if data is nil
				if data == nil {
					continue
				}
				// Process message with provided function
				if err := processFn(ctx, data); err != nil {
					return err
				}
				// commit partition in consumer side, after processfn call (which has the writer commit)
				if err := reader.CommitMessages(ctx, msg); err != nil {
					return fmt.Errorf("[KAFKA] failed to commit message at offset %d for topic %s, partition %d: %v", msg.Offset, topic, partition, err)
				}
				logger.Infof("[KAFKA] processed incremental message at offset %d for topic %s, partition %d", msg.Offset, topic, partition)
			}
		}
	})
	return err
}

func (k *Kafka) PostIncremental(_ context.Context, stream types.StreamInterface, noErr bool) error {
	if noErr {
		k.mutex.Lock()
		defer k.mutex.Unlock()
		topic := stream.Name()
		// state save consumer group id
		if k.consumerGroupID != "" {
			k.state.SetCursor(stream.Self(), "consumer_group_id", k.consumerGroupID)
		}
		k.syncedTopics[topic] = true
	}
	return nil
}
