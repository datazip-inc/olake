package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/segmentio/kafka-go"
)

// IncrementalChanges is not supported for MySQL
func (k *Kafka) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	topic := stream.Name()
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

	// Get persisted state offsets, partitions
	stateOffsets := make(map[int]int64)
	if k.state != nil {
		cursor := k.state.GetCursor(stream.Self(), "partitions")
		if cursor != nil {
			partitionMap, ok := cursor.(map[string]any)
			if ok {
				for _, partition := range topicDetail.Partitions {
					partitionID := fmt.Sprintf("%d", partition.ID)
					if val, exists := partitionMap[partitionID]; exists {
						offset, _ := typeutils.ReformatInt64(val)
						stateOffsets[partition.ID] = offset
					}
				}
			}
		}
	}

	// Generate a new consumer group ID if not configured
	groupID := k.config.ConsumerGroup
	if groupID == "" {
		groupID = fmt.Sprintf("olake-consumer-incremental-%s-%d", stream.ID(), time.Now().Unix())
		logger.Infof("[KAFKA] No consumer group specified; using generated group ID: %s", groupID)
	}

	// Create consumer group to handle partition assignment
	consumerGroup, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      groupID,
		Brokers: strings.Split(k.config.BootstrapServers, ","),
		Topics:  []string{topic},
	})
	if err != nil {
		return fmt.Errorf("[KAFKA] failed to create consumer group for incremental sync: %v", err)
	}

	// Current generation (set of partition assignments) for the consumer group
	consumerGen, err := consumerGroup.Next(ctx)
	if err != nil {
		return fmt.Errorf("[KAFKA] failed to get consumer group generation: %v", err)
	}

	k.mutex.Lock()
	k.consumerGroups[topic] = consumerGroup
	k.consumerGen = consumerGen
	k.mutex.Unlock()

	offsetsLock := &sync.Mutex{}
	lastProcessedOffsets := make(map[int]int64)

	// Prepare partition data for concurrent processing
	partitionData := make([]struct {
		Partition   int
		StartOffset int64
	}, 0, len(consumerGen.Assignments[topic]))

	// Building partition data first
	for _, assignment := range consumerGen.Assignments[topic] {
		partition := assignment.ID
		startOffset := assignment.Offset

		// If we have state for this partition, use it
		if offset, exists := stateOffsets[partition]; exists && offset > 0 {
			startOffset = offset + 1
		}
		partitionData = append(partitionData, struct {
			Partition   int
			StartOffset int64
		}{
			Partition:   partition,
			StartOffset: startOffset,
		})
	}

	// Processing partitions concurrently
	err = utils.Concurrent(ctx, partitionData, k.config.MaxThreads, func(ctx context.Context, data struct {
		Partition   int
		StartOffset int64
	}, _ int) error {
		partition := data.Partition
		startOffset := data.StartOffset

		// Partition specific-reader
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   strings.Split(k.config.BootstrapServers, ","),
			Topic:     topic,
			Partition: partition,
			MinBytes:  1,
			MaxBytes:  10e6,
			Dialer:    k.dialer,
		})
		defer reader.Close()

		// Set offset based on state or from beginning
		if err := reader.SetOffset(startOffset); err != nil {
			var kerr kafka.Error
			// If offset is invalid, fallback to configured offset reset policy
			if errors.Is(kerr, kafka.OffsetOutOfRange) {
				logger.Warnf("[KAFKA] offset %d out of range for topic %s, partition %d, resetting to beginning", startOffset, topic, partition)
				resetOffset, resetOffsetErr := ResolveOffset(k.config.AutoOffsetReset)
				if resetOffsetErr != nil {
					logger.Errorf("[KAFKA] invalid auto_offset_reset policy: %s", resetOffsetErr)
				}
				if setOffsetErr := reader.SetOffset(resetOffset); setOffsetErr != nil {
					logger.Errorf("[KAFKA] failed to reset offset to %d for topic %s, partition %d: %s", resetOffset, topic, partition, setOffsetErr)
				}
			} else {
				logger.Errorf("[KAFKA] failed to set offset %d for topic %s, partition %d: %v", startOffset, topic, partition, err)
				return err
			}
		}

		logger.Infof("[KAFKA] starting incremental sync for topic %s, partition %d", topic, partition)

		lastOffset := startOffset
		processedAny := false
		// safely store last processed offset
		saveOffset := func() {
			if processedAny {
				offsetsLock.Lock()
				lastProcessedOffsets[partition] = lastOffset
				offsetsLock.Unlock()
			}
		}

		for {
			select {
			case <-ctx.Done():
				logger.Infof("[KAFKA] incremental sync stopped for topic %s, partition %d due to context cancellation", topic, partition)
				saveOffset()
				return ctx.Err()
			default:
				// Read message within user-provided deadline
				remaining := time.Until(time.Now().Add(time.Duration(k.config.WaitTime) * time.Second))
				if remaining <= 0 {
					logger.Warnf("[KAFKA] wait time exhausted for topic %s, partition %d", topic, partition)
					saveOffset()
					return nil
				}

				fetchCtx, cancel := context.WithTimeout(ctx, remaining)
				msg, err := reader.ReadMessage(fetchCtx)
				cancel()

				// Handle message read errors
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						// no data within wait time
						logger.Infof("[KAFKA] no messages in topic %s, partition %d within remaining time", topic, partition)
						saveOffset()
						return nil
					}
					if errors.Is(err, kafka.ErrGenerationEnded) {
						// current generation ended, rebalance happened
						logger.Infof("[KAFKA] generation ended for partition %d", partition)
						saveOffset()
						return nil
					}
					if errors.Is(err, context.Canceled) {
						// context canceled i.e. shutdown
						logger.Infof("[KAFKA] context canceled for partition %d", partition)
						return nil
					}
					// any failure during reading
					logger.Errorf("[KAFKA] error reading message in incremental sync: %v", err)
					saveOffset()
					return err
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

				// Process message with provided function
				if err := processFn(fetchCtx, data); err != nil {
					logger.Errorf("[KAFKA] failed to process message at offset %d: %v", msg.Offset, err)
					offsetsLock.Lock()
					lastProcessedOffsets[partition] = lastOffset
					offsetsLock.Unlock()
					return err
				}
				lastOffset = msg.Offset
				processedAny = true
				logger.Debugf("[KAFKA] processed incremental message at offset %d for topic %s, partition %d", msg.Offset, topic, partition)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("[KAFKA] error during concurrent partition processing: %v", err)
	}

	// save last offsets per partition for post commit
	k.mutex.Lock()
	defer k.mutex.Unlock()
	if _, ok := k.offsetMap[topic]; !ok {
		k.offsetMap[topic] = make(map[int]int64)
	}
	for partition, offset := range lastProcessedOffsets {
		k.offsetMap[topic][partition] = offset
	}

	return nil
}

func (k *Kafka) PostIncremental(_ context.Context, stream types.StreamInterface, noErr bool) error {
	if noErr {
		k.mutex.Lock()
		defer k.mutex.Unlock()
		if k.consumerGen == nil {
			return fmt.Errorf("[KAFKA] no active consumer generation for commit")
		}

		topic := stream.Name()
		offsets, ok := k.offsetMap[topic]
		if !ok {
			return fmt.Errorf("[KAFKA] no offsets found for topic %s", topic)
		}

		partitionsState := map[string]int64{}
		commitOffsets := map[string]map[int]int64{topic: {}}
		commitRequired := false

		// Prepare offsets to be committed
		if len(offsets) > 0 {
			for partition, offset := range offsets {
				commitOffsets[topic][partition] = offset + 1
				partitionsState[fmt.Sprintf("%d", partition)] = offset + 1
				commitRequired = true
			}
		}

		if commitRequired {
			// Kafka expects the next offset to be read, so commit offset+1
			k.state.SetCursor(stream.Self(), "partitions", partitionsState)

			// commit to kafka
			if err := k.consumerGen.CommitOffsets(commitOffsets); err != nil {
				return fmt.Errorf("[KAFKA] failed to commit offsets to Kafka: %w", err)
			}
			logger.Infof("[KAFKA] offsets committed for topic %s", topic)
		} else {
			// Preserve existing state
			if cursor := k.state.GetCursor(stream.Self(), "partitions"); cursor != nil {
				k.state.SetCursor(stream.Self(), "partitions", cursor)
			}
			logger.Infof("[KAFKA] no offsets to commit for topic %s", topic)
		}

		k.syncedTopics[topic] = true
	}
	return nil
}

// Provides auto offset resolution based on the policy
func ResolveOffset(policy string) (int64, error) {
	switch policy {
	case "earliest":
		return kafka.FirstOffset, nil
	case "latest":
		return kafka.LastOffset, nil
	default:
		return 0, fmt.Errorf("[KAFKA] invalid auto_offset_reset value: %q", policy)
	}
}
