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

	// Get persisted state offsets, partitions
	stateOffsets := make(map[int]int64)
	if k.state != nil {
		cursor := k.state.GetCursor(stream.Self(), "partitions")
		if cursor != nil {
			partitionMap, ok := cursor.(map[string]interface{})
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
		logger.Infof("No consumer group specified; using generated group ID: %s", groupID)
	}

	// Create consumer group to handle partition assignment
	consumerGroup, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      groupID,
		Brokers: strings.Split(k.config.BootstrapServers, ","),
		Topics:  []string{topic},
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer group for incremental sync: %v", err)
	}

	// Current generation (set of partition assignments) for the consumer group
	consumerGen, err := consumerGroup.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consumer group generation: %v", err)
	}

	k.mutex.Lock()
	k.consumerGroups[topic] = consumerGroup
	k.consumerGen = consumerGen
	k.mutex.Unlock()

	var wg sync.WaitGroup
	offsetsLock := &sync.Mutex{}
	lastProcessedOffsets := make(map[int]int64)

	// Process each assigned partition concurrently
	for _, assignment := range consumerGen.Assignments[topic] {
		partition := assignment.ID
		startOffset := assignment.Offset
		// If we have state for this partition, use it
		if offset, exists := stateOffsets[partition]; exists && offset > 0 {
			startOffset = offset
		}
		wg.Add(1)
		go func(partition int, startOffset int64) {
			defer wg.Done()

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
				// If offset is invalid, fallback to configured reset policy
				if errors.Is(kerr, kafka.OffsetOutOfRange) {
					logger.Warnf("Offset %d out of range for topic %s, partition %d, resetting to beginning", startOffset, topic, partition)
					resetOffset, resetOffsetErr := ResolveOffset(k.config.AutoOffsetReset)
					if resetOffsetErr != nil {
						logger.Errorf("Invalid auto_offset_reset policy: %s", resetOffsetErr)
					}
					if setOffsetErr := reader.SetOffset(resetOffset); setOffsetErr != nil {
						logger.Errorf("Failed to reset offset to %d for topic %s, partition %d: %s", resetOffset, topic, partition, setOffsetErr)
					}
				} else {
					logger.Errorf("failed to set offset %d for topic %s, partition %d: %v", startOffset, topic, partition, err)
					return
				}
			}

			logger.Infof("Starting incremental sync for topic %s, partition %d", topic, partition)

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
					logger.Infof("Incremental sync stopped for topic %s, partition %d due to context cancellation", topic, partition)
					saveOffset()
					return
				default:
					// Read message within user-provided deadline
					remaining := time.Until(time.Now().Add(time.Duration(k.config.WaitTime) * time.Second))
					if remaining <= 0 {
						logger.Debugf("Wait time exhausted for topic %s, partition %d", topic, partition)
						saveOffset()
						return
					}

					fetchCtx, cancel := context.WithTimeout(ctx, remaining)
					msg, err := reader.ReadMessage(fetchCtx)
					cancel()

					// Handle message read errors
					if err != nil {
						if errors.Is(err, context.DeadlineExceeded) {
							logger.Debugf("No messages in topic %s, partition %d within remaining time", topic, partition)
							saveOffset()
							return
						}
						if errors.Is(err, kafka.ErrGenerationEnded) {
							logger.Infof("Generation ended for partition %d", partition)
							saveOffset()
							return
						}
						if errors.Is(err, context.Canceled) {
							logger.Infof("Context canceled for partition %d", partition)
							return
						}
						logger.Errorf("error reading message in incremental sync: %v", err)
						saveOffset()
						return
					}
					data := func() map[string]interface{} {
						var result map[string]interface{}
						if err := json.Unmarshal(msg.Value, &result); err != nil {
							logger.Errorf("Failed to unmarshal message value: %v", err)
							return nil
						}
						result["partition"] = msg.Partition
						result["offset"] = msg.Offset
						result["key"] = string(msg.Key)
						result["timestamp"] = msg.Time.UnixMilli()
						return result
					}()

					// Process message with provided function
					if err := processFn(data); err != nil {
						logger.Errorf("Failed to process message at offset %d: %v", msg.Offset, err)
						offsetsLock.Lock()
						lastProcessedOffsets[partition] = lastOffset
						offsetsLock.Unlock()
						return
					}
					lastOffset = msg.Offset
					processedAny = true
					logger.Debugf("Processed incremental message at offset %d for topic %s, partition %d", msg.Offset, topic, partition)
				}
			}
		}(partition, startOffset)
	}

	// Wait for all partition workers
	wg.Wait()

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
			return fmt.Errorf("no active consumer generation for commit")
		}

		topic := stream.Name()
		offsets, ok := k.offsetMap[topic]
		if !ok {
			return fmt.Errorf("no offsets found for topic %s", topic)
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
				return fmt.Errorf("failed to commit offsets to Kafka: %w", err)
			}
			logger.Infof("Offsets committed for topic %s", topic)
		} else {
			// Preserve existing state
			if cursor := k.state.GetCursor(stream.Self(), "partitions"); cursor != nil {
				k.state.SetCursor(stream.Self(), "partitions", cursor)
			}
			logger.Infof("No offsets to commit for topic %s", topic)
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
		return 0, fmt.Errorf("invalid auto_offset_reset value: %q", policy)
	}
}
