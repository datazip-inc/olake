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

	// get consumer group id from state
	if k.state != nil {
		if cursor := k.state.GetCursor(stream.Self(), "consumer_group_id"); cursor != nil {
			if gID, ok := cursor.(string); ok && gID != "" {
				k.consumerGroupID = gID
			}
		}
	}
	logger.Infof("[KAFKA] using consumer group id: %s for topic: %s", k.consumerGroupID, topic)

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

	concThreads := utils.Ternary(k.config.MaxThreads < len(partitionData), k.config.MaxThreads, len(partitionData)).(int)
	// Processing partitions concurrently
	err = utils.Concurrent(ctx, partitionData, concThreads, func(ctx context.Context, data struct {
		Partition   int
		StartOffset int64
	}, _ int) error {
		partition := data.Partition
		readerID := fmt.Sprintf("%s-partition-%d", topic, partition)
		// Partition specific-reader
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  strings.Split(k.config.BootstrapServers, ","),
			Topic:    topic,
			GroupID:  k.consumerGroupID,
			MinBytes: 1,
			MaxBytes: 10e6,
			Dialer:   k.dialer,
		})
		// save reader thread-safely
		k.mutex.Lock()
		k.readers[readerID] = reader
		k.mutex.Unlock()

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
				logger.Infof("[KAFKA] processed incremental message at offset %d for topic %s, partition %d", msg.Offset, topic, partition)

				// save last msg
				k.mutex.Lock()
				k.lastMessages[readerID] = msg
				k.mutex.Unlock()
			}
		}
	})
	return err
}

func (k *Kafka) PostIncremental(ctx context.Context, stream types.StreamInterface, noErr bool) error {
	if !noErr {
		return nil
	}
	k.mutex.Lock()
	// state save consumer group id
	if k.consumerGroupID != "" {
		k.state.SetCursor(stream.Self(), "consumer_group_id", k.consumerGroupID)
	}
	k.mutex.Unlock()

	readers, msgs := k.popAllReadersAndMessages()
	var readerErr []string
	for readerID, lastMsg := range msgs {
		r, ok := readers[readerID]
		if !ok || r == nil {
			logger.Warnf("[KAFKA] no reader found for topic %s while committing last offset %d", readerID, lastMsg.Offset)
			continue
		}
		if commitErr := r.CommitMessages(ctx, lastMsg); commitErr != nil {
			readerErr = append(readerErr, fmt.Sprintf("[KAFKA] failed to commit last message at offset %d for topic %s: %v", lastMsg.Offset, readerID, commitErr))
		}
	}
	for _, r := range readers {
		if r != nil {
			if closeErr := r.Close(); closeErr != nil {
				readerErr = append(readerErr, fmt.Sprintf("[KAFKA] failed to close reader: %v", closeErr))
			}
		}
	}
	if len(readerErr) > 0 {
		return fmt.Errorf("[KAFKA] post-incremental errors: %s", strings.Join(readerErr, "; "))
	}
	return nil
}

// to safely retrieve and clear readers and last messages
func (k *Kafka) popAllReadersAndMessages() (map[string]*kafka.Reader, map[string]kafka.Message) {
	k.mutex.Lock()
	defer k.mutex.Unlock()
	readers := k.readers
	msgs := k.lastMessages
	k.readers = make(map[string]*kafka.Reader)
	k.lastMessages = make(map[string]kafka.Message)
	return readers, msgs
}
