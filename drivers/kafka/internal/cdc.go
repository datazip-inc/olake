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

func (k *Kafka) PreCDC(_ context.Context, streams []types.StreamInterface) error {
	if len(streams) == 0 {
		return fmt.Errorf("no valid streams found for CDC")
	}

	// Generate a new consumer group ID if not configured
	var groupID string
	if cursor := k.state.GetCursor(streams[0].Self(), "consumer_group_id"); cursor != nil {
		if gID, ok := cursor.(string); ok && gID != "" {
			groupID = gID
		}
	}
	groupID = utils.Ternary(groupID == "", utils.Ternary(k.config.ConsumerGroupID != "", k.config.ConsumerGroupID, fmt.Sprintf("olake-consumer-group-%d", time.Now().Unix())), groupID).(string)
	k.consumerGroupID = groupID
	logger.Infof("using consumer group id: %s", k.consumerGroupID)
	return nil
}

func (k *Kafka) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	// topic := stream.Name()
	// // Fetch topic metadata to get partitions
	// metadataReq := &kafka.MetadataRequest{
	// 	Topics: []string{topic},
	// }
	// metadataResp, err := k.adminClient.Metadata(ctx, metadataReq)
	// if err != nil {
	// 	return fmt.Errorf("failed to fetch topic metadata: %v", err)
	// }

	// var topicDetail kafka.Topic
	// for _, t := range metadataResp.Topics {
	// 	if t.Name == topic {
	// 		topicDetail = t
	// 		break
	// 	}
	// }
	// if topicDetail.Error != nil {
	// 	return fmt.Errorf("topic %s not found in metadata: %v", topic, topicDetail.Error)
	// }

	// partitionData := make([]struct {
	// 	Partition   int
	// 	StartOffset int64
	// }, 0, len(topicDetail.Partitions))

	// // Building partition data first
	// for _, partition := range topicDetail.Partitions {
	// 	partitionID := partition.ID
	// 	startOffset := utils.Ternary(k.config.AutoOffsetReset == "latest", kafka.LastOffset, kafka.FirstOffset).(int64)
	// 	partitionData = append(partitionData, struct {
	// 		Partition   int
	// 		StartOffset int64
	// 	}{
	// 		Partition:   partitionID,
	// 		StartOffset: startOffset,
	// 	})
	// }

	// // Create a new context for this stream to ensure proper cancellation
	// streamCtx, cancel := context.WithCancel(ctx)
	// defer cancel()

	// utils.ConcurrentInGroup(connGroup, partitionData, func(ctx context.Context, data struct {
	// 	Partition   int
	// 	StartOffset int64
	// }) error {
	// 	partition := data.Partition
	// 	readerID := fmt.Sprintf("%s_%d_%s", stream.ID(), partition, utils.ULID())

	// 	reader := kafka.NewReader(kafka.ReaderConfig{
	// 		Brokers:  strings.Split(k.config.BootstrapServers, ","),
	// 		Topic:    topic,
	// 		GroupID:  k.consumerGroupID, // participate in same consumer group
	// 		MinBytes: 1,
	// 		MaxBytes: 10e6,
	// 		Dialer:   k.dialer,
	// 	})

	// 	// save reader
	// 	k.readers.Store(readerID, reader)
	// 	logger.Infof("starting kafka streaming for topic %s, partition %d (reader %s)", topic, partition, readerID)

	// 	partitionIdleWait := time.Duration(k.config.WaitTime) * time.Second

	// 	for {
	// 		select {
	// 		case <-streamCtx.Done():
	// 			logger.Infof("kafka streaming stopped for topic %s, partition %d due to context cancellation", topic, partition)
	// 			return streamCtx.Err()
	// 		default:
	// 			deadline := time.Now().Add(partitionIdleWait)
	// 			fetchCtx, cancel := context.WithDeadline(streamCtx, deadline)
	// 			msg, err := reader.FetchMessage(fetchCtx)
	// 			cancel()

	// 			if err != nil {
	// 				if errors.Is(err, kafka.ErrGenerationEnded) {
	// 					// rebalance / generation end - keep reader alive and let membership reassign
	// 					logger.Infof("generation ended (rebalance) for partition %d, continuing", partition)
	// 					time.Sleep(100 * time.Millisecond)
	// 					continue
	// 				}
	// 				if errors.Is(err, context.DeadlineExceeded) {
	// 					logger.Infof("no messages in topic %s, partition %d within wait time", topic, partition)
	// 					return nil
	// 				}
	// 				if errors.Is(err, context.Canceled) {
	// 					logger.Infof("context canceled for partition %d", partition)
	// 					return nil
	// 				}
	// 				return fmt.Errorf("error reading message in incremental sync: %v", err)
	// 			}

	// 			data := func() map[string]any {
	// 				var result map[string]any
	// 				if msg.Value == nil {
	// 					logger.Warnf("received nil message value at offset %d for topic %s, partition %d", msg.Offset, topic, partition)
	// 					return nil
	// 				}
	// 				if err := json.Unmarshal(msg.Value, &result); err != nil {
	// 					logger.Errorf("failed to unmarshal message value: %v", err)
	// 					return nil
	// 				}
	// 				result["partition"] = msg.Partition
	// 				result["offset"] = msg.Offset
	// 				result["key"] = string(msg.Key)
	// 				result["kafka_timestamp"] = msg.Time.UnixMilli()
	// 				return result
	// 			}()

	// 			if data == nil {
	// 				// we skip storing nil-valued messages
	// 				continue
	// 			}

	// 			if procErr := processFn(ctx, abstract.CDCChange{
	// 				Stream:    stream,
	// 				Timestamp: msg.Time,
	// 				Kind:      "create",
	// 				Data:      data,
	// 			}); procErr != nil {
	// 				return procErr
	// 			}

	// 			// save last processed message
	// 			k.lastMessages.Store(readerID, msg)
	// 			logger.Infof("processed message at offset %d for topic %s, partition %d", msg.Offset, topic, partition)
	// 		}
	// 	}
	// })

	// if err := connGroup.Block(); err != nil {
	// 	return err
	// }
	return nil
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, data abstract.PartitionMetaData, processFn abstract.CDCMsgFn) error {
	partitionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	topic := data.Stream.Name()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(k.config.BootstrapServers, ","),
		Topic:    topic,
		GroupID:  k.consumerGroupID, // participate in same consumer group
		MinBytes: 1,
		MaxBytes: 10e6,
		Dialer:   k.dialer,
	})

	// save reader
	k.readers.Store(data.ReaderID, reader)
	logger.Infof("starting kafka streaming for topic %s, partition %d (reader %s)", topic, data.PartitionID, data.ReaderID)

	partitionIdleWait := time.Duration(k.config.WaitTime) * time.Second
	for {
		select {
		case <-ctx.Done():
			logger.Infof("kafka streaming stopped for topic %s, partition %d due to context cancellation", topic, data.PartitionID)
			return ctx.Err()
		default:
			deadline := time.Now().Add(partitionIdleWait)
			fetchCtx, cancel := context.WithDeadline(partitionCtx, deadline)
			msg, err := reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, kafka.ErrGenerationEnded) {
					// rebalance / generation end - keep reader alive and let membership reassign
					logger.Infof("generation ended (rebalance) for partition %d, continuing", data.PartitionID)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if errors.Is(err, context.DeadlineExceeded) {
					logger.Infof("no messages in topic %s, partition %d within wait time", topic, data.PartitionID)
					return nil
				}
				if errors.Is(err, context.Canceled) {
					logger.Infof("context canceled for partition %d", data.PartitionID)
					return nil
				}
				return fmt.Errorf("error reading message in incremental sync: %v", err)
			}

			messageData := func() map[string]any {
				var result map[string]any
				if msg.Value == nil {
					logger.Warnf("received nil message value at offset %d for topic %s, partition %d", msg.Offset, topic, data.PartitionID)
					return nil
				}
				if err := json.Unmarshal(msg.Value, &result); err != nil {
					logger.Errorf("failed to unmarshal message value: %v", err)
					return nil
				}
				result["partition"] = msg.Partition
				result["offset"] = msg.Offset
				result["key"] = string(msg.Key)
				result["kafka_timestamp"] = msg.Time.UnixMilli()
				return result
			}()

			if messageData == nil {
				// we skip storing nil-valued messages
				continue
			}

			if procErr := processFn(ctx, abstract.CDCChange{
				Stream:    data.Stream,
				Timestamp: msg.Time,
				Kind:      "create",
				Data:      messageData,
			}); procErr != nil {
				return procErr
			}

			// save last processed message
			k.lastMessages.Store(data.ReaderID, msg)
			logger.Infof("processed message at offset %d for topic %s, partition %d", msg.Offset, topic, data.PartitionID)
		}
	}
}

func (k *Kafka) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if noErr {
		var readerErr []string
		lastMsgVal, okMsg := k.lastMessages.LoadAndDelete(readerID)
		if okMsg {
			lastMsg, ok := lastMsgVal.(kafka.Message)
			if !ok {
				logger.Warnf("last message for %s is not kafka.Message", readerID)
			}
			if readerVal, okReader := k.readers.Load(readerID); okReader {
				if r, ok := readerVal.(*kafka.Reader); ok && r != nil {
					if commitErr := r.CommitMessages(ctx, lastMsg); commitErr != nil {
						readerErr = append(readerErr, fmt.Sprintf("failed to commit last message at offset %d for reader %s: %v", lastMsg.Offset, readerID, commitErr))
					}
				}
			}
		}

		if readerVal, okReader := k.readers.Load(readerID); okReader {
			if r, ok := readerVal.(*kafka.Reader); ok && r != nil {
				if closeErr := r.Close(); closeErr != nil {
					readerErr = append(readerErr, fmt.Sprintf("failed to close reader %s: %v", readerID, closeErr))
				} else {
					logger.Debugf("closed reader %s", readerID)
				}
			}
			k.readers.Delete(readerID)
		}

		if len(readerErr) > 0 {
			return fmt.Errorf("post-streaming kafka errors: %s", strings.Join(readerErr, "; "))
		}

		// save consumer group id to state
		k.mutex.Lock()
		if k.consumerGroupID != "" {
			k.state.SetCursor(stream.Self(), "consumer_group_id", k.consumerGroupID)
		}
		k.mutex.Unlock()
	}
	return nil
}

func (k *Kafka) GetPartitions(ctx context.Context, streams []types.StreamInterface) ([]abstract.PartitionMetaData, error) {
	var allPartitions []abstract.PartitionMetaData
	for _, stream := range streams {
		topic := stream.Name()

		// Fetch topic metadata
		metadataReq := &kafka.MetadataRequest{Topics: []string{topic}}
		metadataResp, err := k.adminClient.Metadata(ctx, metadataReq)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch topic metadata: %v", err)
		}

		var topicDetail kafka.Topic
		for _, t := range metadataResp.Topics {
			if t.Name == topic {
				topicDetail = t
				break
			}
		}
		if topicDetail.Error != nil {
			return nil, fmt.Errorf("topic %s not found in metadata: %v", topic, topicDetail.Error)
		}

		// Build partition metadata
		for _, partition := range topicDetail.Partitions {
			partitionID := partition.ID
			startOffset := utils.Ternary(
				k.config.AutoOffsetReset == "latest",
				kafka.LastOffset,
				kafka.FirstOffset,
			).(int64)

			readerID := fmt.Sprintf("%s_%d_%s", stream.ID(), partitionID, utils.ULID())

			allPartitions = append(allPartitions, abstract.PartitionMetaData{
				ReaderID:    readerID,
				Stream:      stream,
				PartitionID: partitionID,
				StartOffset: startOffset,
			})
		}
	}

	return allPartitions, nil
}
