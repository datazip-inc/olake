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

func (k *Kafka) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
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

	// Set partitions for all streams
	for _, stream := range streams {
		if err := k.SetPartitions(ctx, stream); err != nil {
			return fmt.Errorf("failed to set partitions for stream %s: %w", stream.ID(), err)
		}
	}

	// Iterate over all stored partition metadata and create readers
	k.partitionMeta.Range(func(key, value any) bool {
		streamID, ok := key.(string)
		if !ok {
			return true // skip invalid key
		}
		partitions, ok := value.([]types.PartitionMetaData)
		if !ok {
			return true // skip invalid value
		}

		for idx, p := range partitions {
			readerID := fmt.Sprintf("%s_%s", streamID, utils.ULID())
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:  strings.Split(k.config.BootstrapServers, ","),
				Topic:    p.Stream.Name(),
				GroupID:  k.consumerGroupID,
				MinBytes: 1,    // can be taken from user
				MaxBytes: 10e6, // can be taken from user
				Dialer:   k.dialer,
			})
			// Save reader and update partition metadata
			k.readers.Store(readerID, reader)
			partitions[idx].ReaderID = readerID
		}
		return true
	})

	return nil
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, data types.PartitionMetaData, processFn abstract.CDCMsgFn) error {
	partitionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger.Infof("starting kafka streaming for topic %s (reader %s)", data.Stream.Name(), data.ReaderID)
	reader, ok := k.readers.Load(data.ReaderID)
	var readerInstance *kafka.Reader
	if ok {
		readerInstance, _ = reader.(*kafka.Reader)
	}

	for {
		msg, err := readerInstance.FetchMessage(partitionCtx)
		if err != nil {
			if errors.Is(err, kafka.ErrGenerationEnded) {
				// note: rebalance / generation end - do we need to keep reader alive or return nil?
				logger.Infof("generation ended for partition %d", data.PartitionID)
				return nil
			}
			if errors.Is(err, context.Canceled) {
				logger.Infof("context canceled for reader %s", data.ReaderID)
				return nil
			}
			return fmt.Errorf("error reading message in incremental sync: %v", err)
		}

		messageData := func() map[string]any {
			var result map[string]any
			if msg.Value == nil {
				logger.Warnf("received nil message value at offset %d for topic %s, partition %d", msg.Offset, data.Stream.Name(), msg.Partition)
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
		lastOffsetCheckPoint := msg.HighWaterMark - 1

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
		// when message reaches the last offset
		if msg.Offset == lastOffsetCheckPoint {
			return nil
		}
	}
}

func (k *Kafka) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if noErr {
		value, ok := k.lastMessages.Load(readerID)
		if !ok {
			// No last message, nothing to commit
			return nil
		}
		lastMsg, ok := value.(kafka.Message)
		if !ok {
			logger.Warnf("last message for %s is not kafka.Message", readerID)
			return nil
		}
		readerVal, ok := k.readers.Load(readerID)
		if !ok {
			logger.Warnf("reader not found for %s", readerID)
			return nil
		}
		r, ok := readerVal.(*kafka.Reader)
		if !ok || r == nil {
			logger.Warnf("invalid reader for %s", readerID)
			return nil
		}
		if commitErr := r.CommitMessages(ctx, lastMsg); commitErr != nil {
			logger.Errorf("failed to commit last message at offset %d for reader %s: %v", lastMsg.Offset, readerID, commitErr)
		}
		logger.Infof("committed message at offset %d for reader: %s", lastMsg.Offset, readerID)

		// Save consumer group id to state
		k.state.SetCursor(stream.Self(), "consumer_group_id", k.consumerGroupID)
	}
	return nil
}

func (k *Kafka) SetPartitions(ctx context.Context, stream types.StreamInterface) error {
	var allPartitions []types.PartitionMetaData
	// get topic metadata
	topic := stream.Name()
	topicDetail, err := k.GetTopicMetadata(ctx, topic)
	if err != nil {
		return err
	}

	// fetch first and last offset of the all partition
	offsetRequests := make([]kafka.OffsetRequest, 0, len(topicDetail.Partitions)*2)
	for _, p := range topicDetail.Partitions {
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{Partition: p.ID, Timestamp: kafka.FirstOffset})
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{Partition: p.ID, Timestamp: kafka.LastOffset})
	}
	offsetsResp, err := k.adminClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: offsetRequests}})
	if err != nil {
		return fmt.Errorf("failed to list offsets for topic %s: %w", topic, err)
	}

	// Fetch already committed offset of partition
	committedTopicOffsets := k.FetchCommittedOffsets(ctx, topic, topicDetail.Partitions)

	// Build partition metadata
	for _, idx := range offsetsResp.Topics[topic] {
		committedOffset, hasCommittedOffset := committedTopicOffsets[idx.Partition]

		// check if the partition has any messages at all, if not then skip
		if idx.FirstOffset >= idx.LastOffset {
			logger.Infof("skipping empty partition %d for topic %s (first: %d, last: %d)", idx.Partition, topic, idx.FirstOffset, idx.LastOffset)
			continue
		}

		// If a committed offset is available and there are no new messages, skip
		if hasCommittedOffset && committedOffset >= idx.LastOffset {
			logger.Infof("skipping partition %d for topic %s, no new messages (committed: %d, last: %d)", idx.Partition, topic, committedOffset, idx.LastOffset)
			continue
		}

		allPartitions = append(allPartitions, types.PartitionMetaData{
			Stream:      stream,
			PartitionID: idx.Partition,
			EndOffset:   idx.LastOffset,
		})
	}

	k.partitionMeta.Store(stream.ID(), allPartitions)
	return nil
}

func (k *Kafka) GetPartitions() map[string][]types.PartitionMetaData {
	partitionData := make(map[string][]types.PartitionMetaData)

	k.partitionMeta.Range(func(key, value any) bool {
		streamID, ok := key.(string)
		if !ok {
			return true // skip invalid keys
		}
		partitions, ok := value.([]types.PartitionMetaData)
		if !ok {
			return true // skip invalid values
		}

		partitionData[streamID] = partitions
		return true
	})
	return partitionData
}

func (k *Kafka) FetchCommittedOffsets(ctx context.Context, topic string, partitions []kafka.Partition) map[int]int64 {
	partitionsToFetch := make([]int, 0, len(partitions))
	for _, p := range partitions {
		partitionsToFetch = append(partitionsToFetch, p.ID)
	}

	fetchOffsetReq := &kafka.OffsetFetchRequest{
		GroupID: k.consumerGroupID,
		Topics:  map[string][]int{topic: partitionsToFetch},
	}

	committedOffsetsResp, err := k.adminClient.OffsetFetch(ctx, fetchOffsetReq)
	if err != nil {
		logger.Warnf("could not fetch committed offsets for group %s", k.consumerGroupID)
	}

	committedTopicOffsets := make(map[int]int64)
	if committedOffsetsResp != nil && committedOffsetsResp.Topics != nil {
		if offsets, ok := committedOffsetsResp.Topics[topic]; ok {
			for _, p := range offsets {
				committedTopicOffsets[p.Partition] = p.CommittedOffset
			}
		}
	}
	return committedTopicOffsets
}

func (k *Kafka) StreamChanges(_ context.Context, _ types.StreamInterface, _ abstract.CDCMsgFn) error {
	return nil
}
