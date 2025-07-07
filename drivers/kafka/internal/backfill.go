package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
)

// KafkaOffset defines the offset information for a Kafka topic partition.
type KafkaOffset struct {
	Topic     string
	Partition int
	Offset    int64
}

func (k *Kafka) GetOrSplitChunks(ctx context.Context, _ *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	if stream.GetSyncMode() != types.FULLREFRESH {
		return nil, fmt.Errorf("GetOrSplitChunks is only for full_refresh mode")
	}

	topic := stream.Name()
	// Fetch topic metadata
	metadataReq := &kafka.MetadataRequest{
		Topics: []string{topic},
	}
	metadataResp, err := k.adminClient.Metadata(ctx, metadataReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch kafka topic metadata: %v", err)
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
	partitions := make([]int, 0, len(topicDetail.Partitions))
	for _, p := range topicDetail.Partitions {
		partitions = append(partitions, p.ID)
	}

	// Fetch start and end offsets for each partition
	startOffsets := make(map[int]int64)
	endOffsets := make(map[int]int64)

	// Build offset requests
	earliestReqs := make([]kafka.OffsetRequest, len(partitions))
	latestReqs := make([]kafka.OffsetRequest, len(partitions))
	for i, partition := range partitions {
		earliestReqs[i] = kafka.OffsetRequest{
			Partition: partition,
			Timestamp: kafka.FirstOffset,
		}
		latestReqs[i] = kafka.OffsetRequest{
			Partition: partition,
			Timestamp: kafka.LastOffset,
		}
	}

	// Get earliest offsets
	earliestResp, err := k.adminClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: earliestReqs,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get start offsets for topic %s: %v", topic, err)
	}

	// Get latest offsets
	latestResp, err := k.adminClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: latestReqs,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get end offsets for topic %s: %v", topic, err)
	}

	// Process earliest offsets
	for _, p := range earliestResp.Topics[topic] {
		if p.Error != nil {
			return nil, fmt.Errorf("error in partition %d: %v", p.Partition, p.Error)
		}
		startOffsets[p.Partition] = p.FirstOffset
	}

	// Process latest offsets
	for _, p := range latestResp.Topics[topic] {
		if p.Error != nil {
			return nil, fmt.Errorf("error in partition %d: %v", p.Partition, p.Error)
		}
		endOffsets[p.Partition] = p.LastOffset
	}

	chunks := types.NewSet[types.Chunk]()
	for _, partition := range partitions {
		earliest := startOffsets[partition]
		latest := endOffsets[partition]

		if earliest < 0 || latest < 0 || earliest > latest {
			logger.Warnf("No messages for topic %s, partition %d: earliest=%d, latest=%d", topic, partition, earliest, latest)
			continue
		}

		chunks.Insert(
			types.Chunk{
				Min: KafkaOffset{
					Topic:     topic,
					Partition: partition,
					Offset:    earliest,
				},
				Max: KafkaOffset{
					Topic:     topic,
					Partition: partition,
					Offset:    latest - 1, // Latest is the next offset to write, so max is latest-1
				},
			})
		logger.Infof("Created chunk for topic %s, partition %d: min_offset=%d, max_offset=%d", topic, partition, earliest, latest-1)
	}
	return chunks, nil
}

func (k *Kafka) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	if stream.GetSyncMode() != types.FULLREFRESH {
		return fmt.Errorf("ChunkIterator is only for full_refresh mode")
	}

	minOffset := chunk.Min.(KafkaOffset)
	maxOffset := chunk.Max.(KafkaOffset)
	topic := minOffset.Topic
	partition := minOffset.Partition
	minOffsetValue := minOffset.Offset
	maxOffsetValue := maxOffset.Offset

	logger.Infof("Starting Chunk processing for topic %s, partition %d, min_offset=%d, max_offset=%d", topic, partition, minOffsetValue, maxOffsetValue)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   strings.Split(k.config.BootstrapServers, ","),
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10e6,
		Dialer:    k.dialer,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			logger.Warnf("Error closing Kafka reader for topic %s, partition %d: %v", topic, partition, err)
		}
	}()

	if err := reader.SetOffset(minOffsetValue); err != nil {
		return fmt.Errorf("failed to set offset %d for topic %s, partition %d: %v", minOffsetValue, topic, partition, err)
	}

	if minOffsetValue > maxOffsetValue {
		logger.Warnf("No messages available for topic %s, partition %d: min_offset=%d, max_offset=%d", topic, partition, minOffsetValue, maxOffsetValue)
		return nil
	}

	// Track the last processed offset
	var lastProcessed int64 = -1
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := reader.ReadMessage(fetchCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// If we've already processed up to max, we're done
				if lastProcessed >= maxOffsetValue {
					logger.Infof("Processed all available messages up to offset %d for topic %s, partition %d", maxOffsetValue, topic, partition)
					return nil
				}
				logger.Warnf("No messages received within timeout for topic %s, partition %d; assuming end", topic, partition)
				break
			}
			if err == context.Canceled {
				return nil
			}
			logger.Warnf("No more available messages to fetch for topic %s, partition %d: %v; attempting to continue", topic, partition, err)
			return nil
		}

		if msg.Offset > maxOffsetValue {
			logger.Infof("Reached offset %d beyond max %d for topic %s, partition %d", msg.Offset, maxOffsetValue, topic, partition)
			break
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
			logger.Errorf("failed to process message at offset %d: %v", msg.Offset, err)
			return err
		}
		logger.Infof("Processed message at offset %d for topic %s, partition %d", msg.Offset, topic, partition)

		// Stop after processing the last message in the chunk
		if lastProcessed >= maxOffsetValue {
			logger.Infof("Completed processing up to max offset %d for topic %s, partition %d", maxOffsetValue, topic, partition)
			return nil
		}
	}

	return nil
}
