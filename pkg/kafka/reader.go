package kafka

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
)

// NewReaderManager creates a new Kafka reader manager
func NewReaderManager(config ReaderConfig) *ReaderManager {
	return &ReaderManager{
		config:          config,
		partitionIndex:  make(map[string]types.PartitionMetaData),
		readers:         make(map[string]*kafka.Reader),
		readerClientIDs: make(map[string]string),
	}
}

// CreateReaders creates Kafka readers based on the provided streams and configuration
func (krm *ReaderManager) CreateReaders(ctx context.Context, streams []types.StreamInterface, consumerGroupID string) error {
	krm.partitionIndex = make(map[string]types.PartitionMetaData)
	for _, stream := range streams {
		if err := krm.SetPartitions(ctx, stream); err != nil {
			return fmt.Errorf("failed to set partitions for stream %s: %w", stream.ID(), err)
		}
	}

	// total partitions with new messages
	totalPartitions := len(krm.partitionIndex)
	if totalPartitions == 0 {
		logger.Infof("no partitions with new messages; skipping reader creation for group %s", consumerGroupID)
		return nil
	}

	krm.readers = make(map[string]*kafka.Reader)
	krm.readerClientIDs = make(map[string]string)

	// reader tasks according to concurrency policy
	readersToCreate := utils.Ternary(krm.ShouldMatchPartitionCount(), totalPartitions, utils.Ternary(krm.config.MaxThreads >= totalPartitions, totalPartitions, krm.config.MaxThreads).(int)).(int)

	for i := 0; i < readersToCreate; i++ {
		readerID := fmt.Sprintf("grp_%s", utils.ULID())
		clientID := fmt.Sprintf("olake-%s-%s", consumerGroupID, readerID)

		// create a per-reader dialer with a unique clientID to identify assignments
		dialerCopy := *krm.config.Dialer
		dialerCopy.ClientID = clientID

		// custom round robin group balancer that ensures proper consumer ID distribution
		groupBalancer := &CustomGroupBalancer{
			requiredConsumerIDs: readersToCreate,
			readerIndex:         i,
		}

		// readers creation
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: utils.SplitAndTrim(krm.config.BootstrapServers),
			GroupID: consumerGroupID,
			GroupTopics: func() []string {
				topics := make([]string, 0, len(streams))
				for _, s := range streams {
					topics = append(topics, s.Name())
				}
				return topics
			}(),
			MinBytes:       1,
			MaxBytes:       10e6,
			GroupBalancers: []kafka.GroupBalancer{groupBalancer},
			Dialer:         &dialerCopy,
		})
		krm.readers[readerID] = reader
		krm.readerLastMessages.Store(readerID, make(map[types.PartitionKey]kafka.Message))
		krm.readerClientIDs[readerID] = clientID
	}
	logger.Infof("created %d readers for %d total partitions, with consumer group %s", len(krm.readers), totalPartitions, consumerGroupID)
	return nil
}

// GetReaders returns the created readers
func (krm *ReaderManager) GetReaders() map[string]*kafka.Reader {
	return krm.readers
}

// GetPartitionIndex returns the partition index
func (krm *ReaderManager) GetPartitionIndex() map[string]types.PartitionMetaData {
	return krm.partitionIndex
}

// ShouldMatchPartitionCount returns whether readers should match partition count
func (krm *ReaderManager) ShouldMatchPartitionCount() bool {
	return krm.config.ThreadsEqualTotalPartitions
}

// GetReaderLastMessages returns the reader last messages
func (krm *ReaderManager) GetReaderLastMessages() map[string]map[types.PartitionKey]kafka.Message {
	result := make(map[string]map[types.PartitionKey]kafka.Message)
	krm.readerLastMessages.Range(func(key, value interface{}) bool {
		if readerID, ok := key.(string); ok {
			if messages, ok := value.(map[types.PartitionKey]kafka.Message); ok {
				result[readerID] = messages
			}
		}
		return true
	})
	return result
}

// GetReaderClientIDs returns the reader client IDs
func (krm *ReaderManager) GetReaderClientIDs() map[string]string {
	return krm.readerClientIDs
}

// SetPartitions sets up partitions for a stream
func (krm *ReaderManager) SetPartitions(ctx context.Context, stream types.StreamInterface) error {
	topic := stream.Name()
	topicDetail, err := krm.GetTopicMetadata(ctx, topic)
	if err != nil {
		return err
	}

	// fetch first and last offset of the all partition
	offsetRequests := make([]kafka.OffsetRequest, 0, len(topicDetail.Partitions)*2)
	for _, p := range topicDetail.Partitions {
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{Partition: p.ID, Timestamp: kafka.FirstOffset})
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{Partition: p.ID, Timestamp: kafka.LastOffset})
	}
	offsetsResp, err := krm.config.AdminClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: offsetRequests}})
	if err != nil {
		return fmt.Errorf("failed to list offsets for topic %s: %w", topic, err)
	}

	// Fetch already committed offset of partition
	committedTopicOffsets := krm.FetchCommittedOffsets(ctx, topic, topicDetail.Partitions)

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

		// decide start offset (same as committed if any)
		startOffset := committedOffset
		if !hasCommittedOffset {
			startOffset = idx.LastOffset
		}

		pm := types.PartitionMetaData{
			Stream:      stream,
			PartitionID: idx.Partition,
			EndOffset:   idx.LastOffset,
			StartOffset: startOffset,
		}

		// update topic's partition index
		if krm.partitionIndex == nil {
			krm.partitionIndex = make(map[string]types.PartitionMetaData)
		}
		krm.partitionIndex[fmt.Sprintf("%s:%d", topic, pm.PartitionID)] = pm
	}
	return nil
}

// GetTopicMetadata fetches metadata for a topic
func (krm *ReaderManager) GetTopicMetadata(ctx context.Context, topic string) (*kafka.Topic, error) {
	metadataReq := &kafka.MetadataRequest{Topics: []string{topic}}
	metadataResp, err := krm.config.AdminClient.Metadata(ctx, metadataReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata for topic %s: %w", topic, err)
	}

	for _, t := range metadataResp.Topics {
		if t.Name == topic {
			if t.Error != nil {
				return nil, fmt.Errorf("topic %s not found in metadata: %v", topic, t.Error)
			}
			return &t, nil
		}
	}

	return nil, fmt.Errorf("topic %s not found in metadata", topic)
}

// FetchCommittedOffsets fetches committed offsets for a topic
func (krm *ReaderManager) FetchCommittedOffsets(ctx context.Context, topic string, partitions []kafka.Partition) map[int]int64 {
	partitionsToFetch := make([]int, 0, len(partitions))
	for _, p := range partitions {
		partitionsToFetch = append(partitionsToFetch, p.ID)
	}

	fetchOffsetReq := &kafka.OffsetFetchRequest{
		GroupID: krm.config.ConsumerGroupID,
		Topics:  map[string][]int{topic: partitionsToFetch},
	}

	committedOffsetsResp, err := krm.config.AdminClient.OffsetFetch(ctx, fetchOffsetReq)
	if err != nil {
		logger.Warnf("could not fetch committed offsets for group %s", krm.config.ConsumerGroupID)
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
