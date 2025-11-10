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
func (r *ReaderManager) CreateReaders(ctx context.Context, streams []types.StreamInterface, consumerGroupID string) error {
	r.partitionIndex = make(map[string]types.PartitionMetaData)
	for _, stream := range streams {
		if err := r.SetPartitions(ctx, stream); err != nil {
			return fmt.Errorf("failed to set partitions for stream %s: %s", stream.ID(), err)
		}
	}

	// total partitions with new messages
	totalPartitions := len(r.partitionIndex)
	if totalPartitions == 0 {
		logger.Infof("no partitions with new messages; skipping reader creation for group %s", consumerGroupID)
		return nil
	}

	r.readers = make(map[string]*kafka.Reader)
	r.readerClientIDs = make(map[string]string)

	// reader tasks according to concurrency policy
	readersToCreate := utils.Ternary(r.ShouldMatchPartitionCount(), totalPartitions, utils.Ternary(r.config.MaxThreads > totalPartitions, totalPartitions, r.config.MaxThreads).(int)).(int)

	for readerIndex := range readersToCreate {
		readerID := fmt.Sprintf("group_%s", utils.ULID())
		clientID := fmt.Sprintf("olake-%s-%s", consumerGroupID, readerID)

		// create a per-reader dialer with a unique clientID to identify assignments
		dialerCopy := *r.config.Dialer
		dialerCopy.ClientID = clientID

		// custom round robin group balancer that ensures proper consumer ID distribution
		groupBalancer := &CustomGroupBalancer{
			requiredConsumerIDs: readersToCreate,
			readerIndex:         readerIndex,
		}

		// readers creation
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: utils.SplitAndTrim(r.config.BootstrapServers),
			GroupID: consumerGroupID,
			GroupTopics: func() []string {
				topics := make([]string, 0, len(streams))
				for _, s := range streams {
					topics = append(topics, s.Name())
				}
				return topics
			}(),
			MinBytes:       1,    // 1 byte
			MaxBytes:       10e6, // 10 MB
			GroupBalancers: []kafka.GroupBalancer{groupBalancer},
			Dialer:         &dialerCopy,
		})
		r.readers[readerID] = reader
		r.readerClientIDs[readerID] = clientID
	}
	logger.Infof("created %d readers for %d total partitions, with consumer group %s", len(r.readers), totalPartitions, consumerGroupID)
	return nil
}

// GetReaders returns the created readers
func (r *ReaderManager) GetReader(readerID string) *kafka.Reader {
	return r.readers[readerID]
}

// GetPartitionIndex returns the partition index
func (r *ReaderManager) GetPartitionIndex(partitionKey string) (types.PartitionMetaData, bool) {
	partitionMeta, exists := r.partitionIndex[partitionKey]
	return partitionMeta, exists
}

// ShouldMatchPartitionCount returns whether readers should match partition count
func (r *ReaderManager) ShouldMatchPartitionCount() bool {
	return r.config.ThreadsEqualTotalPartitions
}

// GetReaderClientIDs returns the reader client IDs
func (r *ReaderManager) GetReaderClientID(readerID string) (string, bool) {
	clientID, exists := r.readerClientIDs[readerID]
	return clientID, exists
}

// return reader ids
func (r *ReaderManager) GetReaderIDs() []string {
	ids := make([]string, 0, len(r.readers))
	for readerID := range r.readers {
		ids = append(ids, readerID)
	}
	return ids
}

// sets partitions that need to be synced for a stream
func (r *ReaderManager) SetPartitions(ctx context.Context, stream types.StreamInterface) error {
	topic := stream.Name()
	topicDetail, err := r.GetTopicMetadata(ctx, topic)
	if err != nil {
		return err
	}

	// fetch first and last offset of the all partition
	offsetRequests := make([]kafka.OffsetRequest, 0, len(topicDetail.Partitions)*2)
	for _, p := range topicDetail.Partitions {
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{Partition: p.ID, Timestamp: kafka.FirstOffset})
		offsetRequests = append(offsetRequests, kafka.OffsetRequest{Partition: p.ID, Timestamp: kafka.LastOffset})
	}

	offsetsResp, err := r.config.AdminClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: offsetRequests}})
	if err != nil {
		return fmt.Errorf("failed to list offsets for topic %s: %s", topic, err)
	}

	// fetch already committed offset of partition
	committedTopicOffsets := r.FetchCommittedOffsets(ctx, topic, topicDetail.Partitions)

	// build partition metadata
	for _, idx := range offsetsResp.Topics[topic] {
		committedOffset, hasCommittedOffset := committedTopicOffsets[idx.Partition]

		// check if the partition has any messages at all, if not then skip
		if idx.FirstOffset >= idx.LastOffset {
			logger.Infof("skipping empty partition %d for topic %s (first: %d, last: %d)", idx.Partition, topic, idx.FirstOffset, idx.LastOffset)
			continue
		}

		// if a committed offset is available and there are no new messages, skip
		if hasCommittedOffset && committedOffset >= idx.LastOffset {
			logger.Infof("skipping partition %d for topic %s, no new messages (committed: %d, last: %d)", idx.Partition, topic, committedOffset, idx.LastOffset)
			continue
		}

		r.partitionIndex[fmt.Sprintf("%s:%d", topic, idx.Partition)] = types.PartitionMetaData{
			Stream:      stream,
			PartitionID: idx.Partition,
			EndOffset:   idx.LastOffset,
		}
	}
	return nil
}

// GetTopicMetadata fetches metadata for a topic
func (r *ReaderManager) GetTopicMetadata(ctx context.Context, topic string) (*kafka.Topic, error) {
	metadataReq := &kafka.MetadataRequest{Topics: []string{topic}}
	metadataResp, err := r.config.AdminClient.Metadata(ctx, metadataReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata for topic %s: %s", topic, err)
	}

	for _, t := range metadataResp.Topics {
		if t.Name == topic {
			if t.Error != nil {
				return nil, fmt.Errorf("topic %s not found in metadata: %s", topic, t.Error)
			}
			return &t, nil
		}
	}

	return nil, fmt.Errorf("topic %s not found in metadata", topic)
}

// FetchCommittedOffsets fetches committed offsets for a topic
func (r *ReaderManager) FetchCommittedOffsets(ctx context.Context, topic string, partitions []kafka.Partition) map[int]int64 {
	partitionsToFetch := make([]int, 0, len(partitions))
	for _, p := range partitions {
		partitionsToFetch = append(partitionsToFetch, p.ID)
	}

	fetchOffsetReq := &kafka.OffsetFetchRequest{
		GroupID: r.config.ConsumerGroupID,
		Topics:  map[string][]int{topic: partitionsToFetch},
	}

	committedOffsetsResp, err := r.config.AdminClient.OffsetFetch(ctx, fetchOffsetReq)
	if err != nil {
		logger.Warnf("could not fetch committed offsets for group %s", r.config.ConsumerGroupID)
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

func (r *ReaderManager) Close() error {
	for _, reader := range r.readers {
		if err := reader.Close(); err != nil {
			return fmt.Errorf("failed to close reader: %s", err)
		}
	}

	for kStr := range r.partitionIndex {
		delete(r.partitionIndex, kStr)
	}

	return nil
}
