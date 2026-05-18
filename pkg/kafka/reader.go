package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewReaderManager creates a new Kafka reader manager
func NewReaderManager(config ReaderConfig) *ReaderManager {
	return &ReaderManager{
		config:         config,
		readers:        make([]*kafkaReader, 0),
		partitionIndex: make(map[string]types.PartitionMetaData),
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

	// reader tasks = max threads if set to total partitions
	readersToCreate := utils.Ternary(r.ShouldMatchPartitionCount(), totalPartitions, utils.Ternary(r.config.MaxThreads > totalPartitions, totalPartitions, r.config.MaxThreads).(int)).(int)

	partitionKeySet := make(map[string]struct{}, len(r.partitionIndex))
	for k := range r.partitionIndex {
		partitionKeySet[k] = struct{}{}
	}
	r.olakeGroupBalancer = NewCustomGroupBalancer(readersToCreate, partitionKeySet)

	for readerIndex := range readersToCreate {
		readerID := fmt.Sprintf("group_%s", utils.ULID())
		clientID := fmt.Sprintf("olake-%s-%s", consumerGroupID, readerID)

		topics := make([]string, 0, len(streams))

		for _, stream := range streams {
			topics = append(topics, stream.Name())
		}

		readerOpts := append([]kgo.Opt{}, r.config.Dialer...)

		readerOpts = append(
			readerOpts,
			kgo.ConsumerGroup(consumerGroupID),
			kgo.ClientID(clientID),
			kgo.ConsumeTopics(topics...),
			kgo.Balancers(r.olakeGroupBalancer),
			kgo.FetchMinBytes(1),
			kgo.FetchMaxBytes(10e6),
			kgo.SessionTimeout(459*time.Second),
			kgo.RebalanceTimeout(60*time.Second),
			kgo.HeartbeatInterval(3*time.Second),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.InstanceID(readerID),
			kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
				logger.Infof("reader %s assigned partitions: %+v", clientID, assigned)
			}),

			kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
				logger.Infof("reader %s revoked partitions: %+v", clientID, revoked)
			}),

			kgo.OnPartitionsLost(func(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
				logger.Warnf("reader %s lost partitions: %+v", clientID, lost)
			}),
		)
		reader, err := kgo.NewClient(readerOpts...)
		if err != nil {
			return fmt.Errorf("failed to create reader %d: %w", readerIndex, err)
		}
		r.readers = append(r.readers, &kafkaReader{
			id:       readerID,
			clientID: clientID,
			reader:   reader,
		})
		logger.Infof("created reader %d with clientID %s", readerIndex, clientID)
	}
	logger.Infof("created %d readers for %d total partitions, with consumer grou	p %s", len(r.readers), totalPartitions, consumerGroupID)

	// warmup poll to trigger group join + partition assignment
	for _, r := range r.readers {
		warmupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		r.reader.PollFetches(warmupCtx)
		cancel()
	}
	return nil
}

// GetReaders returns the created readers
func (r *ReaderManager) GetReader(readerID int) *kgo.Client {
	return r.readers[readerID].reader
}

// GetReaders returns the created readers
func (r *ReaderManager) GetReaderCount() int {
	return len(r.readers)
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
func (r *ReaderManager) GetReaderIDAndClientID(readerIndex int) (string, string) {
	return r.readers[readerIndex].id, r.readers[readerIndex].clientID
}

// sets partitions that need to be synced for a stream
func (r *ReaderManager) SetPartitions(ctx context.Context, stream types.StreamInterface) error {
	topic := stream.Name()
	topicDetail, err := r.GetTopicMetadata(ctx, topic)
	if err != nil {
		return err
	}

	admin := kadm.NewClient(r.config.Client)

	// fetch first and last offset of the all partition
	startOffsets, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to list start offsets for topic %s: %s", topic, err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to list end offsets for topic %s: %s", topic, err)
	}

	// fetch already committed offset of partition
	committedTopicOffsets := r.FetchCommittedOffsets(ctx, topic, topicDetail.Partitions)

	// build partition metadata
	for _, partition := range topicDetail.Partitions {

		startOffset, exists := startOffsets.Lookup(topic, partition.Partition)
		if !exists {
			continue
		}

		endOffset, exists := endOffsets.Lookup(topic, partition.Partition)
		if !exists {
			continue
		}

		committedOffset, hasCommittedOffset := committedTopicOffsets[int(partition.Partition)]

		// check if the partition has any messages at all, if not then skip
		if startOffset.Offset >= endOffset.Offset {
			logger.Infof("skipping empty partition %d for topic %s (first: %d, last: %d)", partition.Partition, topic, startOffset.Offset, endOffset.Offset)
			continue
		}

		// if a committed offset is available and there are no new messages, skip
		if hasCommittedOffset && committedOffset >= endOffset.Offset {
			logger.Infof("skipping partition %d for topic %s, no new messages (committed: %d, last: %d)", partition.Partition, topic, committedOffset, endOffset.Offset)
			continue
		}

		r.partitionIndex[fmt.Sprintf("%s:%d", topic, partition.Partition)] = types.PartitionMetaData{
			Stream:      stream,
			PartitionID: int(partition.Partition),
			EndOffset:   endOffset.Offset,
		}
	}

	return nil
}

// GetTopicMetadata fetches metadata for a topic
func (r *ReaderManager) GetTopicMetadata(ctx context.Context, topic string) (*kadm.TopicDetail, error) {
	admin := kadm.NewClient(r.config.Client)
	metadata, err := admin.ListTopics(ctx, topic)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata for topic %s: %s", topic, err)
	}

	topicDetail, exists := metadata[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found in metadata", topic)
	}
	return &topicDetail, nil
}

// FetchCommittedOffsets fetches committed offsets for a topic
func (r *ReaderManager) FetchCommittedOffsets(ctx context.Context, topic string, partitions map[int32]kadm.PartitionDetail) map[int]int64 {
	partitionsToFetch := make([]int32, 0, len(partitions))
	for _, p := range partitions {
		partitionsToFetch = append(partitionsToFetch, int32(p.Partition))
	}

	admin := kadm.NewClient(r.config.Client)
	offsets, err := admin.FetchOffsets(ctx, r.config.ConsumerGroupID)
	if err != nil {
		logger.Warnf("could not fetch committed offsets for group %s", r.config.ConsumerGroupID)
		return map[int]int64{}
	}

	committedTopicOffsets := make(map[int]int64)
	for _, partition := range partitionsToFetch {

		offset, exists := offsets.Lookup(topic, partition)
		if !exists {
			continue
		}

		committedTopicOffsets[int(partition)] = offset.At
	}
	return committedTopicOffsets
}

func (r *ReaderManager) Close() error {
	for _, kafkaReader := range r.readers {
		kafkaReader.reader.Close()
	}

	for kStr := range r.partitionIndex {
		delete(r.partitionIndex, kStr)
	}

	return nil
}

// RestartReader closes and recreates the reader using the same readerID and clientID.
func (r *ReaderManager) RestartReader(ctx context.Context, readerIndex int, streams []types.StreamInterface, consumerGroupID string) (*kgo.Client, error) {
	currentReader := r.GetReader(readerIndex)
	if currentReader == nil {
		return nil, fmt.Errorf("reader not found for readerIndex %d", readerIndex)
	}

	readerID, clientID := r.GetReaderIDAndClientID(readerIndex)

	logger.Infof("restarting reader %d with clientID %s", readerIndex, clientID)

	currentReader.Close()

	reader, err := r.CreateReader(streams, consumerGroupID, readerID, clientID)
	if err != nil {
		// Slot must not keep a closed *kgo.Client; next Poll/Commit would be undefined.
		r.readers[readerIndex].reader = nil
		return nil, fmt.Errorf("%w: failed to recreate kafka reader %d after close: %v", constants.ErrNonRetryable, readerIndex, err)
	}

	r.readers[readerIndex].reader = reader

	logger.Infof("successfully restarted reader %d with clientID %s", readerIndex, clientID)

	return reader, nil
}

// CreateReader creates a single kafka reader client.
func (r *ReaderManager) CreateReader(streams []types.StreamInterface, consumerGroupID string, readerID string, clientID string) (*kgo.Client, error) {

	topics := make([]string, 0, len(streams))

	for _, stream := range streams {
		topics = append(topics, stream.Name())
	}

	readerOpts := append([]kgo.Opt{}, r.config.Dialer...)

	readerOpts = append(
		readerOpts,
		kgo.ConsumerGroup(consumerGroupID),
		kgo.ClientID(clientID),
		kgo.InstanceID(readerID),
		kgo.ConsumeTopics(topics...),
		kgo.Balancers(r.olakeGroupBalancer),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(10e6),
		kgo.SessionTimeout(459*time.Second),
		kgo.RebalanceTimeout(60*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
			logger.Infof("reader %s assigned partitions: %+v", clientID, assigned)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			logger.Infof("reader %s revoked partitions: %+v", clientID, revoked)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, cl *kgo.Client, lost map[string][]int32) {
			logger.Warnf("reader %s lost partitions: %+v", clientID, lost)
		}),
	)

	reader, err := kgo.NewClient(readerOpts...)
	if err != nil {
		return nil, err
	}

	return reader, nil
}
