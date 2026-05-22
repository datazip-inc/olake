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
	"github.com/twmb/franz-go/pkg/kmsg"
)

// NewReaderManager creates a new Kafka reader manager
func NewReaderManager(config ReaderConfig) *ReaderManager {
	return &ReaderManager{
		config:         config,
		readers:        make([]*kafkaReader, 0),
		partitionIndex: make(map[string]types.PartitionMetaData),
	}
}

// CreateReaders creates Kafka readers based on the provided streams and configuration and warmup the consumer group
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
			// Use eager rebalancing so all consumers receive rebalance callbacks during
			// rebalances, allowing CDC processing to stop gracefully and consistently.
			kgo.Balancers(kgo.RoundRobinBalancer()),
			kgo.FetchMinBytes(1),
			kgo.FetchMaxBytes(10e6),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.InstanceID(readerID),
		)
		reader, err := kgo.NewClient(readerOpts...)
		if err != nil {
			return fmt.Errorf("failed to create reader %d: %v", readerIndex, err)
		}
		r.readers = append(r.readers, &kafkaReader{
			id:       readerID,
			clientID: clientID,
			reader:   reader,
		})
	}
	logger.Infof("created %d readers for %d total partitions, with consumer group %s", len(r.readers), totalPartitions, consumerGroupID)
	// warmup the consumer group
	return r.warmupConsumerGroup(ctx, consumerGroupID)
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

	// fetch first offset of the all partition
	startOffsets, err := r.config.Admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to list start offsets for topic %s: %s", topic, err)
	}

	// fetch last offset of the all partition
	endOffsets, err := r.config.Admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to list end offsets for topic %s: %s", topic, err)
	}

	// fetch already committed offset of partition
	committedTopicOffsets, err := r.FetchCommittedOffsets(ctx, topic, topicDetail.Partitions)
	if err != nil {
		return fmt.Errorf("failed to fetch committed offsets for topic %s: %s", topic, err)
	}

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
	metadata, err := r.config.Admin.ListTopics(ctx, topic)
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
func (r *ReaderManager) FetchCommittedOffsets(ctx context.Context, topic string, partitions map[int32]kadm.PartitionDetail) (map[int]int64, error) {
	partitionsToFetch := make([]int32, 0, len(partitions))
	for _, partitionDetail := range partitions {
		partitionsToFetch = append(partitionsToFetch, int32(partitionDetail.Partition))
	}

	offsets, err := r.config.Admin.FetchOffsets(ctx, r.config.ConsumerGroupID)
	if err != nil {
		return nil, fmt.Errorf("could not fetch committed offsets for group %s", r.config.ConsumerGroupID)
	}

	committedTopicOffsets := make(map[int]int64)
	for _, partition := range partitionsToFetch {
		offset, exists := offsets.Lookup(topic, partition)
		if !exists {
			continue
		}

		committedTopicOffsets[int(partition)] = offset.At
	}
	return committedTopicOffsets, nil
}

// RemoveExistingConsumers force removes all existing consumers from the consumer group and closes reader clients.
func (r *ReaderManager) RemoveExistingConsumers(ctx context.Context, client *kgo.Client) error {
	if r.config.ConsumerGroupID != "" {
		groups, err := r.config.Admin.DescribeGroups(ctx, r.config.ConsumerGroupID)
		if err != nil {
			return fmt.Errorf("describe groups failed: %v", err)
		}

		group, ok := groups[r.config.ConsumerGroupID]
		if ok && group.Err != nil {
			return fmt.Errorf("describe groups error: %v", group.Err)
		}

		if ok && len(group.Members) > 0 {
			req := kmsg.NewPtrLeaveGroupRequest()
			req.Group = r.config.ConsumerGroupID

			for _, member := range group.Members {
				req.Members = append(req.Members, kmsg.LeaveGroupRequestMember{
					MemberID: member.MemberID,
					InstanceID: func() *string {
						if member.InstanceID == nil {
							return nil
						}
						v := *member.InstanceID
						return &v
					}(),
				})
			}

			response, err := req.RequestWith(ctx, client)
			if err != nil {
				return fmt.Errorf("leave group request failed: %v", err)
			}

			if response.ErrorCode != 0 {
				return fmt.Errorf("leave group error code: %d", response.ErrorCode)
			}
		}
	}

	for _, kafkaReader := range r.readers {
		if kafkaReader.reader != nil {
			kafkaReader.reader.Close()
		}
	}

	for kStr := range r.partitionIndex {
		delete(r.partitionIndex, kStr)
	}

	return nil
}

// RestartReader closes and recreates the reader using the same instanceID.
func (r *ReaderManager) RestartReader(_ context.Context, readerIndex int, streams []types.StreamInterface, consumerGroupID string) (*kgo.Client, error) {
	currentReader := r.GetReader(readerIndex)
	if currentReader == nil {
		return nil, fmt.Errorf("reader not found for readerIndex %d", readerIndex)
	}

	readerID, clientID := r.GetReaderIDAndClientID(readerIndex)

	currentReader.Close()

	reader, err := r.CreateReader(streams, consumerGroupID, readerID, clientID)
	if err != nil {
		// Slot must not keep a closed *kgo.Client; next Poll/Commit would be undefined.
		r.readers[readerIndex].reader = nil
		return nil, fmt.Errorf("%v: failed to recreate kafka reader %d after close: %v", constants.ErrNonRetryable, readerIndex, err)
	}

	r.readers[readerIndex].reader = reader

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
		// Use eager rebalancing so all consumers receive rebalance callbacks during
		// rebalances, allowing CDC processing to stop gracefully and consistently.
		kgo.Balancers(kgo.RoundRobinBalancer()),
		kgo.FetchMaxBytes(10e6),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, cl *kgo.Client, _ map[string][]int32) {
			if r.RebalanceDetected(cl) {
				r.exitMode.Store(gracefulExit)
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, cl *kgo.Client, _ map[string][]int32) {
			if r.RebalanceDetected(cl) {
				r.exitMode.Store(gracefulExit)
			}
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			logger.Warnf("reader %s lost partitions: %+v", clientID, lost)
			r.exitMode.Store(nonRetryableExit)
		}),
	)

	reader, err := kgo.NewClient(readerOpts...)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// GenerationID returns the consumer-group generation stored after CreateReaders warmup.
func (r *ReaderManager) GenerationID() int32 {
	return r.generationID.Load()
}

// RebalanceDetected is true when the client's group generation differs from the stored baseline.
func (r *ReaderManager) RebalanceDetected(client *kgo.Client) bool {
	_, generationID := client.GroupMetadata()
	return generationID >= 0 && generationID != r.generationID.Load()
}

// ShouldStopProcessing reports a consumer-group rebalance (assign/revoke during CDC).
// The fetch loop must exit with nil — not an error — so abstract layer does not retry.
func (r *ReaderManager) ShouldStopProcessing() bool {
	return r.exitMode.Load() == gracefulExit
}

// ErrForExitMode returns an error only for nonRetryableExit (e.g. partitions lost).
func (r *ReaderManager) ErrForExitMode() error {
	switch r.exitMode.Load() {
	case normalProcessing, gracefulExit:
		return nil
	case nonRetryableExit:
		return fmt.Errorf("%v: kafka sync aborted", constants.ErrNonRetryable)
	default:
		return fmt.Errorf("%v: kafka sync aborted", constants.ErrNonRetryable)
	}
}

// warmupConsumerGroup concurrently polls all readers until the Kafka consumer group completes JoinGroup/SyncGroup and partition assignment.
// Readiness is detected via GroupMetadata() instead of waiting for fetches to return records.
func (r *ReaderManager) warmupConsumerGroup(ctx context.Context, consumerGroupID string) (err error) {
	warmupCtx, warmupCancel := context.WithTimeout(ctx, 120*time.Second)

	pollGroup := utils.NewCGroup(warmupCtx)
	utils.ConcurrentInGroup(pollGroup, r.readers, func(ctx context.Context, _ int, kafkaReader *kafkaReader) error {
		for ctx.Err() == nil {
			if errs := kafkaReader.reader.PollFetches(ctx).Errors(); len(errs) > 0 && ctx.Err() == nil {
				return fmt.Errorf("warmup poll failed for consumer group %s: %v", consumerGroupID, errs[0].Err)
			}
		}
		return nil
	})

	allJoined := false
	defer func() {
		warmupCancel()
		err = pollGroup.Block()
		if err != nil {
			return
		}
		if !allJoined {
			err = fmt.Errorf("timed out waiting for consumer group %s join: %v", consumerGroupID, context.DeadlineExceeded)
		}
	}()

	for warmupCtx.Err() == nil {
		allJoined = true
		var generationID int32
		for _, kafkaReader := range r.readers {
			_, genID := kafkaReader.reader.GroupMetadata()
			if genID < 0 {
				allJoined = false
				break
			}
			generationID = genID
		}
		if allJoined {
			r.generationID.Store(generationID)
			logger.Infof("stored consumer group %s generation id: %d", consumerGroupID, generationID)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}
