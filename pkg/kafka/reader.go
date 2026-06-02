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

// CreateReaders creates Kafka readers based on the provided streams and configuration
func (r *ReaderManager) CreateReaders(ctx context.Context, streams []types.StreamInterface) error {
	// populate topics from streams
	for _, stream := range streams {
		r.topics = append(r.topics, stream.Name())
	}

	r.partitionIndex = make(map[string]types.PartitionMetaData)
	for _, stream := range streams {
		if err := r.SetPartitions(ctx, stream); err != nil {
			return fmt.Errorf("failed to set partitions for stream %s: %s", stream.ID(), err)
		}
	}

	// total partitions with new messages
	totalPartitions := len(r.partitionIndex)
	if totalPartitions == 0 {
		logger.Infof("no partitions with new messages, skipping reader creation for group %s", r.config.ConsumerGroupID)
		return nil
	}

	// reader tasks = max threads if set to total partitions
	readersToCreate := utils.Ternary(r.ShouldMatchPartitionCount(), totalPartitions, utils.Ternary(r.config.MaxThreads > totalPartitions, totalPartitions, r.config.MaxThreads).(int)).(int)

	for readerIndex := range readersToCreate {
		readerID := fmt.Sprintf("group_%s", utils.ULID())
		clientID := fmt.Sprintf("olake-%s-%s", r.config.ConsumerGroupID, readerID)

		// create reader (rebalance callbacks disabled during initial reader creation and partition assignment)
		reader, err := r.CreateReader(readerID, clientID, readersToCreate, false)
		if err != nil {
			return fmt.Errorf("failed to create reader %d: %s", readerIndex, err)
		}

		// add reader to manager
		r.readers = append(r.readers, &kafkaReader{
			id:       readerID,
			clientID: clientID,
			reader:   reader,
		})
	}
	logger.Infof("created %d readers for %d total partitions, with consumer group %s", len(r.readers), totalPartitions, r.config.ConsumerGroupID)
	// wait for consumer group members to join and partitions to be assigned, with a 2-minute deadline.
	return r.waitForPartitionAssignment(ctx)
}

// GetReader returns the created readers
func (r *ReaderManager) GetReader(readerID int) *kgo.Client {
	return r.readers[readerID].reader
}

// GetReaderCount returns the created readers count
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

// GetReaderIDAndClientID returns the reader client IDs
func (r *ReaderManager) GetReaderIDAndClientID(readerIndex int) (string, string) {
	return r.readers[readerIndex].id, r.readers[readerIndex].clientID
}

// sets partitions that need to be synced for a stream
func (r *ReaderManager) SetPartitions(ctx context.Context, stream types.StreamInterface) error {
	topic := stream.Name()
	topicDetail, topicDetailErr := r.GetTopicMetadata(ctx, topic)
	if topicDetailErr != nil {
		return fmt.Errorf("failed to fetch topic metadata for topic %s: %s", topic, topicDetailErr)
	}

	// fetch first offset details of the all partition
	startOffsets, startOffsetErr := r.config.AdminClient.ListStartOffsets(ctx, topic)
	if startOffsetErr != nil {
		return fmt.Errorf("failed to list start offsets for topic %s: %s", topic, startOffsetErr)
	}

	// fetch last offset details of the all partition
	endOffsets, endOffsetErr := r.config.AdminClient.ListEndOffsets(ctx, topic)
	if endOffsetErr != nil {
		return fmt.Errorf("failed to list end offsets for topic %s: %s", topic, endOffsetErr)
	}

	// fetch already committed offset of partition
	committedTopicOffsets, committedOffsetsErr := r.FetchCommittedOffsets(ctx, topic)
	if committedOffsetsErr != nil {
		return fmt.Errorf("failed to fetch committed offsets for topic %s: %s", topic, committedOffsetsErr)
	}

	// build partition metadata
	for _, partitionDetail := range topicDetail.Partitions {
		startOffsetDetail, startOffsetExists := startOffsets.Lookup(topic, partitionDetail.Partition)
		if !startOffsetExists {
			logger.Infof("skipping partition %d for topic %s, start offset not found", partitionDetail.Partition, topic)
			continue
		}

		endOffsetDetail, endOffsetExists := endOffsets.Lookup(topic, partitionDetail.Partition)
		if !endOffsetExists {
			logger.Infof("skipping partition %d for topic %s, end offset not found", partitionDetail.Partition, topic)
			continue
		}

		// check if the partition has any messages at all, if not then skip
		if startOffsetDetail.Offset >= endOffsetDetail.Offset {
			logger.Infof("skipping empty partition %d for topic %s (first: %d, last: %d)", partitionDetail.Partition, topic, startOffsetDetail.Offset, endOffsetDetail.Offset)
			continue
		}

		committedOffset := committedTopicOffsets[partitionDetail.Partition]

		// if a committed offset is available and there are no new messages, skip
		if committedOffset >= endOffsetDetail.Offset {
			logger.Infof("skipping partition %d for topic %s, no new messages (committed: %d, last: %d)", partitionDetail.Partition, topic, committedOffset, endOffsetDetail.Offset)
			continue
		}

		r.partitionIndex[fmt.Sprintf("%s:%d", topic, partitionDetail.Partition)] = types.PartitionMetaData{
			Stream:      stream,
			PartitionID: partitionDetail.Partition,
			EndOffset:   endOffsetDetail.Offset,
		}
	}
	return nil
}

// GetTopicMetadata fetches metadata for a topic
func (r *ReaderManager) GetTopicMetadata(ctx context.Context, topic string) (*kadm.TopicDetail, error) {
	metadata, err := r.config.AdminClient.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata for topic %s: %s", topic, err)
	}

	topicDetail, exists := metadata[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found in metadata", topic)
	}
	return &topicDetail, nil
}

// FetchCommittedOffsets fetches committed offsets for a topic.
func (r *ReaderManager) FetchCommittedOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	offsets, err := r.config.AdminClient.FetchOffsets(ctx, r.config.ConsumerGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch committed offsets for group %s: %s", r.config.ConsumerGroupID, err)
	}

	committedTopicOffsets := make(map[int32]int64)

	topicOffsets, exists := offsets[topic]
	if !exists {
		return committedTopicOffsets, nil
	}

	for partition, offset := range topicOffsets {
		committedTopicOffsets[partition] = offset.At
	}

	return committedTopicOffsets, nil
}

// RemoveExistingConsumers force removes all existing consumers from the consumer group and closes reader clients.
func (r *ReaderManager) RemoveExistingConsumers(ctx context.Context, client *kgo.Client) error {
	// coordinator may not be active immediately (due to broker startup or coordinator election); retry describe until ready.
	cleanupCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	var describedGroups kadm.DescribedGroups
	for {
		var describeErr error
		describedGroups, describeErr = r.config.AdminClient.DescribeGroups(cleanupCtx, r.config.ConsumerGroupID)
		if describeErr == nil {
			break
		}
		select {
		case <-cleanupCtx.Done():
			return fmt.Errorf("describe groups failed: %s", describeErr)
		case <-time.After(2 * time.Second):
		}
	}

	describedGroup := describedGroups[r.config.ConsumerGroupID]
	if describedGroup.Err != nil {
		return fmt.Errorf("describe groups error: %s", describedGroup.Err)
	}

	if len(describedGroup.Members) > 0 {
		leaveGroupRequest := kmsg.NewPtrLeaveGroupRequest()
		leaveGroupRequest.Group = r.config.ConsumerGroupID

		for _, member := range describedGroup.Members {
			leaveGroupRequest.Members = append(leaveGroupRequest.Members, kmsg.LeaveGroupRequestMember{
				MemberID:   member.MemberID,
				InstanceID: member.InstanceID,
			})
		}

		leaveGroupResponse, leaveGroupResponseErr := leaveGroupRequest.RequestWith(cleanupCtx, client)
		if leaveGroupResponseErr != nil {
			return fmt.Errorf("leave group request failed: %s", leaveGroupResponseErr)
		}

		if leaveGroupResponse.ErrorCode != 0 {
			return fmt.Errorf("leave group error code: %d", leaveGroupResponse.ErrorCode)
		}
	}

	for _, kafkaReader := range r.readers {
		kafkaReader.reader.Close()
	}

	return nil
}

// RestartReader closes and recreates the reader using the same instanceID.
func (r *ReaderManager) RestartReader(readerIndex int) (*kgo.Client, error) {
	currentReader := r.GetReader(readerIndex)
	if currentReader == nil {
		return nil, fmt.Errorf("reader not found for readerIndex %d", readerIndex)
	}

	readerID, clientID := r.GetReaderIDAndClientID(readerIndex)

	currentReader.Close()

	newReader, err := r.CreateReader(readerID, clientID, len(r.readers), true)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to recreate kafka reader %d after close: %s", constants.ErrNonRetryable, readerIndex, err)
	}

	r.readers[readerIndex].reader = newReader

	return newReader, nil
}

// CreateReader creates a single kafka reader client.
// When enableRebalanceCallbacks is true, rebalance callbacks are registered on the reader.
func (r *ReaderManager) CreateReader(readerID, clientID string, requiredConsumers int, enableRebalanceCallbacks bool) (*kgo.Client, error) {
	readerOpts := append([]kgo.Opt{}, r.config.Dialer...)

	readerOpts = append(
		readerOpts,
		kgo.ConsumerGroup(r.config.ConsumerGroupID),
		kgo.ClientID(clientID),
		kgo.InstanceID(readerID),
		kgo.ConsumeTopics(r.topics...),
		kgo.Balancers(&CustomGroupBalancer{
			requiredConsumerIDs: requiredConsumers,
			partitionIndex:      r.partitionIndex,
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(10e6),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	if enableRebalanceCallbacks {
		// Exit gracefully when a rebalance is detected via assign/revoke callbacks.
		onRebalance := func(_ context.Context, client *kgo.Client, _ map[string][]int32) {
			if r.RebalanceDetected(client) {
				r.exitMode.Store(gracefulExit)
			}
		}

		// Trigger non-retryable shutdown when partition ownership is lost.
		onPartitionsLost := func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			logger.Warnf("reader %s lost partitions: %+v", clientID, lost)
			r.exitMode.Store(nonRetryableExit)
		}

		readerOpts = append(readerOpts,
			kgo.OnPartitionsAssigned(onRebalance),
			kgo.OnPartitionsRevoked(onRebalance),
			kgo.OnPartitionsLost(onPartitionsLost),
		)
	}

	reader, err := kgo.NewClient(readerOpts...)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// RebalanceDetected is true when the client's group generation differs from the stored baseline.
func (r *ReaderManager) RebalanceDetected(client *kgo.Client) bool {
	_, generationID := client.GroupMetadata()
	return generationID >= 0 && generationID != r.generationID.Load()
}

// FetchExitState reports whether CDC processing should stop after PollFetches.
// exitMode is updated by consumer group rebalance callbacks before PollFetches returns.
func (r *ReaderManager) FetchExitState() (stop bool, err error) {
	// ReaderManager will be nil during discover mode.
	if r == nil {
		return false, nil
	}

	switch r.exitMode.Load() {
	case normalProcessing:
		return false, nil
	case gracefulExit:
		logger.Infof("stopping kafka CDC processing gracefully due to consumer group rebalance")
		return true, nil
	case nonRetryableExit:
		return true, fmt.Errorf("%w: kafka sync aborted due to partition loss during consumer group rebalance", constants.ErrNonRetryable)
	default:
		return true, fmt.Errorf("%w: kafka sync aborted: unexpected exit mode", constants.ErrNonRetryable)
	}
}

// waitForPartitionAssignment blocks until Kafka completes partition assignment
// for all readers in the consumer group.
func (r *ReaderManager) waitForPartitionAssignment(ctx context.Context) error {
	joinCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		if joinCtx.Err() != nil {
			return fmt.Errorf("timed out waiting for partition assignment on consumer group %s: %s", r.config.ConsumerGroupID, joinCtx.Err())
		}
		var (
			allReadersJoined           = true
			expectedGenerationID int32 = -1
		)
		for _, kafkaReader := range r.readers {
			_, currentReaderGenerationID := kafkaReader.reader.GroupMetadata()

			// generation id -1 means not yet joined
			// mismatch means readers are on different generations, partition assignment not yet completed
			if currentReaderGenerationID < 0 || (expectedGenerationID >= 0 && expectedGenerationID != currentReaderGenerationID) {
				allReadersJoined = false
				break
			}
			if expectedGenerationID < 0 {
				expectedGenerationID = currentReaderGenerationID
			}
		}

		if allReadersJoined {
			r.generationID.Store(expectedGenerationID)
			// brief wait to let partition assignment fully propagate before fetching starts.
			time.Sleep(2 * time.Second)
			logger.Infof("consumer group %s stable: all readers assigned, generation id: %d", r.config.ConsumerGroupID, expectedGenerationID)
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}
