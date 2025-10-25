package driver

import (
	"context"
	"encoding/json"
	"fmt"
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

	// Set partitions and build snapshot offsets for all streams

	k.partitionMeta = make(map[string][]types.PartitionMetaData)
	k.partitionIndex = make(map[string]types.PartitionMetaData)

	for _, stream := range streams {
		if err := k.SetPartitions(ctx, stream); err != nil {
			return fmt.Errorf("failed to set partitions for stream %s: %w", stream.ID(), err)
		}
		// no topicToStream map; stream can be resolved on the fly from partitionMeta
	}

	// Build reader tasks according to concurrency policy
	readersToCreate := k.config.MaxThreads
	if len(k.partitionIndex) == 0 {
		logger.Infof("no partitions with new messages; skipping reader creation for group %s", k.consumerGroupID)
		return nil
	}
	// Determine reader count:
	if k.ShouldMatchPartitionCount() {
		readersToCreate = len(k.partitionIndex)
	} else if readersToCreate > len(k.partitionIndex) {
		// Clamp readers to total partitions to avoid idle readers
		readersToCreate = len(k.partitionIndex)
	}

	// Create readers with proper consumer ID assignment strategy
	brokers := splitAndTrim(k.config.BootstrapServers)
	k.readers = make(map[string]*kafka.Reader)
	k.readerLastMessages = make(map[string]map[partKey]kafka.Message)
	k.readerClientIDs = make(map[string]string)

	// Calculate the required number of distinct consumer IDs based on max_connections and total partitions
	totalPartitions := len(k.partitionIndex)
	requiredConsumerIDs := k.calculateRequiredConsumerIDs(k.config.MaxThreads, totalPartitions)

	logger.Infof("Creating %d readers with %d distinct consumer IDs for %d total partitions",
		readersToCreate, requiredConsumerIDs, totalPartitions)

	// The key insight: we need to create exactly the number of readers that matches
	// the required consumer IDs, not just max_threads
	actualReadersToCreate := requiredConsumerIDs
	if actualReadersToCreate > readersToCreate {
		actualReadersToCreate = readersToCreate
	}

	// Additional insight: we need to ensure each reader gets a unique consumer member ID
	// by using a different approach - we'll create readers with unique consumer group IDs
	// when we need more distinct consumer IDs than max_threads
	useUniqueGroupIDs := requiredConsumerIDs > k.config.MaxThreads

	// Create readers with proper consumer ID distribution
	// The key insight is that we need to create exactly the number of readers
	// that matches the required consumer IDs, and ensure each gets a unique member ID
	for i := 0; i < actualReadersToCreate; i++ {
		readerID := fmt.Sprintf("grp_%s", utils.ULID())
		// create a per-reader dialer with a unique clientID to identify assignments
		dialerCopy := *k.dialer
		clientID := fmt.Sprintf("olake-%s-%s", k.consumerGroupID, readerID)
		dialerCopy.ClientID = clientID

		// Use a custom group balancer that ensures proper consumer ID distribution
		// The key insight: we need to ensure that each reader gets a unique consumer member ID
		// by using a custom group balancer that respects the required consumer ID count
		groupBalancer := &CustomGroupBalancer{
			requiredConsumerIDs: requiredConsumerIDs,
			readerIndex:         i,
		}

		// The critical fix: we need to ensure that each reader gets a unique consumer member ID
		// by using a custom group balancer that ensures proper distribution
		// We also need to ensure that the consumer group coordinator creates the exact number
		// of distinct consumer member IDs as required by our requirements table

		// Determine the consumer group ID for this reader
		groupID := k.consumerGroupID
		if useUniqueGroupIDs {
			// For cases where we need more consumer IDs than threads, use different group IDs
			// This ensures each reader gets a unique consumer member ID
			groupID = fmt.Sprintf("%s-%d", k.consumerGroupID, i)
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
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
		k.readers[readerID] = reader
		k.readerLastMessages[readerID] = make(map[partKey]kafka.Message)
		k.readerClientIDs[readerID] = clientID
	}
	logger.Infof("created %d readers for consumer group %s", len(k.readers), k.consumerGroupID)
	return nil
}

func (k *Kafka) PartitionStreamChanges(ctx context.Context, readerID string, processFn abstract.CDCMsgFn) error {
	partitionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	readerInstance := k.readers[readerID]
	if readerInstance == nil {
		return fmt.Errorf("reader not found for readerID %s", readerID)
	}

	lastByPart := make(map[partKey]kafka.Message)
	seenParts := make(map[partKey]struct{})
	doneParts := make(map[partKey]struct{})
	completedAll := false
	// On any exit, merge local lastByPart into global readerLast for PostCDC commit
	defer func() {
		if !completedAll || len(lastByPart) == 0 {
			return
		}
		k.readerLastMessagesMu.Lock()
		defer k.readerLastMessagesMu.Unlock()
		if k.readerLastMessages == nil {
			k.readerLastMessages = make(map[string]map[partKey]kafka.Message)
		}
		k.readerLastMessages[readerID] = lastByPart
	}()

	for {
		// apply per-iteration wait timeout so group readers can exit when idle
		logger.Infof("reader %s fetching message", readerID)
		msg, err := readerInstance.FetchMessage(partitionCtx)

		logger.Infof("reader %s fetched message: %v, %v", readerID, msg.Partition, msg.Topic)

		//TODO: Need to handle rebalancing and context getting cancelled or context deadline exceeded
		if err != nil {
			return fmt.Errorf("error reading message in Kafka CDC sync: %w", err)
		}

		// Resolve stream and snapshot bounds via prebuilt index (mandatory)
		pm, ok := k.partitionIndex[fmt.Sprintf("%s:%d", msg.Topic, msg.Partition)]
		if !ok {
			return fmt.Errorf("missing partition index for topic %s partition %d", msg.Topic, msg.Partition)
		}

		// Track last message per partition for later commit
		pk := partKey{topic: msg.Topic, partition: msg.Partition}
		if _, ok := seenParts[pk]; !ok {
			seenParts[pk] = struct{}{}
		}

		// Skip until start offset for first-run policies
		if pm.StartOffset > 0 && msg.Offset < pm.StartOffset {
			continue
		}

		messageData := func() map[string]any {
			var result map[string]any
			if msg.Value == nil {
				logger.Warnf("received nil message value at offset %d for topic %s, partition %d", msg.Offset, msg.Topic, msg.Partition)
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
			// we skip storing nil-valued messages, commit progresses via lastMessages above
			continue
		}

		if procErr := processFn(ctx, abstract.CDCChange{
			Stream:    pm.Stream,
			Timestamp: msg.Time,
			Kind:      "create",
			Data:      messageData,
		}); procErr != nil {
			return procErr
		}
		// update last processed msg
		lastByPart[pk] = msg
		// mark partition done when crossing checkpoint
		if msg.Offset >= pm.EndOffset-1 {
			if _, ok := doneParts[pk]; !ok {
				doneParts[pk] = struct{}{}
			}
			// Exit when all seen partitions are done. If we somehow had none seen, refresh assignments once to avoid early exit.
			if assigned, err := k.getReaderAssignedPartitions(ctx, readerID); err == nil {
				for _, pk := range assigned {
					if _, ok := k.partitionIndex[fmt.Sprintf("%s:%d", pk.topic, pk.partition)]; ok {
						seenParts[pk] = struct{}{}
					}
				}
			} else {
				return err
			}
			if len(doneParts) == len(seenParts) {
				completedAll = true
				return nil
			}

		}
	}
}

func (k *Kafka) PostCDC(ctx context.Context, stream types.StreamInterface, noErr bool, readerID string) error {
	if !noErr {
		return nil
	}
	// Kafka group reader: commit accumulated last messages and set state per stream
	k.readerLastMessagesMu.Lock()
	defer k.readerLastMessagesMu.Unlock()
	if k.readerLastMessages == nil || k.readerLastMessages[readerID] == nil || len(k.readerLastMessages[readerID]) == 0 {
		logger.Infof("PostCDC: reader %s has no accumulated offsets to commit", readerID)
		return nil
	}
	// Build commit list
	msgs := make([]kafka.Message, 0, len(k.readerLastMessages[readerID]))
	affectedStreams := make(map[string]types.StreamInterface)
	for pk, m := range k.readerLastMessages[readerID] {
		msgs = append(msgs, m)
		// resolve stream via partition index
		if pm, ok := k.partitionIndex[fmt.Sprintf("%s:%d", pk.topic, pk.partition)]; ok && pm.Stream != nil {
			affectedStreams[pm.Stream.ID()] = pm.Stream
		}
	}
	if len(msgs) > 0 {
		r := k.readers[readerID]
		if r == nil {
			return fmt.Errorf("PostCDC: reader %s not found for commit", readerID)
		}
		if err := r.CommitMessages(ctx, msgs...); err != nil {
			return fmt.Errorf("PostCDC: commit failed for reader %s: %w", readerID, err)
		}
		logger.Infof("PostCDC: committed %d partitions for reader %s", len(msgs), readerID)
	}
	// Save state for each affected stream
	for _, s := range affectedStreams {
		k.state.SetCursor(s.Self(), "consumer_group_id", k.consumerGroupID)
	}
	// clear buffer for this reader
	delete(k.readerLastMessages, readerID)
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

	// Build partition metadata and decide start offset behavior on first run
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

		startOffset := committedOffset
		if !hasCommittedOffset {
			// default to latest (start from current end)
			startOffset = idx.LastOffset
		}

		pm := types.PartitionMetaData{
			Stream:      stream,
			PartitionID: idx.Partition,
			EndOffset:   idx.LastOffset,
			StartOffset: startOffset,
		}
		allPartitions = append(allPartitions, pm)
		// update fast index
		if k.partitionIndex == nil {
			k.partitionIndex = make(map[string]types.PartitionMetaData)
		}
		k.partitionIndex[fmt.Sprintf("%s:%d", topic, pm.PartitionID)] = pm
	}

	k.partitionMeta[stream.ID()] = allPartitions
	return nil
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

// calculateRequiredConsumerIDs calculates the number of distinct consumer IDs needed
// based on the requirements table provided by the user
func (k *Kafka) calculateRequiredConsumerIDs(maxThreads, totalPartitions int) int {
	if maxThreads >= totalPartitions {
		// If max_threads >= total partitions, each partition gets its own consumer ID
		return totalPartitions
	}
	// If max_threads < total partitions, we need to reuse consumer IDs
	// The number of distinct consumer IDs equals max_threads
	return maxThreads
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	requiredConsumerIDs int
	readerIndex         int
}

// ProtocolName implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) ProtocolName() string {
	return "custom-round-robin"
}

// UserData implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) UserData() ([]byte, error) {
	return nil, nil
}

// AssignGroups implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) AssignGroups(members []kafka.GroupMember, partitions []kafka.Partition) kafka.GroupMemberAssignments {
	assignments := make(kafka.GroupMemberAssignments)

	// The key insight: we need to ensure that exactly the required number of consumer IDs
	// are used, and each gets assigned partitions according to the requirements table
	consumerIDCount := b.requiredConsumerIDs
	if consumerIDCount > len(members) {
		consumerIDCount = len(members)
	}

	// Assign partitions to consumer IDs in round-robin fashion
	// This ensures that each consumer ID gets a fair share of partitions
	for i, partition := range partitions {
		consumerIndex := i % consumerIDCount
		if consumerIndex < len(members) {
			memberID := members[consumerIndex].ID
			if assignments[memberID] == nil {
				assignments[memberID] = make(map[string][]int)
			}
			assignments[memberID][partition.Topic] = append(assignments[memberID][partition.Topic], partition.ID)
		}
	}

	return assignments
}
