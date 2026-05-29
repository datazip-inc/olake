package kafka

import (
	"fmt"
	"sort"

	"github.com/datazip-inc/olake/types"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ProtocolName implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) ProtocolName() string {
	return "olake-kafka-round-robin"
}

// IsCooperative returns false to indicate that the balancer is not cooperative.
func (b *CustomGroupBalancer) IsCooperative() bool {
	return false
}

// JoinGroupMetadata encodes consumer subscription metadata for group joining.
func (b *CustomGroupBalancer) JoinGroupMetadata(topicInterests []string, _ map[string][]int32, generation int32) []byte {
	memberMetadata := kmsg.NewConsumerMemberMetadata()
	memberMetadata.Version = 3
	memberMetadata.Topics = topicInterests
	memberMetadata.Generation = generation
	return memberMetadata.AppendTo(nil)
}

// ParseSyncAssignment decodes topic partition assignments from SyncGroup response data.
func (b *CustomGroupBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(assignment)
}

// MemberBalancer returns a GroupMemberBalancer for the given group members.
func (b *CustomGroupBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	consumerBalancer, err := kgo.NewConsumerBalancer(b, members)
	if err != nil {
		return nil, nil, err
	}
	return consumerBalancer, consumerBalancer.MemberTopics(), nil
}

// Balance assigns active partitions to consumers using round-robin distribution.
func (b *CustomGroupBalancer) Balance(consumerBalancer *kgo.ConsumerBalancer, partitionsPerTopic map[string]int32) kgo.IntoSyncAssignment {
	plan := consumerBalancer.NewPlan()
	members := consumerBalancer.Members()
	if len(members) == 0 {
		return plan
	}

	consumerCount := min(b.requiredConsumerIDs, len(members))
	if consumerCount == 0 {
		return plan
	}

	activePartitions := make([]types.PartitionKey, 0)
	for topic, partitionCount := range partitionsPerTopic {
		for partition := int32(0); partition < partitionCount; partition++ {
			if _, ok := b.partitionIndex[fmt.Sprintf("%s:%d", topic, partition)]; ok {
				activePartitions = append(activePartitions, types.PartitionKey{Topic: topic, Partition: partition})
			}
		}
	}

	sort.Slice(activePartitions, func(currentIndex, nextIndex int) bool {
		currentPartition, nextPartition := activePartitions[currentIndex], activePartitions[nextIndex]
		return currentPartition.Topic < nextPartition.Topic || (currentPartition.Topic == nextPartition.Topic && currentPartition.Partition < nextPartition.Partition)
	})

	for currentIndex, activePartition := range activePartitions {
		plan.AddPartition(&members[currentIndex%consumerCount], activePartition.Topic, activePartition.Partition)
	}

	return plan
}

// custom balancer example:
// | max_threads | total partitions | reader-IDs per stream (distinct) | reused? |
// | ------------ | ---------------- | -------------------------------- | ------- |
// | 6            | 6 (3+3)          | 3 + 3                            | no      |
// | 5            | 6                | 3 + 2                            | 1 ID    |
// | 4            | 6                | 2 + 2                            | 2 IDs   |
// | 3            | 6                | 2 + 1                            | 3 IDs   |
// | 2            | 6                | 1 + 1                            | 4 IDs   |
// | 1            | 6                | 1 + 1                            | 5 IDs   |
