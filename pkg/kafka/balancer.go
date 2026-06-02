package kafka

import (
	"fmt"

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
	memberMetadata.Topics = topicInterests
	return memberMetadata.AppendTo(nil)
}

// ParseSyncAssignment decodes topic partition assignments from SyncGroup response data.
func (b *CustomGroupBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(assignment)
}

// MemberBalancer returns a GroupMemberBalancer for the given group members.
func (b *CustomGroupBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	consumerBalancer, err := kgo.NewConsumerBalancer(b, members)
	return consumerBalancer, consumerBalancer.MemberTopics(), err
}

// Balance assigns active partitions to consumers using round-robin distribution.
func (b *CustomGroupBalancer) Balance(consumerBalancer *kgo.ConsumerBalancer, partitionsPerTopic map[string]int32) kgo.IntoSyncAssignment {
	// a new plan for partition assignment
	plan := consumerBalancer.NewPlan()

	// list of group members (consumers)
	members := consumerBalancer.Members()
	if len(members) == 0 {
		return plan
	}

	// number of consumers to use
	consumerCount := min(b.requiredConsumerIDs, len(members))

	// active partitions with data in partition index
	activePartitions := make([]types.PartitionKey, 0)
	for topic, partitions := range partitionsPerTopic {
		for partition := range partitions {
			if _, ok := b.partitionIndex[fmt.Sprintf("%s:%d", topic, partition)]; ok {
				activePartitions = append(activePartitions, types.PartitionKey{Topic: topic, Partition: partition})
			}
		}
	}

	// partition assignment in round-robin manner across consumers
	for index, activePartition := range activePartitions {
		consumerIndex := index % consumerCount
		plan.AddPartition(&members[consumerIndex], activePartition.Topic, activePartition.Partition)
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
