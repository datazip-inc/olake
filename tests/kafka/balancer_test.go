package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// rebalanceBalancer speaks the driver's "olake-kafka-round-robin" consumer-group protocol
// so the trigger consumer can join the driver's group and force a rebalance. It claims
// only the partitions in active (keyed "topic:partition"), mirroring how the driver
// assigns just the partitions it holds metadata for.
type rebalanceBalancer struct {
	active map[string]bool
}

func newRebalanceBalancer(topic string, partitions ...int32) *rebalanceBalancer {
	active := make(map[string]bool, len(partitions))
	for _, partition := range partitions {
		active[fmt.Sprintf("%s:%d", topic, partition)] = true
	}
	return &rebalanceBalancer{active: active}
}

func (b *rebalanceBalancer) ProtocolName() string {
	return "olake-kafka-round-robin"
}

func (b *rebalanceBalancer) IsCooperative() bool {
	return false
}

func (b *rebalanceBalancer) JoinGroupMetadata(topicInterests []string, _ map[string][]int32, _ int32) []byte {
	memberMetadata := kmsg.NewConsumerMemberMetadata()
	memberMetadata.Topics = topicInterests
	return memberMetadata.AppendTo(nil)
}

func (b *rebalanceBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(assignment)
}

func (b *rebalanceBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	consumerBalancer, err := kgo.NewConsumerBalancer(b, members)
	return consumerBalancer, consumerBalancer.MemberTopics(), err
}

func (b *rebalanceBalancer) Balance(consumerBalancer *kgo.ConsumerBalancer, partitionsPerTopic map[string]int32) kgo.IntoSyncAssignment {
	plan := consumerBalancer.NewPlan()

	members := consumerBalancer.Members()
	if len(members) == 0 {
		return plan
	}

	type partitionKey struct {
		topic     string
		partition int32
	}
	activePartitions := make([]partitionKey, 0)
	for topic, partitions := range partitionsPerTopic {
		for partition := range partitions {
			if b.active[fmt.Sprintf("%s:%d", topic, partition)] {
				activePartitions = append(activePartitions, partitionKey{topic: topic, partition: partition})
			}
		}
	}

	// partition assignment in round-robin manner across consumers
	for index, activePartition := range activePartitions {
		consumerIndex := index % len(members)
		plan.AddPartition(&members[consumerIndex], activePartition.topic, activePartition.partition)
	}

	return plan
}
