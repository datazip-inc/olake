package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// groupProtocolName is the consumer-group protocol olake's reader advertises. Restated here, not
// imported: every member of a group must offer the same protocol, so this is a wire contract.
const groupProtocolName = "olake-kafka-round-robin"

// triggerBalancer lets the rebalance trigger join olake's consumer group by speaking its protocol.
// It assigns only activePartitions, round-robin across members, mirroring what olake's reader does.
type triggerBalancer struct {
	topic            string
	activePartitions map[int32]struct{}
}

func newTriggerBalancer(topic string, partitions ...int32) *triggerBalancer {
	active := make(map[int32]struct{}, len(partitions))
	for _, p := range partitions {
		active[p] = struct{}{}
	}
	return &triggerBalancer{topic: topic, activePartitions: active}
}

func (b *triggerBalancer) ProtocolName() string { return groupProtocolName }

func (b *triggerBalancer) IsCooperative() bool { return false }

func (b *triggerBalancer) JoinGroupMetadata(topicInterests []string, _ map[string][]int32, _ int32) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Topics = topicInterests
	return meta.AppendTo(nil)
}

func (b *triggerBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(assignment)
}

func (b *triggerBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	consumerBalancer, err := kgo.NewConsumerBalancer(b, members)
	return consumerBalancer, consumerBalancer.MemberTopics(), err
}

// Balance runs only when this member is elected group leader; olake's own balancer runs otherwise.
func (b *triggerBalancer) Balance(consumerBalancer *kgo.ConsumerBalancer, partitionsPerTopic map[string]int32) kgo.IntoSyncAssignment {
	plan := consumerBalancer.NewPlan()
	members := consumerBalancer.Members()
	if len(members) == 0 {
		return plan
	}

	assigned := 0
	for topic, partitions := range partitionsPerTopic {
		for partition := int32(0); partition < partitions; partition++ {
			if topic != b.topic {
				continue
			}
			if _, ok := b.activePartitions[partition]; !ok {
				continue
			}
			plan.AddPartition(&members[assigned%len(members)], topic, partition)
			assigned++
		}
	}
	return plan
}
