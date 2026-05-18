package kafka

import (
	"errors"
	"fmt"
	"sort"

	"github.com/datazip-inc/olake/utils/logger"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type CustomGroupBalancer struct {
	requiredConsumerIDs int
	partitionIndex      map[string]struct{}

	// sticky ownership: topic -> Kafka static group instance id (GROUP_INSTANCE_ID).
	// Intentionally not keyed by  MemberID so ownership survives member id rotation on restart.
	topicOwners map[string]string
}

// NewCustomGroupBalancer returns a GroupBalancer shared by all consumer clients in the same
// process so topicOwners survives leader changes within that process.
//
// The balancer is cooperative (KIP-429): join metadata carries OwnedPartitions, and the
// leader applies BalancePlan.AdjustCooperative before sync. Once every member uses this
// protocol, the group must not mix in eager-only assignors for the same group.id.
func NewCustomGroupBalancer(requiredConsumerIDs int, partitionIndex map[string]struct{}) *CustomGroupBalancer {
	return &CustomGroupBalancer{
		requiredConsumerIDs: requiredConsumerIDs,
		partitionIndex:      partitionIndex,
		topicOwners:         make(map[string]string),
	}
}

func (b *CustomGroupBalancer) ProtocolName() string {
	return "olake-sticky-topic"
}

func (b *CustomGroupBalancer) IsCooperative() bool {
	return true
}

func (b *CustomGroupBalancer) JoinGroupMetadata(
	topicInterests []string,
	currentAssignment map[string][]int32,
	generation int32,
) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 3
	meta.Topics = topicInterests
	meta.Generation = generation
	logger.Infof("JoinGroupMetadata")
	if len(currentAssignment) > 0 {
		topicNames := make([]string, 0, len(currentAssignment))
		for topic := range currentAssignment {
			topicNames = append(topicNames, topic)
		}
		sort.Strings(topicNames)

		owned := make([]kmsg.ConsumerMemberMetadataOwnedPartition, 0, len(topicNames))
		for _, topic := range topicNames {
			parts := append([]int32(nil), currentAssignment[topic]...)
			if len(parts) == 0 {
				continue
			}
			sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
			owned = append(owned, kmsg.ConsumerMemberMetadataOwnedPartition{
				Topic:      topic,
				Partitions: parts,
			})
		}
		meta.OwnedPartitions = owned
	}

	return meta.AppendTo(nil)
}

func (b *CustomGroupBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return kgo.ParseConsumerSyncAssignment(assignment)
}

func (b *CustomGroupBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	cb, err := kgo.NewConsumerBalancer(b, members)
	if err != nil {
		return nil, nil, err
	}
	return cb, cb.MemberTopics(), nil
}

// Balance implements kgo.ConsumerBalancerBalance.
func (b *CustomGroupBalancer) Balance(cb *kgo.ConsumerBalancer, topics map[string]int32) kgo.IntoSyncAssignment {
	plan := cb.NewPlan()

	members := append([]kmsg.JoinGroupResponseMember(nil), cb.Members()...)
	if len(members) == 0 {
		return plan
	}

	for i := range members {
		m := &members[i]
		if m.InstanceID == nil || *m.InstanceID == "" {
			cb.SetError(errors.New("olake-sticky-topic (cooperative) balancer requires every member to set a non-empty static group instance id (kgo.InstanceID); no MemberID fallback for stickiness"))
			return plan
		}
	}

	sort.Slice(members, func(i, j int) bool {
		return *members[i].InstanceID < *members[j].InstanceID
	})

	instanceToMember := make(map[string]*kmsg.JoinGroupResponseMember, len(members))
	for i := range members {
		m := &members[i]
		instanceToMember[*m.InstanceID] = m
	}

	consumerIDCount := min(
		b.requiredConsumerIDs,
		len(members),
	)

	type topicInfo struct {
		name       string
		partitions []int32
		weight     int
	}

	topicMap := make(map[string]*topicInfo)

	// build topic metadata
	for topic, partitionCount := range topics {

		for partition := int32(0); partition < partitionCount; partition++ {

			key := fmt.Sprintf(
				"%s:%d",
				topic,
				partition,
			)

			if _, exists := b.partitionIndex[key]; !exists {
				continue
			}

			if _, exists := topicMap[topic]; !exists {
				topicMap[topic] = &topicInfo{
					name: topic,
				}
			}

			topicMap[topic].partitions =
				append(
					topicMap[topic].partitions,
					partition,
				)

			topicMap[topic].weight++
		}
	}

	topicList := make([]*topicInfo, 0, len(topicMap))

	for _, topic := range topicMap {
		topicList = append(topicList, topic)
	}

	// largest topics first
	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].weight >
			topicList[j].weight
	})

	memberLoad := make(map[string]int)

	for idx := 0; idx < consumerIDCount; idx++ {
		member := &members[idx]
		memberLoad[member.MemberID] = 0
	}

	unassignedTopics := []*topicInfo{}

	// sticky assignment first (topic -> static instance id)
	for _, topic := range topicList {

		ownerInstance, stickyKnown := b.topicOwners[topic.name]
		if !stickyKnown || ownerInstance == "" {
			unassignedTopics = append(unassignedTopics, topic)
			continue
		}

		member := instanceToMember[ownerInstance]
		if member != nil {
			for _, partition := range topic.partitions {
				plan.AddPartition(
					member,
					topic.name,
					partition,
				)
			}

			memberLoad[member.MemberID] += topic.weight

			continue
		}

		unassignedTopics = append(unassignedTopics, topic)
	}

	// assign remaining topics
	// to least loaded member
	for _, topic := range unassignedTopics {

		var selectedMember *kmsg.JoinGroupResponseMember
		minLoad := -1

		for idx := 0; idx < consumerIDCount; idx++ {

			member := &members[idx]

			if minLoad == -1 ||
				memberLoad[member.MemberID] < minLoad {

				minLoad =
					memberLoad[member.MemberID]

				selectedMember =
					member
			}
		}

		for _, partition := range topic.partitions {

			plan.AddPartition(
				selectedMember,
				topic.name,
				partition,
			)
		}

		memberLoad[selectedMember.MemberID] +=
			topic.weight

		// persist sticky ownership by static instance id (not MemberID).
		b.topicOwners[topic.name] = *selectedMember.InstanceID
	}

	plan.AdjustCooperative(cb)
	return plan
}
