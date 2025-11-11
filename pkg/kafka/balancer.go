package kafka

import (
	"github.com/segmentio/kafka-go"
)

// ProtocolName implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) ProtocolName() string {
	return "olake-kafka-round-robin"
}

// UserData implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) UserData() ([]byte, error) {
	return nil, nil
}

// AssignGroups implements kafka.GroupBalancer interface
func (b *CustomGroupBalancer) AssignGroups(members []kafka.GroupMember, partitions []kafka.Partition) kafka.GroupMemberAssignments {
	assignments := make(kafka.GroupMemberAssignments)

	// number of consumers to use
	consumerIDCount := min(b.requiredConsumerIDs, len(members))

	// quick lookup map from partitionIndex: topic -> set of partition IDs
	topicPartitionsMap := make(map[string]map[int]struct{})
	for _, metadata := range b.partitionIndex {
		if metadata.Stream == nil || metadata.Stream.Name() == "" {
			continue
		}
		if _, exists := topicPartitionsMap[metadata.Stream.Name()]; !exists {
			topicPartitionsMap[metadata.Stream.Name()] = make(map[int]struct{})
		}
		topicPartitionsMap[metadata.Stream.Name()][metadata.PartitionID] = struct{}{}
	}

	// active partitions with data in partition index
	activePartitions := make([]kafka.Partition, 0, len(partitions))
	for _, partition := range partitions {
		if topicSet, ok := topicPartitionsMap[partition.Topic]; ok {
			if _, exists := topicSet[partition.ID]; exists {
				activePartitions = append(activePartitions, partition)
			}
		}
	}

	if len(activePartitions) != 0 {
		// Assign partitions to consumers in round-robin
		for idx, partition := range activePartitions {
			consumerIndex := idx % consumerIDCount
			memberID := members[consumerIndex].ID
			if assignments[memberID] == nil {
				assignments[memberID] = make(map[string][]int)
			}
			assignments[memberID][partition.Topic] = append(assignments[memberID][partition.Topic], partition.ID)
		}
	}

	return assignments
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
