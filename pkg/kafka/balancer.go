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

	// we need to ensure that exactly the required number of consumer IDs are used,
	// and each gets assigned partitions accordingly.
	consumerIDCount := min(b.requiredConsumerIDs, len(members))

	// partitions assigment to consumer IDs in round-robin fashion
	for idx, partition := range partitions {
		consumerIndex := idx % consumerIDCount
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

// custom balancer example:
// | max_threads | total partitions | reader-IDs per stream (distinct) | reused? |
// | ------------ | ---------------- | -------------------------------- | ------- |
// | 6            | 6 (3+3)          | 3 + 3                            | no      |
// | 5            | 6                | 3 + 2                            | 1 ID    |
// | 4            | 6                | 2 + 2                            | 2 IDs   |
// | 3            | 6                | 2 + 1                            | 3 IDs   |
// | 2            | 6                | 1 + 1                            | 4 IDs   |
// | 1            | 6                | 1 + 1                            | 5 IDs   |
