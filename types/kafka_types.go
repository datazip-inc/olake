package types

import "github.com/segmentio/kafka-go"

// PartitionMetaData holds metadata about a Kafka partition for a specific stream reader
type PartitionMetaData struct {
	ReaderID    string
	Stream      StreamInterface
	PartitionID int
	EndOffset   int64
}

// PartitionKey represents a unique key for a Kafka partition and topic
type PartitionKey struct {
	Topic     string
	Partition int
}

// KafkaRecord represents a record (data + message) from a Kafka partition
type KafkaRecord struct {
	Data    map[string]interface{}
	Message kafka.Message
}
