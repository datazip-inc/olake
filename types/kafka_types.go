package types

// PartitionMetaData holds metadata about a Kafka partition for a specific stream reader
type PartitionMetaData struct {
	ReaderID    string
	Stream      StreamInterface
	PartitionID int
	EndOffset   int64
	StartOffset int64
}

// PartitionKey represents a unique key for a Kafka partition and topic
type PartitionKey struct {
	Topic     string
	Partition int
}
