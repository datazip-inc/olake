package types

import (
	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeJSON     SchemaType = "JSON"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

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

// RegisteredSchema holds the schema information
type RegisteredSchema struct {
	SchemaType SchemaType
	Schema     string
	Codec      *goavro.Codec // Only for Avro schemas
}
