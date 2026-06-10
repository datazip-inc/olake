package types

import (
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeJSON     SchemaType = "JSON"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
)

// PartitionMetaData holds metadata about a Kafka partition for a specific stream reader
type PartitionMetaData struct {
	Stream          StreamInterface
	PartitionID     int32
	StartOffset     int64
	EndOffset       int64
	CommittedOffset int64
}

// KafkaMetadataState holds the metadata for a Kafka stream
type KafkaMetadataState struct {
	ConsumerGroupID string          `json:"consumer_group_id"`
	Offsets         map[int32]int64 `json:"offsets"`
}

// PartitionKey represents a unique key for a Kafka partition and topic
type PartitionKey struct {
	Topic     string
	Partition int32
}

// KafkaRecord represents a record (data + message) from a Kafka partition
type KafkaRecord struct {
	Data    map[string]interface{}
	Message *kgo.Record
}

// RegisteredSchema holds the schema information
type RegisteredSchema struct {
	SchemaType SchemaType
	Codec      *goavro.Codec // Only for Avro schemas
}
