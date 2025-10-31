package kafka

import (
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/segmentio/kafka-go"
)

// ReaderConfig holds configuration for creating Kafka readers
type ReaderConfig struct {
	MaxThreads                  int
	ThreadsEqualTotalPartitions bool
	BootstrapServers            string
	ConsumerGroupID             string
	AutoOffsetReset             string
	Dialer                      *kafka.Dialer
	AdminClient                 *kafka.Client
}

// ReaderManager manages Kafka readers and their metadata
type ReaderManager struct {
	config             ReaderConfig
	readers            map[string]*kafka.Reader
	partitionIndex     map[string]types.PartitionMetaData
	readerClientIDs    map[string]string
	readerLastMessages sync.Map // map[string]map[partKey]kafka.Message
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	requiredConsumerIDs int
	readerIndex         int
}
