package kafka

import (
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/segmentio/kafka-go"
)

// ReaderConfig holds configuration for creating Kafka readers
type ReaderConfig struct {
	BootstrapServers            string
	MaxThreads                  int
	ConsumerGroupID             string
	Dialer                      *kafka.Dialer
	AdminClient                 *kafka.Client
	ThreadsEqualTotalPartitions bool
}

// ReaderManager manages Kafka readers and their metadata
type ReaderManager struct {
	config             ReaderConfig
	partitionIndex     map[string]types.PartitionMetaData
	readers            map[string]*kafka.Reader
	readerLastMessages sync.Map // map[string]map[partKey]kafka.Message
	readerClientIDs    map[string]string
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	requiredConsumerIDs int
	readerIndex         int
}
