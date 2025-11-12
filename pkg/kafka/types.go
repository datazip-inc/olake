package kafka

import (
	"github.com/datazip-inc/olake/types"
	"github.com/segmentio/kafka-go"
)

// ReaderConfig holds configuration for creating Kafka readers
type ReaderConfig struct {
	MaxThreads                  int
	ThreadsEqualTotalPartitions bool
	BootstrapServers            string
	ConsumerGroupID             string
	Dialer                      *kafka.Dialer
	AdminClient                 *kafka.Client
}

// ReaderManager manages Kafka readers and their metadata
type ReaderManager struct {
	config          ReaderConfig
	readers         map[string]*kafka.Reader           // for fast reader access
	partitionIndex  map[string]types.PartitionMetaData // get per-partition boundaries
	readerClientIDs map[string]string                  // reader's client id mapping
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	requiredConsumerIDs int
	readerIndex         int
	partitionIndex      map[string]types.PartitionMetaData
}
