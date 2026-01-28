package kafka

import (
	"net/http"
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
	Dialer                      *kafka.Dialer
	AdminClient                 *kafka.Client
}

type kafkaReader struct {
	id       string
	clientID string
	reader   *kafka.Reader
}

// ReaderManager manages Kafka readers and their metadata
type ReaderManager struct {
	config         ReaderConfig
	readers        []*kafkaReader
	partitionIndex map[string]types.PartitionMetaData // get per-partition boundaries
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	requiredConsumerIDs int
	readerIndex         int
	partitionIndex      map[string]types.PartitionMetaData
}

// SchemaRegistryClient holds the schema registry client information
type SchemaRegistryClient struct {
	endpoint   string
	username   string
	password   string
	httpClient *http.Client
	schemaMap  sync.Map // map[uint32]*RegisteredSchema (key -> schemaID)
}
