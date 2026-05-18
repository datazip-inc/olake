package kafka

import (
	"net/http"
	"sync"

	"github.com/datazip-inc/olake/types"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReaderConfig holds configuration for creating Kafka readers
type ReaderConfig struct {
	MaxThreads                  int
	ThreadsEqualTotalPartitions bool
	BootstrapServers            string
	ConsumerGroupID             string
	Dialer                      []kgo.Opt
	Client                      *kgo.Client
}

type kafkaReader struct {
	id       string
	clientID string
	reader   *kgo.Client
}

// ReaderManager manages Kafka readers and their metadata
type ReaderManager struct {
	config         ReaderConfig
	readers        []*kafkaReader
	partitionIndex map[string]types.PartitionMetaData // get per-partition boundaries
	// Shared across all reader clients in this manager so custom balance state (e.g. topic stickiness) stays consistent.
	olakeGroupBalancer *CustomGroupBalancer
}

// SchemaRegistryClient holds the schema registry client information
type SchemaRegistryClient struct {
	Endpoint string `json:"endpoint"`

	// Authentication
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	BearerToken string `json:"bearer_token,omitempty"`

	httpClient *http.Client
	schemaMap  sync.Map // map[uint32]*RegisteredSchema (key -> schemaID)
}
