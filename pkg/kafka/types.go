package kafka

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake/types"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ReaderManager exit modes used to control CDC processing flow.
const (
	normalProcessing int32 = iota // normal CDC processing state
	gracefulExit                  // stop processing during rebalance without triggering abstract-layer retries
	nonRetryableExit              // stop processing due to unrecoverable consumer state
)

// ReaderConfig holds configuration for creating Kafka readers
type ReaderConfig struct {
	MaxThreads                  int
	ThreadsEqualTotalPartitions bool
	ConsumerGroupID             string
	Dialer                      []kgo.Opt
	AdminClient                 *kadm.Client
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
	topics         []string                           // consume topics, set in CreateReaders and used by CreateReader
	partitionIndex map[string]types.PartitionMetaData // get per-partition boundaries
	exitMode       atomic.Int32                       // normalProcessing | gracefulExit | nonRetryableExit
	generationID   atomic.Int32                       // Group generationId is used to detect rebalances
}

// CustomGroupBalancer ensures proper consumer ID distribution according to requirements
type CustomGroupBalancer struct {
	requiredConsumerIDs int
	partitionIndex      map[string]types.PartitionMetaData
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
