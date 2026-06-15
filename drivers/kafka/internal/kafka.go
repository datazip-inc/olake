package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"regexp"
	"slices"
	"sync"

	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	kafkaplain "github.com/twmb/franz-go/pkg/sasl/plain"
	kafkascram "github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	Key            = "_kafka_key"
	Offset         = "_kafka_offset"
	Partition      = "_kafka_partition"
	KafkaTimestamp = "_kafka_timestamp"

	// ConfluentWireFormatMagicByte is the magic byte prefix for Confluent Schema Registry wire format
	ConfluentWireFormatMagicByte = 0x00
)

// InternalKafkaTopics are internal kafka topics (created due to external services) to be skipped
var InternalKafkaTopics = []string{"__amazon_msk_canary", "_schemas"}

type Kafka struct {
	config               *Config
	dialer               []kgo.Opt
	client               *kgo.Client
	state                *types.State
	consumerGroupID      string
	readerManager        *kafkapkg.ReaderManager
	checkpointMessage    sync.Map // last message for each reader w.r.t. partition to be used for checkpointing
	schemaRegistryClient *kafkapkg.SchemaRegistryClient
	adminClient          *kadm.Client
}

func (k *Kafka) GetConfigRef() abstract.Config {
	k.config = &Config{}
	return k.config
}

func (k *Kafka) Spec() any {
	return Config{}
}

func (k *Kafka) Type() string {
	return string(constants.Kafka)
}

func (k *Kafka) MaxConnections() int {
	// return number of readers if available else default
	if k.readerManager != nil {
		return k.readerManager.GetReaderCount()
	}
	return k.config.MaxThreads
}

func (k *Kafka) MaxRetries() int {
	return k.config.RetryCount
}

func (k *Kafka) CDCSupported() bool {
	return true
}

func (k *Kafka) SetupState(state *types.State) {
	k.state = state
}

func (k *Kafka) Setup(ctx context.Context) error {
	if err := k.config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %s", err)
	}

	dialer, err := k.createDialer()
	if err != nil {
		return fmt.Errorf("failed to create Kafka dialer: %s", err)
	}

	// create admin client for metadata and offset operations
	client, err := kgo.NewClient(dialer...)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %s", err)
	}

	k.client = client
	k.adminClient = kadm.NewClient(client)

	// Test connectivity by fetching metadata
	err = client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping kafka brokers: %s", err)
	}

	k.dialer = dialer

	// initialize confluent schema registry client if configured
	if k.config.SchemaRegistry != nil {
		k.config.SchemaRegistry.Init()
		if err := k.config.SchemaRegistry.Validate(); err != nil {
			return fmt.Errorf("schema registry validation failed: %s", err)
		}
		k.schemaRegistryClient = k.config.SchemaRegistry
		logger.Infof("initialized schema registry client for endpoint: %s", k.config.SchemaRegistry.Endpoint)
	}

	// TODO: Avoid modifying the configured/default retry count during initialization across all drivers.
	// check for default backoff count
	k.config.RetryCount = utils.Ternary(k.config.RetryCount <= 0, 1, k.config.RetryCount+1).(int)

	return nil
}

func (k *Kafka) Close() error {
	if k.readerManager != nil {
		if err := k.readerManager.RemoveExistingConsumers(context.Background(), k.client); err != nil {
			logger.Warnf("failed to remove existing consumers during close: %s", err)
		}
	}

	if k.client != nil {
		k.client.Close()
	}
	return nil
}

func (k *Kafka) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Kafka")
	metadata, err := k.adminClient.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %s", err)
	}

	var topicNames []string
	for topicName := range metadata {
		// skip internal topics
		if slices.Contains(InternalKafkaTopics, topicName) {
			continue
		}
		topicNames = append(topicNames, topicName)
	}
	return topicNames, nil
}

// TODO: for avro, we use decode messages to get stream properties similar to JSON, we should directly use the avro schema to get stream properties
func (k *Kafka) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("producing schema for topic [%s]", streamName)
	stream := types.NewStream(streamName, "topics", nil)
	stream.WithSyncMode(types.STRICTCDC)
	stream.SyncMode = types.STRICTCDC

	// create reader manager for schema discovery
	readerManager := kafkapkg.NewReaderManager(kafkapkg.ReaderConfig{
		Dialer:      k.dialer,
		AdminClient: k.adminClient,
	})

	// get the topic metadata
	topicDetail, err := readerManager.GetTopicMetadata(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata for topic %s: %s", streamName, err)
	}

	startOffsets, endOffsets, err := readerManager.ListTopicOffsets(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to list offsets for topic %s: %s", streamName, err)
	}

	if topicDetail.Err != nil {
		return nil, fmt.Errorf("topic metadata for %s: %s", streamName, topicDetail.Err)
	}

	partitionList := topicDetail.Partitions.Sorted()

	var mu sync.Mutex
	// get messages from partitions for schema discovery
	err = utils.Concurrent(ctx, partitionList, len(partitionList), func(ctx context.Context, partitionDetail kadm.PartitionDetail, _ int) error {
		if partitionDetail.Err != nil {
			return fmt.Errorf("partition %d: %s", partitionDetail.Partition, partitionDetail.Err)
		}

		startOffset, endOffset, offsetsFound := readerManager.GetPartitionOffsets(startOffsets, endOffsets, streamName, partitionDetail.Partition)
		if !offsetsFound {
			return nil
		}

		// skip empty partitions
		if startOffset.Offset >= endOffset.Offset {
			return nil
		}

		consumerOpts := append([]kgo.Opt{}, k.dialer...)

		consumerOpts = append(
			consumerOpts,
			kgo.FetchMaxBytes(10e6),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				streamName: {partitionDetail.Partition: kgo.NewOffset().At(startOffset.Offset)},
			}),
		)

		reader, err := kgo.NewClient(consumerOpts...)
		if err != nil {
			return err
		}
		defer reader.Close()

		messageCount := 0

		_ = k.processKafkaMessages(ctx, reader, func(record types.KafkaRecord) (bool, error) {
			messageCount++
			if record.Data != nil {
				mu.Lock()
				// resolve data for schema
				err := typeutils.Resolve(stream, record.Data)
				mu.Unlock()
				if err != nil {
					return true, err
				}
			}

			// stop if hit 10000 messages or reach the last known offset
			shouldExit := messageCount >= 10000 || record.Message.Offset >= endOffset.Offset-1
			return shouldExit, nil
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema for topic %s: %s", streamName, err)
	}

	stream.SourceDefinedPrimaryKey = types.NewSet(Offset, Partition)
	return stream, nil
}

// createDialer creates a Kafka dialer with the appropriate security settings.
func (k *Kafka) createDialer() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(utils.SplitAndTrim(k.config.BootstrapServers)...),
	}

	// Parse SASL credentials
	username, password, err := parseSASLPlain(k.config.Protocol.SASLJAASConfig)
	if err != nil && k.config.Protocol.SASLJAASConfig != "" {
		return nil, err
	}

	// Configure security settings
	switch k.config.Protocol.SecurityProtocol {
	case "PLAINTEXT":
		// No additional configuration needed

	case "SSL":
		// Pure TLS without SASL authentication
		tlsConfig, err := k.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))

	case "SASL_PLAINTEXT":
		switch k.config.Protocol.SASLMechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(kafkaplain.Auth{User: username, Pass: password}.AsMechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(kafkascram.Auth{User: username, Pass: password}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", k.config.Protocol.SASLMechanism)
		}

	case "SASL_SSL":
		// TLS with SASL authentication
		tlsConfig, err := k.buildTLSConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))

		switch k.config.Protocol.SASLMechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(kafkaplain.Auth{User: username, Pass: password}.AsMechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(kafkascram.Auth{User: username, Pass: password}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", k.config.Protocol.SASLMechanism)
		}

	default:
		return nil, fmt.Errorf("unsupported security protocol: %s", k.config.Protocol.SecurityProtocol)
	}

	return opts, nil
}

// parseSASLPlain parses the SASL JAAS configuration to extract username and password.
func parseSASLPlain(jassConfig string) (string, string, error) {
	if jassConfig == "" {
		return "", "", nil
	}
	re := regexp.MustCompile(`username="([^"]+)"\s+password="([^"]+)"`)
	matches := re.FindStringSubmatch(jassConfig)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid sasl_jaas_config for PLAIN")
	}
	return matches[1], matches[2], nil
}

// TODO: check if we can use the utils.BuildTLSConfig function here (kafka)
// buildTLSConfig creates TLS configuration with optional external certificates
func (k *Kafka) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Apply SSL config if provided
	if k.config.Protocol.SSL != nil {
		tlsConfig.InsecureSkipVerify = k.config.Protocol.TLSSkipVerify

		// Load CA certificate if provided
		if k.config.Protocol.SSL.ServerCA != "" {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM([]byte(k.config.Protocol.SSL.ServerCA)) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		// Load client certificate and key for mTLS
		if k.config.Protocol.SSL.ClientCert != "" && k.config.Protocol.SSL.ClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(k.config.Protocol.SSL.ClientCert), []byte(k.config.Protocol.SSL.ClientKey))
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate/key: %s", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	return tlsConfig, nil
}

// checkPartitionCompletion checks if a partition is complete and handles loop termination
func (k *Kafka) checkPartitionCompletion(assignedPartitions []types.PartitionKey, completedPartitions, observedPartitions map[types.PartitionKey]struct{}) (bool, error) {
	// cache observed partitions
	if len(observedPartitions) == 0 {
		// Ensure we have all assigned partitions tracked
		for _, assignedPk := range assignedPartitions {
			if _, exists := k.readerManager.GetPartitionMeta(kafkapkg.PartitionMetadataKey(assignedPk.Topic, assignedPk.Partition)); exists {
				observedPartitions[assignedPk] = struct{}{}
			}
		}
	}

	// exit when all partitions are done
	return len(completedPartitions) == len(observedPartitions), nil
}

// getReaderAssignedPartitions queries the consumer group and returns topic/partition pairs
// assigned to the reader identified by readerIndex. We match on the per-reader readerID.
func (k *Kafka) getReaderAssignedPartitions(ctx context.Context, readerIndex int) ([]types.PartitionKey, error) {
	readerID, _ := k.readerManager.GetReaderIDAndClientID(readerIndex)
	if readerID == "" {
		return nil, fmt.Errorf("readerID not found for reader index %d", readerIndex)
	}

	describeGroupResp, describeGroupRespErr := k.adminClient.DescribeGroups(ctx, k.consumerGroupID)
	if describeGroupRespErr != nil {
		return nil, fmt.Errorf("DescribeGroups failed: %s", describeGroupRespErr)
	}

	if describeGroupResp.Error() != nil {
		return nil, fmt.Errorf("describe group %s response error: %s", k.consumerGroupID, describeGroupResp.Error())
	}

	var assigned []types.PartitionKey
	for _, member := range describeGroupResp[k.consumerGroupID].Members {
		if member.InstanceID == nil || *member.InstanceID != readerID {
			continue
		}

		assignment, ok := member.Assigned.AsConsumer()
		if !ok {
			continue
		}

		for _, topic := range assignment.Topics {
			for _, partition := range topic.Partitions {
				assigned = append(assigned, types.PartitionKey{Topic: topic.Topic, Partition: partition})
			}
		}
	}

	return assigned, nil
}

// isConfluentWireFormat checks if data starts with Confluent wire format magic byte
// Wire format: [magic byte (0x00)] [4-byte schema ID (big-endian)] [payload]
func isConfluentWireFormat(data []byte) bool {
	return len(data) >= 5 && data[0] == ConfluentWireFormatMagicByte
}
