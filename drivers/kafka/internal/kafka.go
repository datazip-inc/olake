package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Kafka struct {
	config               *Config
	dialer               *kafka.Dialer
	adminClient          *kafka.Client
	state                *types.State
	consumerGroupID      string
	readers              map[string]*kafka.Reader             // created once in PreCDC, read-only afterwards
	partitionMeta        map[string][]types.PartitionMetaData // streamID -> partitions
	partitionIndex       map[string]types.PartitionMetaData   // key: partitionId (topic+":"+partition) -> partition metadata
	readerLastMessages   map[string]map[partKey]kafka.Message // per-reader final last messages
	readerLastMessagesMu sync.Mutex
	readerClientIDs      map[string]string // readerID -> clientID used by this reader's dialer
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
		return fmt.Errorf("config validation failed: %v", err)
	}

	dialer, err := k.createDialer()
	if err != nil {
		return fmt.Errorf("failed to create Kafka dialer: %v", err)
	}

	// Create admin client for metadata and offset operations
	adminClient := &kafka.Client{
		Addr: kafka.TCP(splitAndTrim(k.config.BootstrapServers)...),
		Transport: &kafka.Transport{
			SASL: dialer.SASLMechanism,
			TLS:  dialer.TLS,
		},
	}

	// Test connectivity by fetching metadata
	_, err = adminClient.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return fmt.Errorf("failed to ping Kafka brokers: %v", err)
	}
	k.dialer = dialer
	k.adminClient = adminClient
	return nil
}

func (k *Kafka) Close() error {
	k.adminClient = nil
	k.dialer = nil
	for id, r := range k.readers {
		if r != nil {
			if err := r.Close(); err != nil {
				logger.Warnf("failed to close reader %s: %v\n", id, err)
			}
		}
		delete(k.readers, id)
	}
	for kStr := range k.partitionMeta {
		delete(k.partitionMeta, kStr)
	}
	for kStr := range k.partitionIndex {
		delete(k.partitionIndex, kStr)
	}
	return nil
}

func (k *Kafka) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Kafka")
	resp, err := k.adminClient.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("[KAFKA] failed to list topics: %v", err)
	}

	var topicNames []string
	for _, topic := range resp.Topics {
		topicNames = append(topicNames, topic.Name)
	}
	return topicNames, nil
}

func (k *Kafka) ProduceSchema(_ context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("[KAFKA] producing schema for topic [%s]", streamName)
	stream := types.NewStream(streamName, "topics", nil)
	schema := types.NewTypeSchema()
	schema.AddTypes("message", types.String)        // message payload
	schema.AddTypes("key", types.String)            // Kafka message key
	schema.AddTypes("offset", types.Int64)          // Offset for tracking
	schema.AddTypes("partition", types.Int64)       // Partition
	schema.AddTypes("kafka_timestamp", types.Int64) // Message timestamp
	stream.WithSchema(schema)
	stream.SourceDefinedPrimaryKey = types.NewSet("offset", "partition")
	return stream, nil
}

// createDialer creates a Kafka dialer with the appropriate security settings.
func (k *Kafka) createDialer() (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
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
	case "SASL_PLAINTEXT":
		switch k.config.Protocol.SASLMechanism {
		case "PLAIN":
			dialer.SASLMechanism = plain.Mechanism{
				Username: username,
				Password: password,
			}
		case "SCRAM-SHA-512":
			dialer.SASLMechanism, err = scram.Mechanism(scram.SHA512, username, password)
			if err != nil {
				return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %v", err)
			}
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", k.config.Protocol.SASLMechanism)
		}
	case "SASL_SSL":
		dialer.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		switch k.config.Protocol.SASLMechanism {
		case "PLAIN":
			dialer.SASLMechanism = plain.Mechanism{
				Username: username,
				Password: password,
			}
		case "SCRAM-SHA-512":
			dialer.SASLMechanism, err = scram.Mechanism(scram.SHA512, username, password)
			if err != nil {
				return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %v", err)
			}
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", k.config.Protocol.SASLMechanism)
		}
	default:
		return nil, fmt.Errorf("unsupported security protocol: %s", k.config.Protocol.SecurityProtocol)
	}

	return dialer, nil
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

func (k *Kafka) GetTopicMetadata(ctx context.Context, topic string) (*kafka.Topic, error) {
	metadataReq := &kafka.MetadataRequest{Topics: []string{topic}}
	metadataResp, err := k.adminClient.Metadata(ctx, metadataReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata for topic %s: %w", topic, err)
	}

	for _, t := range metadataResp.Topics {
		if t.Name == topic {
			if t.Error != nil {
				return nil, fmt.Errorf("topic %s not found in metadata: %v", topic, t.Error)
			}
			return &t, nil
		}
	}

	return nil, fmt.Errorf("topic %s not found in metadata", topic)
}

// getReaderAssignedPartitions queries the consumer group and returns topic/partition pairs
// assigned to the reader identified by readerID. We match on the per-reader ClientID.
func (k *Kafka) getReaderAssignedPartitions(ctx context.Context, readerID string) ([]partKey, error) {
	if k.adminClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	clientID, ok := k.readerClientIDs[readerID]
	if !ok || clientID == "" {
		return nil, fmt.Errorf("clientID not found for reader %s", readerID)
	}

	// Use the first broker address set on the client; fall back to bootstrap servers
	addr := k.adminClient.Addr
	if addr == nil {
		brokers := splitAndTrim(k.config.BootstrapServers)
		if len(brokers) == 0 {
			return nil, fmt.Errorf("no brokers configured")
		}
		addr = kafka.TCP(brokers...)
	}

	resp, err := k.adminClient.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		Addr:     addr,
		GroupIDs: []string{k.consumerGroupID},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeGroups failed: %w", err)
	}
	var assigned []partKey
	for _, g := range resp.Groups {
		if g.GroupID != k.consumerGroupID || g.Error != nil {
			continue
		}
		for _, m := range g.Members {
			// try to match the client we created: primary on ClientID, fallback to MemberID or suffix match
			if m.ClientID != clientID && m.MemberID != clientID && !strings.Contains(m.ClientID, readerID) && !strings.Contains(m.MemberID, readerID) {
				continue
			}
			for _, t := range m.MemberAssignments.Topics {
				for _, p := range t.Partitions {
					assigned = append(assigned, partKey{topic: t.Topic, partition: p})
				}
			}
		}
	}
	return assigned, nil
}

// ShouldMatchPartitionCount returns whether to align thread count with total partitions
func (k *Kafka) ShouldMatchPartitionCount() bool {
	return k.config.ThreadsEqualTotalPartitions
}

// partKey identifies a topic-partition pair for map keys
type partKey struct {
	topic     string
	partition int
}

// GetReaderTasks returns the list of reader IDs to run
func (k *Kafka) GetReaderTasks() []string {
	ids := make([]string, 0, len(k.readers))
	for readerID := range k.readers {
		ids = append(ids, readerID)
	}
	return ids
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}
