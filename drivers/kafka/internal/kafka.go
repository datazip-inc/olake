package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	kafkapkg "github.com/datazip-inc/olake/pkg/kafka"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	Message        = "message"
	Key            = "key"
	Offset         = "offset"
	Partition      = "partition"
	KafkaTimestamp = "kafka_timestamp"
)

type Kafka struct {
	config            *Config
	dialer            *kafka.Dialer
	adminClient       *kafka.Client
	state             *types.State
	consumerGroupID   string
	readerManager     *kafkapkg.ReaderManager
	checkpointMessage sync.Map // last message for each reader w.r.t. partition to be used for checkpointing
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
		return fmt.Errorf("config validation failed: %s", err)
	}

	dialer, err := k.createDialer()
	if err != nil {
		return fmt.Errorf("failed to create Kafka dialer: %s", err)
	}

	// create admin client for metadata and offset operations
	k.adminClient = &kafka.Client{
		Addr: kafka.TCP(utils.SplitAndTrim(k.config.BootstrapServers)...),
		Transport: &kafka.Transport{
			SASL: dialer.SASLMechanism,
			TLS:  dialer.TLS,
		},
	}

	// Test connectivity by fetching metadata
	_, err = k.adminClient.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return fmt.Errorf("failed to ping Kafka brokers: %s", err)
	}

	k.dialer = dialer
	return nil
}

func (k *Kafka) Close() error {
	k.adminClient = nil
	k.dialer = nil
	k.readerManager.Close()
	return nil
}

func (k *Kafka) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for Kafka")
	resp, err := k.adminClient.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %s", err)
	}

	var topicNames []string
	for _, topic := range resp.Topics {
		topicNames = append(topicNames, topic.Name)
	}
	return topicNames, nil
}

func (k *Kafka) ProduceSchema(_ context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("producing schema for topic [%s]", streamName)
	stream := types.NewStream(streamName, "topics", nil).WithSyncMode(types.STRICTCDC)
	stream.SyncMode = types.STRICTCDC
	schema := types.NewTypeSchema()
	schema.AddTypes(Message, types.String)       // message payload
	schema.AddTypes(Key, types.String)           // Kafka message key
	schema.AddTypes(Offset, types.Int64)         // Offset for tracking
	schema.AddTypes(Partition, types.Int64)      // Partition
	schema.AddTypes(KafkaTimestamp, types.Int64) // Message timestamp
	stream.WithSchema(schema)
	stream.SourceDefinedPrimaryKey = types.NewSet(Offset, Partition)
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
				return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %s", err)
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
				return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %s", err)
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

// checkPartitionCompletion checks if a partition is complete and handles loop termination
func (k *Kafka) checkPartitionCompletion(ctx context.Context, readerID string, completedPartitions, observedPartitions map[types.PartitionKey]struct{}) (bool, error) {
	// cache observed partitions
	// Ensure we have all assigned partitions tracked
	assigned, err := k.getReaderAssignedPartitions(ctx, readerID)
	if err != nil {
		return false, err
	}

	for _, assignedPk := range assigned {
		if _, exists := k.readerManager.GetPartitionIndex(fmt.Sprintf("%s:%d", assignedPk.Topic, assignedPk.Partition)); exists {
			observedPartitions[assignedPk] = struct{}{}
		}
	}

	// exit when all partitions are done
	return len(completedPartitions) == len(observedPartitions), nil
}

// getReaderAssignedPartitions queries the consumer group and returns topic/partition pairs
// assigned to the reader identified by readerID. We match on the per-reader ClientID.
func (k *Kafka) getReaderAssignedPartitions(ctx context.Context, readerID string) ([]types.PartitionKey, error) {
	clientID, ok := k.readerManager.GetReaderClientID(readerID)
	if !ok || clientID == "" {
		return nil, fmt.Errorf("clientID not found for reader %s", readerID)
	}

	// use the first broker address set on the client; fall back to bootstrap servers
	addr := k.adminClient.Addr
	if addr == nil {
		brokers := utils.SplitAndTrim(k.config.BootstrapServers)
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

	var assigned []types.PartitionKey
	for _, group := range resp.Groups {
		if group.GroupID != k.consumerGroupID || group.Error != nil {
			continue
		}
		for _, member := range group.Members {
			// try to match the client we created: primary on ClientID, fallback to MemberID or suffix match
			if member.ClientID != clientID && member.MemberID != clientID && !strings.Contains(member.ClientID, readerID) && !strings.Contains(member.MemberID, readerID) {
				continue
			}
			for _, topic := range member.MemberAssignments.Topics {
				for _, partition := range topic.Partitions {
					assigned = append(assigned, types.PartitionKey{Topic: topic.Topic, Partition: partition})
				}
			}
		}
	}

	return assigned, nil
}

// GetReaderTasks returns the list of reader IDs to run
func (k *Kafka) GetReaderIDs() []string {
	return k.readerManager.GetReaderIDs()
}
