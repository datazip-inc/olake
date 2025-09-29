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
	config          *Config
	dialer          *kafka.Dialer
	adminClient     *kafka.Client
	state           *types.State
	consumerGroupID string
	readers         sync.Map // map[string]*kafka.Reader
	partitionMeta   sync.Map // map[string][]abstract.PartitionMetaData
	lastMessages    sync.Map
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
	return true // Kafka uses streaming mode with offsets
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
		Addr: kafka.TCP(strings.Split(k.config.BootstrapServers, ",")...),
	}

	// Test connectivity by fetching metadata for a dummy topic
	_, err = adminClient.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{"__test"},
	})
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
	k.partitionMeta.Range(func(key, _ any) bool {
		k.partitionMeta.Delete(key)
		return true
	})
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
